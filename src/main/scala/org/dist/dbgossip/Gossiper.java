package org.dist.dbgossip;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.dist.dbgossip.Verb.GOSSIP_DIGEST_SYN;

public class Gossiper {
    public static Gossiper instance = new Gossiper();
    public final static int intervalInMillis = 1000;
    private static final int RING_DELAY = 30 * 1000;
    public final static int QUARANTINE_DELAY = RING_DELAY * 2;
    
    final ConcurrentSkipListSet<InetAddressAndPort> liveEndpoints = new ConcurrentSkipListSet<InetAddressAndPort>();
    public long firstSynSendAt;

    /* unreachable member set */
    private ConcurrentHashMap<InetAddressAndPort, Long> unreachableEndpoints = new ConcurrentHashMap<InetAddressAndPort, Long>();

    /* initial seeds for joining the cluster */
    ConcurrentSkipListSet<InetAddressAndPort> seeds = new ConcurrentSkipListSet<InetAddressAndPort>();

    /* map where key is the endpoint and value is the state associated with the endpoint */
    ConcurrentHashMap<InetAddressAndPort, EndpointState> endpointStateMap = new ConcurrentHashMap<InetAddressAndPort, EndpointState>();
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
    private ScheduledFuture<?> scheduledGossipTask;

    public boolean isEnabled() {
        return false;
    }

    public void notifyFailureDetector(Map<InetAddressAndPort, EndpointState> remoteEpStateMap) {

    }

    public void applyStateLocally(Map<InetAddressAndPort, EndpointState> remoteEpStateMap) {

    }

    public EndpointState getStateForVersionBiggerThan(InetAddressAndPort addr, int maxVersion) {
        return null;
    }

    public void start(int generationNbr, Map<ApplicationState, VersionedValue> preloadLocalStates) {
        buildSeedsList();
        /* initialize the heartbeat state for this localEndpoint */
        maybeInitializeLocalState(generationNbr);
        EndpointState localState = endpointStateMap.get(FBUtilities.getBroadcastAddressAndPort());
        localState.addApplicationStates(preloadLocalStates);


        scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask(),
                Gossiper.intervalInMillis,
                Gossiper.intervalInMillis,
                TimeUnit.MILLISECONDS);
    }

    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    public void maybeInitializeLocalState(int generationNbr) {
        HeartBeatState hbState = new HeartBeatState(generationNbr);
        EndpointState localState = new EndpointState(hbState);
        localState.markAlive();
        endpointStateMap.putIfAbsent(FBUtilities.getBroadcastAddressAndPort(), localState);
    }

    void buildSeedsList() {
        for (InetAddressAndPort seed : DatabaseDescriptor.getSeeds()) {
            if (seed.equals(FBUtilities.getBroadcastAddressAndPort()))
                continue;
            seeds.add(seed);
        }
    }
    private static final ReentrantLock taskLock = new ReentrantLock();

    private class GossipTask implements Runnable
    {
        public void run()
        {
            try
            {
                //wait on messaging service to start listening
                MessagingService.instance().waitUntilListening();

                taskLock.lock();

                /* Update the local heartbeat counter. */
                endpointStateMap.get(FBUtilities.getBroadcastAddressAndPort()).getHeartBeatState().updateHeartBeat();
//                logger.trace("My heartbeat is now {}", endpointStateMap.get(FBUtilities.getBroadcastAddressAndPort()).getHeartBeatState().getHeartBeatVersion());
                final List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
                Gossiper.instance.makeRandomGossipDigest(gDigests);

                if (gDigests.size() > 0)
                {
                    GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                            DatabaseDescriptor.getPartitionerName(),
                            gDigests);
                    Message<GossipDigestSyn> message = Message.out(GOSSIP_DIGEST_SYN, digestSynMessage);
                    /* Gossip to some random live member */
                    boolean gossipedToSeed = doGossipToLiveMember(message);

                    /* Gossip to some unreachable member with some probability to check if he is back up */
                    maybeGossipToUnreachableMember(message);

                    /* Gossip to a seed if we did not do so above, or we have seen less nodes
                       than there are seeds.  This prevents partitions where each group of nodes
                       is only gossiping to a subset of the seeds.

                       The most straightforward check would be to check that all the seeds have been
                       verified either as live or unreachable.  To avoid that computation each round,
                       we reason that:

                       either all the live nodes are seeds, in which case non-seeds that come online
                       will introduce themselves to a member of the ring by definition,

                       or there is at least one non-seed node in the list, in which case eventually
                       someone will gossip to it, and then do a gossip to a random seed from the
                       gossipedToSeed check.

                       See CASSANDRA-150 for more exposition. */
                    if (!gossipedToSeed || liveEndpoints.size() < seeds.size())
                        maybeGossipToSeed(message);

                    doStatusCheck();
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                taskLock.unlock();
            }
        }

        private void doStatusCheck() {
        }
    }

    /* Possibly gossip to a seed for facilitating partition healing */
    private void maybeGossipToSeed(Message<GossipDigestSyn> prod)
    {
        int size = seeds.size();
        if (size > 0)
        {
            if (size == 1 && seeds.contains(FBUtilities.getBroadcastAddressAndPort()))
            {
                return;
            }

            if (liveEndpoints.size() == 0)
            {
                sendGossip(prod, seeds);
            }
            else
            {
                /* Gossip with the seed with some probability. */
                double probability = seeds.size() / (double) (liveEndpoints.size() + unreachableEndpoints.size());
                double randDbl = random.nextDouble();
                if (randDbl <= probability)
                    sendGossip(prod, seeds);
            }
        }
    }

    /* Sends a Gossip message to an unreachable member */
    private void maybeGossipToUnreachableMember(Message<GossipDigestSyn> message)
    {
        double liveEndpointCount = liveEndpoints.size();
        double unreachableEndpointCount = unreachableEndpoints.size();
        if (unreachableEndpointCount > 0)
        {
            /* based on some probability */
            double prob = unreachableEndpointCount / (liveEndpointCount + 1);
            double randDbl = random.nextDouble();
            if (randDbl < prob)
                sendGossip(message, unreachableEndpoints.keySet());
        }
    }

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    private boolean doGossipToLiveMember(Message<GossipDigestSyn> message)
    {
        int size = liveEndpoints.size();
        if (size == 0)
            return false;
        return sendGossip(message, liveEndpoints);
    }

    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     * @param message
     * @param epSet   a set of endpoint from which a random endpoint is chosen.
     * @return true if the chosen endpoint is also a seed.
     */
    private boolean sendGossip(Message<GossipDigestSyn> message, Set<InetAddressAndPort> epSet)
    {
        List<InetAddressAndPort> liveEndpoints = ImmutableList.copyOf(epSet);

        int size = liveEndpoints.size();
        if (size < 1)
            return false;
        /* Generate a random number from 0 -> size */
        int index = (size == 1) ? 0 : random.nextInt(size);
        InetAddressAndPort to = liveEndpoints.get(index);
        if (true)
            System.out.println("Sending a GossipDigestSyn to " + to);
        if (firstSynSendAt == 0)
            firstSynSendAt = System.nanoTime();
        MessagingService.instance().send(message, to);

        boolean isSeed = seeds.contains(to);
        return isSeed;
    }

    private final Random random = new Random();


    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param gDigests list of Gossip Digests.
     */
    private void makeRandomGossipDigest(List<GossipDigest> gDigests)
    {
        EndpointState epState;
        int generation = 0;
        int maxVersion = 0;

        // local epstate will be part of endpointStateMap
        List<InetAddressAndPort> endpoints = new ArrayList<>(endpointStateMap.keySet());
        Collections.shuffle(endpoints, random);
        for (InetAddressAndPort endpoint : endpoints)
        {
            epState = endpointStateMap.get(endpoint);
            if (epState != null)
            {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = getMaxEndpointStateVersion(epState);
            }
            gDigests.add(new GossipDigest(endpoint, generation, maxVersion));
        }

        if (true)
        {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest gDigest : gDigests)
            {
                sb.append(gDigest);
                sb.append(" ");
            }
            System.out.println("Gossip Digests are : " + sb);
        }

     }

    int getMaxEndpointStateVersion(EndpointState epState)
    {
        int maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
        for (Map.Entry<ApplicationState, VersionedValue> state : epState.states())
            maxVersion = Math.max(maxVersion, state.getValue().version);
        return maxVersion;
    }

}
