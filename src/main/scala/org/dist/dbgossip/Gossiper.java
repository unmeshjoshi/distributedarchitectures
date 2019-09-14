package org.dist.dbgossip;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    
    final List<InetAddressAndPort> liveEndpoints = new ArrayList<>();
    public long firstSynSendAt;

    /* unreachable member set */
    private List<InetAddressAndPort> unreachableEndpoints = new ArrayList<InetAddressAndPort>();

    /* initial seeds for joining the cluster */
    List<InetAddressAndPort> seeds = new ArrayList<>();

    /* map where key is the endpoint and value is the state associated with the endpoint */
    ConcurrentHashMap<InetAddressAndPort, EndPointState> endpointStateMap = new ConcurrentHashMap<InetAddressAndPort, EndPointState>();
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
    private ScheduledFuture<?> scheduledGossipTask;

    public boolean isEnabled() {
        return false;
    }

    public void notifyFailureDetector(Map<InetAddressAndPort, EndPointState> remoteEpStateMap) {

    }

    private void handleNewJoin(InetAddressAndPort ep, EndPointState epState)
    {
        /* Mark this endpoint as "live" */
        endpointStateMap.put(ep, epState);
        isAlive(ep, epState, true);
    }

    synchronized void isAlive(InetAddressAndPort addr, EndPointState epState, boolean value)
    {
        epState.markAlive();
        if ( value )
        {
            liveEndpoints.add(addr);
            unreachableEndpoints.remove(addr);
        }
        else
        {
            liveEndpoints.remove(addr);
            unreachableEndpoints.add(addr);
        }
    }

    public void applyStateLocally(Map<InetAddressAndPort, EndPointState> remoteEpStateMap) {
        for (InetAddressAndPort inetAddressAndPort : remoteEpStateMap.keySet()) {
            handleNewJoin(inetAddressAndPort, remoteEpStateMap.get(inetAddressAndPort));
        }
    }



    public Map<InetAddressAndPort, EndPointState> handleGossipDigestSynAck(List<GossipDigest> gDigestList,
                                                                           Map<InetAddressAndPort, EndPointState> epStateMap) {

        if ( epStateMap.size() > 0 )
        {
            /* Notify the Failure Detector */
            notifyFailureDetector(epStateMap);
            applyStateLocally(epStateMap);
        }

        /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
        Map<InetAddressAndPort, EndPointState> deltaEpStateMap = new HashMap<InetAddressAndPort, EndPointState>();
        for( GossipDigest gDigest : gDigestList )
        {
            InetAddressAndPort addr = gDigest.getEndPoint();
            EndPointState localEpStatePtr = getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
            if ( localEpStatePtr != null )
                deltaEpStateMap.put(addr, localEpStatePtr);
        }
        return deltaEpStateMap;
    }

    synchronized EndPointState getStateForVersionBiggerThan(InetAddressAndPort forEndpoint, int version)
    {
        EndPointState epState = endpointStateMap.get(forEndpoint);
        EndPointState reqdEndPointState = null;

        if ( epState != null )
        {
            /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
             */
            int localHbVersion = epState.getHeartBeatState().getHeartBeatVersion();
            if ( localHbVersion > version )
            {
                reqdEndPointState = new EndPointState(epState.getHeartBeatState());
            }
            Map<ApplicationState, VersionedValue> appStateMap = epState.getApplicationState();
            /* Accumulate all application states whose versions are greater than "version" variable */
            Set<ApplicationState> keys = appStateMap.keySet();
            for ( ApplicationState key : keys )
            {
                VersionedValue appState = appStateMap.get(key);
                if ( appState.version > version )
                {
                    if ( reqdEndPointState == null )
                    {
                        reqdEndPointState = new EndPointState(epState.getHeartBeatState());
                    }
                    reqdEndPointState.addApplicationState(key, appState);
                }
            }
        }
        return reqdEndPointState;
    }


    public void start(int generationNbr, Map<ApplicationState, VersionedValue> preloadLocalStates) {
        buildSeedsList();
        /* initialize the heartbeat state for this localEndpoint */
        maybeInitializeLocalState(generationNbr);
        EndPointState localState = endpointStateMap.get(FBUtilities.getBroadcastAddressAndPort());
        localState.addApplicationStates(preloadLocalStates);


        scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask(),
                Gossiper.intervalInMillis,
                Gossiper.intervalInMillis,
                TimeUnit.MILLISECONDS);
    }

    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    public void maybeInitializeLocalState(int generationNbr) {
        HeartBeatState hbState = new HeartBeatState(generationNbr);
        EndPointState localState = new EndPointState(hbState);
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

                final List<GossipDigest> gDigests = getGossipDigests();
                if (gDigests.size() > 0)
                {
                    GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
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

        private List<GossipDigest> getGossipDigests() {
            final List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
            Gossiper.instance.makeRandomGossipDigest(gDigests);
            return gDigests;
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
                sendGossip(message, unreachableEndpoints);
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
    private boolean sendGossip(Message<GossipDigestSyn> message, List<InetAddressAndPort> epSet)
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
        EndPointState epState;
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

    int getMaxEndpointStateVersion(EndPointState epState)
    {
        int maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
        for (Map.Entry<ApplicationState, VersionedValue> state : epState.states())
            maxVersion = Math.max(maxVersion, state.getValue().version);
        return maxVersion;
    }

    int getMaxEndPointStateVersion(EndPointState epState)
    {
        List<Integer> versions = new ArrayList<Integer>();
        versions.add( epState.getHeartBeatState().getHeartBeatVersion() );
        Map<ApplicationState, VersionedValue> appStateMap = epState.getApplicationState();

        Set<ApplicationState> keys = appStateMap.keySet();
        for ( ApplicationState key : keys )
        {
            int stateVersion = appStateMap.get(key).version;
            versions.add( stateVersion );
        }

        /* sort to get the max version to build GossipDigest for this endpoint */
        Collections.sort(versions);
        int maxVersion = versions.get(versions.size() - 1);
        versions.clear();
        return maxVersion;
    }

    /*
       This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
       and the delta state are built up.
   */
    synchronized void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList, Map<InetAddressAndPort, EndPointState> deltaEpStateMap)
    {
        for ( GossipDigest gDigest : gDigestList )
        {
            int remoteGeneration = gDigest.getGeneration();
            int maxRemoteVersion = gDigest.getMaxVersion();
            /* Get state associated with the end point in digest */
            EndPointState epStatePtr = endpointStateMap.get(gDigest.getEndPoint());
            /*
                Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
                then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
                request all the data for this endpoint.
            */
            if ( epStatePtr != null )
            {
                int localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                /* get the max version of all keys in the state associated with this endpoint */
                int maxLocalVersion = getMaxEndPointStateVersion(epStatePtr);
                if ( remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion )
                    continue;

                if ( remoteGeneration > localGeneration )
                {
                    /* we request everything from the gossiper */
                    requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
                }
                if ( remoteGeneration < localGeneration )
                {
                    /* send all data with generation = localgeneration and version > 0 */
                    sendAll(gDigest, deltaEpStateMap, 0);
                }
                if ( remoteGeneration == localGeneration )
                {
                    /*
                        If the max remote version is greater then we request the remote endpoint send us all the data
                        for this endpoint with version greater than the max version number we have locally for this
                        endpoint.
                        If the max remote version is lesser, then we send all the data we have locally for this endpoint
                        with version greater than the max remote version.
                    */
                    if ( maxRemoteVersion > maxLocalVersion )
                    {
                        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndPoint(), remoteGeneration, maxLocalVersion) );
                    }
                    if ( maxRemoteVersion < maxLocalVersion )
                    {
                        /* send all data with generation = localgeneration and version > maxRemoteVersion */
                        sendAll(gDigest, deltaEpStateMap, maxRemoteVersion);
                    }
                }
            }
            else
            {
                /* We are here since we have no data for this endpoint locally so request everthing. */
                requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
            }
        }



    }
    /* Request all the state for the endpoint in the gDigest */
    void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration)
    {
        /* We are here since we have no data for this endpoint locally so request everthing. */
        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndPoint(), remoteGeneration, 0) );
    }

    /* Send all the data with version greater than maxRemoteVersion */
    void sendAll(GossipDigest gDigest, Map<InetAddressAndPort, EndPointState> deltaEpStateMap, int maxRemoteVersion)
    {
        EndPointState localEpStatePtr = getStateForVersionBiggerThan(gDigest.getEndPoint(), maxRemoteVersion) ;
        if ( localEpStatePtr != null )
            deltaEpStateMap.put(gDigest.getEndPoint(), localEpStatePtr);
    }

}
