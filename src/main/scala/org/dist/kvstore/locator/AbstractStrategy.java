package org.dist.kvstore.locator;

import org.apache.log4j.Logger;
import org.dist.kvstore.FailureDetector;
import org.dist.kvstore.InetAddressAndPort;
import org.dist.kvstore.TokenMetadata;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains a helper method that will be used by
 * all abstraction that implement the IReplicaPlacementStrategy
 * interface.
*/
public abstract class AbstractStrategy implements IReplicaPlacementStrategy
{
    protected static Logger logger_ = Logger.getLogger(AbstractStrategy.class);
    
    protected TokenMetadata tokenMetadata_;
    
    AbstractStrategy(TokenMetadata tokenMetadata)
    {
        tokenMetadata_ = tokenMetadata;
    }
    
    

    protected InetAddressAndPort getNextAvailableEndPoints(InetAddressAndPort startPoint, List<InetAddressAndPort> topN, List<InetAddressAndPort> liveNodes)
    {
        InetAddressAndPort endpoint = null;
        Map<BigInteger, InetAddressAndPort> tokenToInetAddressAndPortMap = tokenMetadata_.cloneTokenEndPointMap();
        List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToInetAddressAndPortMap.keySet());
        Collections.sort(tokens);
        BigInteger token = tokenMetadata_.getToken(startPoint);
        int index = Collections.binarySearch(tokens, token);
        if(index < 0)
        {
            index = (index + 1) * (-1);
            if (index >= tokens.size())
                index = 0;
        }
        int totalNodes = tokens.size();
        int startIndex = (index+1)%totalNodes;
        for (int i = startIndex, count = 1; count < totalNodes ; ++count, i = (i+1)%totalNodes)
        {
            InetAddressAndPort tmpInetAddressAndPort = tokenToInetAddressAndPortMap.get(tokens.get(i));
            if(FailureDetector.isAlive(tmpInetAddressAndPort) && !topN.contains(tmpInetAddressAndPort) && !liveNodes.contains(tmpInetAddressAndPort))
            {
                endpoint = tmpInetAddressAndPort;
                break;
            }
        }
        return endpoint;
    }

    /*
     * This method returns the hint map. The key is the InetAddressAndPort
     * on which the data is being placed and the value is the
     * InetAddressAndPort which is in the top N.
     * Get the map of top N to the live nodes currently.
     */
    public Map<InetAddressAndPort, InetAddressAndPort> getHintedStorageEndPoints(BigInteger token)
    {
        List<InetAddressAndPort> liveList = new ArrayList<InetAddressAndPort>();
        Map<InetAddressAndPort, InetAddressAndPort> map = new HashMap<InetAddressAndPort, InetAddressAndPort>();
        InetAddressAndPort[] topN = getStorageEndPoints( token );

        for( int i = 0 ; i < topN.length ; i++)
        {
            if( FailureDetector.isAlive(topN[i]))
            {
                map.put(topN[i], topN[i]);
                liveList.add(topN[i]) ;
            }
            else
            {
                InetAddressAndPort InetAddressAndPort = getNextAvailableEndPoints(topN[i], Arrays.asList(topN), liveList);
                if(InetAddressAndPort != null)
                {
                    map.put(InetAddressAndPort, topN[i]);
                    liveList.add(InetAddressAndPort) ;
                }
                else
                {
                    // log a warning , maybe throw an exception
                    logger_.warn("Unable to find a live InetAddressAndPort we might be out of live nodes , This is dangerous !!!!");
                }
            }
        }
        return map;
    }

    public abstract InetAddressAndPort[] getStorageEndPoints(BigInteger token);

    /*
     * This method changes the ports of the endpoints from
     * the control port to the storage ports.
     */
    protected void retrofitPorts(List<InetAddressAndPort> eps)
    {
        //FIXME
//        for ( EndPoint ep : eps )
//        {
//            ep.setPort(DatabaseDescriptor.getStoragePort());
//        }
    }
}
