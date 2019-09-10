/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dist.dbgossip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dist.dbgossip.Verb.GOSSIP_DIGEST_ACK;

public class GossipDigestSynVerbHandler extends GossipVerbHandler<GossipDigestSyn>
{
    public static final GossipDigestSynVerbHandler instance = new GossipDigestSynVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(GossipDigestSynVerbHandler.class);

    public void doVerb(Message<GossipDigestSyn> message)
    {
        InetAddressAndPort from = message.from();
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestSynMessage from {}", from);
        if (!Gossiper.instance.isEnabled())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
            return;
        }

        GossipDigestSyn gDigestMessage = message.payload;




        List<GossipDigest> gDigestList = gDigestMessage.getGossipDigests();

        if (logger.isTraceEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest gDigest : gDigestList)
            {
                sb.append(gDigest);
                sb.append(" ");
            }
            logger.trace("Gossip syn digests are : {}", sb);
        }

        List<GossipDigest> deltaGossipDigestList = new ArrayList<GossipDigest>();
        Map<InetAddressAndPort, EndpointState> deltaEpStateMap = new HashMap<InetAddressAndPort, EndpointState>();
        logger.trace("sending {} digests and {} deltas", deltaGossipDigestList.size(), deltaEpStateMap.size());
        Message<GossipDigestAck> gDigestAckMessage = Message.out(GOSSIP_DIGEST_ACK, new GossipDigestAck(deltaGossipDigestList, deltaEpStateMap));
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAckMessage to {}", from);
        MessagingService.instance().send(gDigestAckMessage, from);

        super.doVerb(message);
    }
}
