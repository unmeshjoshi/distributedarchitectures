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

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.ISO_8859_1;


/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster.
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 * <p>
 * e.g. if we want to disseminate load information for node A do the following:
 * </p>
 * <pre>
 * {@code
 * ApplicationState loadState = new ApplicationState(<string representation of load>);
 * Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 * }
 * </pre>
 */

public class VersionedValue implements Comparable<VersionedValue>
{

    // this must be a char that cannot be present in any token
    public final static char DELIMITER = ',';
    public final static String DELIMITER_STR = new String(new char[]{ DELIMITER });

    // values for ApplicationState.STATUS
    public final static String STATUS_BOOTSTRAPPING = "BOOT";
    public final static String STATUS_BOOTSTRAPPING_REPLACE = "BOOT_REPLACE";
    public final static String STATUS_NORMAL = "NORMAL";
    public final static String STATUS_LEAVING = "LEAVING";
    public final static String STATUS_LEFT = "LEFT";
    public final static String STATUS_MOVING = "MOVING";

    public final static String REMOVING_TOKEN = "removing";
    public final static String REMOVED_TOKEN = "removed";

    public final static String HIBERNATE = "hibernate";
    public final static String SHUTDOWN = "shutdown";

    // values for ApplicationState.REMOVAL_COORDINATOR
    public final static String REMOVAL_COORDINATOR = "REMOVER";

    public final int version;
    public final String value;

    private VersionedValue(String value, int version)
    {
        assert value != null;
        // blindly interning everything is somewhat suboptimal -- lots of VersionedValues are unique --
        // but harmless, and interning the non-unique ones saves significant memory.  (Unfortunately,
        // we don't really have enough information here in VersionedValue to tell the probably-unique
        // values apart.)  See CASSANDRA-6410.
        this.value = value.intern();
        this.version = version;
    }

    private VersionedValue(String value)
    {
        this(value, VersionGenerator.getNextVersion());
    }

    public int compareTo(VersionedValue value)
    {
        return this.version - value.version;
    }

    @Override
    public String toString()
    {
        return "Value(" + value + "," + version + ")";
    }

    public byte[] toBytes()
    {
        return value.getBytes(ISO_8859_1);
    }

    private static String versionString(String... args)
    {
        return StringUtils.join(args, VersionedValue.DELIMITER);
    }

    public static class VersionedValueFactory
    {
        final IPartitioner partitioner;

        public VersionedValueFactory(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }
        
        public VersionedValue cloneWithHigherVersion(VersionedValue value)
        {
            return new VersionedValue(value.value);
        }

        @Deprecated
        public VersionedValue bootReplacing(InetAddress oldNode)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING_REPLACE, oldNode.getHostAddress()));
        }

        public VersionedValue bootReplacingWithPort(InetAddressAndPort oldNode)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING_REPLACE, oldNode.toString()));
        }

        public VersionedValue bootstrapping(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING,
                                                    makeTokenString(tokens)));
        }

        public VersionedValue normal(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_NORMAL,
                                                    makeTokenString(tokens)));
        }

        private String makeTokenString(Collection<Token> tokens)
        {
            return partitioner.getTokenFactory().toString(Iterables.get(tokens, 0));
        }

        public VersionedValue leaving(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEAVING,
                                                    makeTokenString(tokens)));
        }

        public VersionedValue left(Collection<Token> tokens, long expireTime)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEFT,
                                                    makeTokenString(tokens),
                                                    Long.toString(expireTime)));
        }

        public VersionedValue moving(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_MOVING + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public VersionedValue hostId(UUID hostId)
        {
            return new VersionedValue(hostId.toString());
        }

        public VersionedValue tokens(Collection<Token> tokens)
        {
            Map map = new HashMap();
            map.put("tokens", tokens);
            return new VersionedValue(Json.encodeAsString(map));
        }

        public VersionedValue removingNonlocal(UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVING_TOKEN, hostId.toString()));
        }

        public VersionedValue removedNonlocal(UUID hostId, long expireTime)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVED_TOKEN, hostId.toString(), Long.toString(expireTime)));
        }

        public VersionedValue removalCoordinator(UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVAL_COORDINATOR, hostId.toString()));
        }

        public VersionedValue hibernate(boolean value)
        {
            return new VersionedValue(VersionedValue.HIBERNATE + VersionedValue.DELIMITER + value);
        }

        public VersionedValue rpcReady(boolean value)
        {
            return new VersionedValue(String.valueOf(value));
        }

        public VersionedValue shutdown(boolean value)
        {
            return new VersionedValue(VersionedValue.SHUTDOWN + VersionedValue.DELIMITER + value);
        }

        public VersionedValue datacenter(String dcId)
        {
            return new VersionedValue(dcId);
        }

        public VersionedValue rack(String rackId)
        {
            return new VersionedValue(rackId);
        }

        public VersionedValue rpcaddress(InetAddress endpoint)
        {
            return new VersionedValue(endpoint.getHostAddress());
        }

        public VersionedValue nativeaddressAndPort(InetAddressAndPort address)
        {
            return new VersionedValue(address.toString());
        }
    }
}
