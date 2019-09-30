/**
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

package org.dist.kvstore.locator;

import org.dist.kvstore.InetAddressAndPort;

import java.net.UnknownHostException;


public class EndPointSnitch implements IEndPointSnitch
{
    public boolean isOnSameRack(InetAddressAndPort host, InetAddressAndPort host2) throws UnknownHostException
    {
        /*
         * Look at the IP Address of the two hosts. Compare 
         * the 3rd octet. If they are the same then the hosts
         * are in the same rack else different racks. 
        */        
        byte[] ip = host.address().getAddress();
        byte[] ip2 = host2.address().getAddress();
        
        return ( ip[2] == ip2[2] );
    }
    
    public boolean isInSameDataCenter(InetAddressAndPort host, InetAddressAndPort host2) throws UnknownHostException
    {
        /*
         * Look at the IP Address of the two hosts. Compare 
         * the 2nd octet. If they are the same then the hosts
         * are in the same datacenter else different datacenter. 
        */
        byte[] ip = host.address().getAddress();
        byte[] ip2 = host2.address().getAddress();
        
        return ( ip[1] == ip2[1] );
    }
}
