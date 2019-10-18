package org.dist.consensus.zab

import org.dist.kvstore.InetAddressAndPort

case class QuorumPeerConfig(serverId:Long, electionAddress:InetAddressAndPort, serverAddress:InetAddressAndPort, servers:List[QuorumServer]) {

}
