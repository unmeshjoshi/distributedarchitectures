package org.dist.consensus.zab

import org.dist.kvstore.InetAddressAndPort


case class QuorumPeerConfig(serverId: Long, electionAddress: InetAddressAndPort, serverAddress: InetAddressAndPort, servers: List[QuorumServer], val dataDir:String) {
  val syncLimit = 5

  val tickTime = 100

  val initLimit = 20
}
