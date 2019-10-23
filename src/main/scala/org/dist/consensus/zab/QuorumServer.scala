package org.dist.consensus.zab

import org.dist.kvstore.InetAddressAndPort

case class QuorumServer(id:Int, electionAddress:InetAddressAndPort, serverAddress:InetAddressAndPort) {
  def this(id:Int, hostName:String, electionPort:Int, serverPort:Int) {
    this(id, InetAddressAndPort.create(hostName, electionPort), InetAddressAndPort.create(hostName, serverPort))
  }
}
