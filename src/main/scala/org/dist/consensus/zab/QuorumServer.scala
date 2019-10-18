package org.dist.consensus.zab

import org.dist.kvstore.InetAddressAndPort

case class QuorumServer(electionAddress:InetAddressAndPort, serverAddress:InetAddressAndPort) {
  def this(hostName:String, electionPort:Int, serverPort:Int) {
    this(InetAddressAndPort.create(hostName, electionPort), InetAddressAndPort.create(hostName, serverPort))
  }
}
