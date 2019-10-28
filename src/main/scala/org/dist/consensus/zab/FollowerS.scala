package org.dist.consensus.zab

import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream, IOException}
import java.net.{InetSocketAddress, Socket}

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.Logging

import scala.util.control.Breaks

class FollowerS(val self:QuorumPeer) extends Logging {
  var zk:FollowerZookeeperServer = null
  var leaderOs:BinaryOutputArchive = null
  var leaderIs:BinaryInputArchive = null

  def followLeader() = {
    zk = new FollowerZookeeperServer(this)

    //setup processing pipeline
    //connect with leader
    // Find the leader by id
    val maybeAddress: Option[InetAddressAndPort] = self.getLeaderAddress()
    val address = maybeAddress match {
      case None ⇒ throw new RuntimeException("Can not find leader")
      case Some(address) ⇒ address
    }
    var sock = new Socket
    sock.setSoTimeout(self.config.tickTime * self.config.initLimit)
    Breaks.breakable {
      for (i ← 1 until 5) {
        try {
          info(s"Trying connecting to leader at ${address} attempt ${i} from ${self.myid}")
          sock.connect(new InetSocketAddress(address.address, address.port))
          sock.setTcpNoDelay(true)
          info("Connected successfully. Breaking out")
          Breaks.break()
        } catch {
          case io:IOException ⇒ {
            sock = new Socket
            sock.setSoTimeout(self.config.tickTime * self.config.initLimit)
          }
          case e:Exception ⇒ throw e
        }
        Thread.sleep(1000)
      }
    }

    leaderOs = new BinaryOutputArchive(new BufferedOutputStream(sock.getOutputStream))
    leaderIs = new BinaryInputArchive(new BufferedInputStream(sock.getInputStream()))

    info(s"Connected with leader ${address}. Receiving messages")

    val sendLastZxid = self.getLastLoggedZxid
    val qp = new QuorumPacket(Leader.LASTZXID, sendLastZxid)
    leaderOs.writeRecord(qp) //send lastzxid to leader


    val newLeaderPacket = leaderIs.readRecord()
    if (newLeaderPacket.recordType != Leader.NEWLEADER) {
      error("First packet should have been NEWLEADER")
      throw new IOException("First packet should have been NEWLEADER");
    };
    //TODO: read further packets to dump snapshot etc..before sending ACK
    val newLeaderZxid =  newLeaderPacket.zxid
    info(s"Sending ACK for newLeader request from ${self.config.serverId}")
    leaderOs.writeRecord(new QuorumPacket(Leader.ACK, newLeaderZxid & ~ 0xffffffffL))

    Breaks.breakable {
      while(self.running) {
        val packet: QuorumPacket = leaderIs.readRecord()
        packet.recordType match {
          case Leader.PING ⇒
            val response = packet.copy(data="PingResponse".getBytes())
            leaderOs.writeRecord(response)
          case Leader.PROPOSAL ⇒
            val txn: (TxnHeader, SetDataTxn) = Request.deserializeTxn(new ByteArrayInputStream(packet.data))
            zk.logRequest(txn._1, txn._2)
          case Leader.COMMIT ⇒
            val zxid = packet.zxid
            zk.commit(zxid)
          case _ ⇒ {

          }
        }
      }
    }
  }

  def writePacket (pp: QuorumPacket) = {
    leaderOs.synchronized {
      leaderOs.writeRecord(pp);
    }
  }
}
