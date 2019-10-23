package org.dist.consensus.zab

import java.io.{BufferedInputStream, BufferedOutputStream, IOException}
import java.net.{InetSocketAddress, Socket}

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.common.Logging

import scala.util.control.Breaks

class FollowerS(val self:QuorumPeer) extends Logging {

  def followLeader() = {
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

    val leaderOs = new BinaryOutputArchive(new BufferedOutputStream(sock.getOutputStream))
    val leaderIs = new BinaryInputArchive(new BufferedInputStream(sock.getInputStream()))

    info(s"Connected with leader ${address}. Receiving messages")

    Breaks.breakable {
      while(self.running) {
        val packet = leaderIs.readRecord()
        info(s"Responding to packet ${packet}")
        packet.recordType match {
          case Leader.NEWLEADER ⇒
            info("Responding to NEWLEADER request")
            val latestZxidPacket = QuorumPacket(Leader.LASTZXID, 0, Array[Byte]())
            leaderOs.writeRecord(latestZxidPacket)
          case Leader.PING ⇒
            val response = packet.copy(data="PingResponse".getBytes())
            leaderOs.writeRecord(response)
          case _ ⇒ {
            println(s"responding to ${packet}")
          }
        }
      }
    }

  }
}
