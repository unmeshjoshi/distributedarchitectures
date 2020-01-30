package org.dist.patterns.replicatedlog

import java.net.Socket

import org.dist.consensus.zab.{ElectionResult, Elector, Vote}
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.api.{RequestKeys, VoteRequest, VoteResponse}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.util.SocketIO

import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

class LeaderElection(servers:List[Peer], client:Client, self:Server) extends Logging {

  private def setLeaderOrFollowerState(electionResult: ElectionResult) = {
    //set state as leader
    self.currentVote.set(electionResult.winningVote)
    if (electionResult.winningVote.id == self.myid) {
      info(s"Setting ${electionResult.winningVote.id} to be leader")
      self.setPeerState(ServerState.LEADING)
    } else {
      info(s"Setting ${self.myid} to be follower of ${electionResult.winningVote.id}")
      self.setPeerState(ServerState.FOLLOWING)
    }
  }


  private def setCurrentVoteToWouldBeLeaderVote(electionResult: ElectionResult) = {
    info(s"Setting current vote in ${self.myid} to ${electionResult.vote}")
    self.currentVote.set(electionResult.vote)
  }

  def lookForLeader(): Unit = {
    self.currentVote.set(Vote(self.myid, self.wal.lastLogEntryId))

    breakable {

      while (true) {

        val votes = getVotesFromPeers
        val electionResult = new Elector(servers.size).elect(votes.asScala.toMap)

        if (electionResult.isElected()) {
          setLeaderOrFollowerState(electionResult)

          Thread.sleep(100) //TODO: Why?
          break

        } else {

          setCurrentVoteToWouldBeLeaderVote(electionResult)
        }

        info(s"${self.myid} Waiting for leader to be selected: " + votes)
        Thread.sleep(1000)
      }

    }
  }

  private def getVotesFromPeers() = {
    val votes = new java.util.HashMap[InetAddressAndPort, Vote]
    self.peers().foreach(peer ⇒ {
      val request = VoteRequest(self.myid, self.wal.lastLogEntryId)
      val voteRequest = RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(request), 0)
      val response = client.sendReceive(voteRequest, peer.address)
      val maybeVote = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[VoteResponse])
      votes.put(peer.address, Vote(maybeVote.serverId, maybeVote.lastXid))
    })
    votes
  }

}

class Client {
  def sendReceive(requestOrResponse: RequestOrResponse, to:InetAddressAndPort):RequestOrResponse = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).requestResponse(requestOrResponse)
  }
}

