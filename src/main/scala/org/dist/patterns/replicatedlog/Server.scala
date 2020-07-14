package org.dist.patterns.replicatedlog

import java.io.{ByteArrayInputStream, File}
import java.util.concurrent.atomic.AtomicReference

import org.dist.consensus.zab.Vote
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.api.{RequestKeys, VoteRequest, VoteResponse}
import org.dist.patterns.replicatedlog.heartbeat.{HeartBeatScheduler, Peer, PeerProxy}
import org.dist.patterns.wal.{SetValueCommand, WalEntry}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.rapid.SocketClient

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

case class Config(serverId: Long, serverAddress: InetAddressAndPort, peerConfig:List[Peer], walDir:File) {}

class Server(config:Config) extends Thread with Logging {
  val myid = config.serverId

  var commitIndex:Long = 0

  val kv = new mutable.HashMap[String, String]()

  def put(key:String, value:String) = {
    if (leader == null) throw new RuntimeException("Can not propose to non leader")

    //propose is synchronous as of now so value will be applied
    leader.propose(SetValueCommand(key, value))

    kv.get(key)
  }

  def get(key: String) = kv.get(key)
  def setPeerState(newState: ServerState.Value): Unit = {
    state = newState
  }

  def peers() = {
    config.peerConfig.filter(p ⇒ p.id != myid)
  }

  val wal = new ReplicatedWal(config.walDir)
  val entries = wal.readAll()

  @volatile var state: ServerState.Value = ServerState.LOOKING
  @volatile var running = true
  val currentVote = new AtomicReference(Vote(myid, wal.lastLogEntryId))

  def startListening() =  new TcpListener(config.serverAddress, requestHandler).start()

  val electionTimeoutChecker = new HeartBeatScheduler(heartBeatCheck)

  def startFollowing(): Unit = {
    while(this.state == ServerState.FOLLOWING) {
      Thread.sleep(100)
    }
  }

  override def run(): Unit = {
    while (running) {
      state match {
        case ServerState.LOOKING ⇒ {
          try {
            val electionResult = new LeaderElection(config.peerConfig, new SocketClient(), this).lookForLeader()
            electionTimeoutChecker.cancel()
          } catch {
            case e: Exception ⇒ {
              e.printStackTrace()
              state = ServerState.LOOKING
            }
          }
        }
        case ServerState.LEADING ⇒ {
          electionTimeoutChecker.cancel()

//          info(s"Server ${myid} now leading")
          this.leader = new Leader(config.peerConfig, new SocketClient(), this)
          this.leader.startLeading()
        }
        case ServerState.FOLLOWING ⇒ {
//          info(s"Server ${myid} now following")
          electionTimeoutChecker.cancel()
          electionTimeoutChecker.startWithRandomInterval()
          startFollowing()
        }
      }
    }
  }

  var leader:Leader = _

  var heartBeatReceived = false

  def handleHeartBeatTimeout() = {
      this.state == ServerState.LOOKING
  }

  def heartBeatCheck() = {
    info("Checking if heartbeat received")
    if(!heartBeatReceived) {
      handleHeartBeatTimeout()
    } else {
      heartBeatReceived = false //reset
    }
  }

  def handleAppendEntries(appendEntryRequest: AppendEntriesRequest)  = {
    heartBeatReceived = true

    if (appendEntryRequest.data.size == 0) { //this is heartbeat
      updateCommitIndex(appendEntryRequest)
      AppendEntriesResponse(this.wal.lastLogEntryId, true)

    } else if (this.wal.lastLogEntryId >= appendEntryRequest.xid) {
      AppendEntriesResponse(this.wal.lastLogEntryId, false)

    } else {
      this.wal.append(appendEntryRequest.data)
      updateCommitIndex(appendEntryRequest)
    }
  }

  private def updateCommitIndex(appendEntryRequest: AppendEntriesRequest) = {
    if (this.commitIndex < appendEntryRequest.commitIndex) {
      updateCommitIndexAndApplyEntries(appendEntryRequest.commitIndex)
    }
    AppendEntriesResponse(this.wal.lastLogEntryId, true)
  }

  def applyEntries(entries: ListBuffer[WalEntry]) = {
    entries.foreach(entry ⇒ {
      val command = SetValueCommand.deserialize(new ByteArrayInputStream(entry.data))
      info(s"Setting ${command.key} => ${command.value}")
      kv.put(command.key, command.value)
    })
  }

  def updateCommitIndexAndApplyEntries(index:Long) = {
    val previousCommitIndex = commitIndex
    commitIndex = index
    wal.highWaterMark = commitIndex
    info(s"Applying wal entries in ${myid} from ${previousCommitIndex} to ${commitIndex}")
    val entries = wal.entries(previousCommitIndex, commitIndex)
    applyEntries(entries)
  }

  def requestHandler(request:RequestOrResponse) = {
    if (request.requestId == RequestKeys.RequestVoteKey) {
      val vote = VoteResponse(currentVote.get().id, currentVote.get().zxid)
      info(s"Responding vote response from ${myid} be ${currentVote}")
      RequestOrResponse(RequestKeys.RequestVoteKey, JsonSerDes.serialize(vote), request.correlationId)


    } else if (request.requestId == RequestKeys.AppendEntriesKey) {

      val appendEntries = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[AppendEntriesRequest])
      val appendEntriesResponse = handleAppendEntries(appendEntries)
      info(s"Responding AppendEntriesResponse from ${myid} be ${appendEntriesResponse}")
      RequestOrResponse(RequestKeys.AppendEntriesKey, JsonSerDes.serialize(appendEntriesResponse), request.correlationId)

    } else throw new RuntimeException("UnknownRequest")

  }
}

case class AppendEntriesRequest(xid:Long, data:Array[Byte], commitIndex:Long)
case class AppendEntriesResponse(xid:Long, success:Boolean)

class Leader(allServers:List[Peer], client:SocketClient, val self:Server) extends Logging {
  val peerProxies = self.peers().map(p ⇒ PeerProxy(p, client, 0, sendHeartBeat))
  def startLeading() = {
    peerProxies.foreach(_.start())
    while(self.state == ServerState.LEADING) {
      Thread.sleep(100)
    }
  }

  def sendHeartBeat(peerProxy:PeerProxy) = {
    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(0, Array[Byte](), self.wal.highWaterMark))
    val request = RequestOrResponse(RequestKeys.AppendEntriesKey, appendEntries, 0)
    //sendHeartBeat
    val response = client.sendReceive(request, peerProxy.peerInfo.address)
    //TODO: Handle response
    val appendOnlyResponse: AppendEntriesResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AppendEntriesResponse])
    if (appendOnlyResponse.success) {
      peerProxy.matchIndex = appendOnlyResponse.xid
    } else {
      // TODO: handle term and failures
    }
  }

  def stopLeading() = peerProxies.foreach(_.stop())

  var lastEntryId: Long = self.wal.lastLogEntryId


  def propose(setValueCommand:SetValueCommand) = {
    val data = setValueCommand.serialize()

    appendToLocalLog(data)

    broadCastAppendEntries(data)
  }

  private def findMaxIndexWithQuorum = {
    val matchIndexes = peerProxies.map(p ⇒ p.matchIndex)
    val sorted: Seq[Long] = matchIndexes.sorted
    val matchIndexAtQuorum = sorted((allServers.size - 1) / 2)
    matchIndexAtQuorum
  }

  private def broadCastAppendEntries(data: Array[Byte]) = {
    val request = appendEntriesRequestFor(data)

    //TODO: Happens synchronously for demo. Has to be async with each peer having its own thread
    peerProxies.map(peer ⇒ {
      val response = client.sendReceive(request, peer.peerInfo.address)
      val appendEntriesResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AppendEntriesResponse])

      peer.matchIndex = appendEntriesResponse.xid

      val matchIndexAtQuorum = findMaxIndexWithQuorum


      info(s"Peer match indexes are at ${allServers}")
      info(s"CommitIndex from quorum is ${matchIndexAtQuorum}")

      if (self.commitIndex < matchIndexAtQuorum) {
        self.updateCommitIndexAndApplyEntries(matchIndexAtQuorum)
      }

    })
  }

  private def appendEntriesRequestFor(data: Array[Byte]) = {
    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(lastEntryId, data, self.commitIndex))
    val request = RequestOrResponse(RequestKeys.AppendEntriesKey, appendEntries, 0)
    request
  }

  private def appendToLocalLog(data:Array[Byte]) = {
    lastEntryId = self.wal.append(data)
  }
}

