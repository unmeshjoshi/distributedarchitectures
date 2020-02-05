package org.dist.patterns.replicatedlog

import java.io.{ByteArrayInputStream, File}
import java.util.concurrent.atomic.AtomicReference

import org.dist.consensus.zab.Vote
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.patterns.replicatedlog.api.{RequestKeys, VoteRequest}
import org.dist.patterns.wal.{SetValueCommand, WalEntry}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

case class Peer(id:Int, address:InetAddressAndPort, var matchIndex:Long = 0) {
}

case class Config(serverId: Long, serverAddress: InetAddressAndPort, peerConfig:List[Peer], walDir:File) {}

class Server(config:Config) extends Thread with Logging {
  val kv = new mutable.HashMap[String, String]()

  def put(key:String, value:String) = {
    if (leader == null) throw new RuntimeException("Can not propose to non leader")

    //propose is synchronous as of now so value will be applied
    leader.propose(SetValueCommand(key, value))

    kv.get(key)
  }

  def get(key: String) = kv.get(key)

  var commitIndex:Long = 0

  val myid = config.serverId
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

  def startListening() =  new TcpListener(config.serverAddress, this).start()

  override def run(): Unit = {
    while (running) {
      state match {
        case ServerState.LOOKING ⇒ {
          try {
            val electionResult = new LeaderElection(config.peerConfig, new Client(), this).lookForLeader()

          } catch {
            case e: Exception ⇒ {
              e.printStackTrace()
              state = ServerState.LOOKING
            }
          }
        }
        case ServerState.LEADING ⇒ {
//          info(s"Server ${myid} now leading")
          this.leader = new Leader(config.peerConfig, new Client(), this)

        }
        case ServerState.FOLLOWING ⇒ {
//          info(s"Server ${myid} now following")
        }
      }
    }
  }

  var leader:Leader = _


  def handleAppendEntries(appendEntryRequest: AppendEntriesRequest)  = {
    if (this.wal.lastLogEntryId >= appendEntryRequest.xid) {
      AppendEntriesResponse(this.wal.lastLogEntryId, false)
    } else {
      this.wal.append(appendEntryRequest.data)
      if (this.commitIndex < appendEntryRequest.commitIndex) {
        updateCommitIndexAndApplyEntries(appendEntryRequest.commitIndex)
      }
      AppendEntriesResponse(this.wal.lastLogEntryId, true)
    }
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
    info(s"Applying wal entries in ${myid} from ${previousCommitIndex} to ${commitIndex}")
    val entries = wal.entries(previousCommitIndex, commitIndex)
    applyEntries(entries)
  }
}

case class AppendEntriesRequest(xid:Long, data:Array[Byte], commitIndex:Long)
case class AppendEntriesResponse(xid:Long, success:Boolean)

class Leader(allServers:List[Peer], client:Client, self:Server) extends Logging {
  var lastEntryId: Long = self.wal.lastLogEntryId


  def updatePeerMatchIndexes(responses: Seq[(Peer, AppendEntriesResponse)]) = {
    responses.foreach({
      case tuple:(Peer, AppendEntriesResponse) ⇒ {
        tuple._1.matchIndex = tuple._2.xid
      }
    })
  }


  def propose(setValueCommand:SetValueCommand) = {
    val data = setValueCommand.serialize()
    lastEntryId = self.wal.append(data)
    val localServer = allServers.filter(p ⇒ p.id == self.myid)(0)
    localServer.matchIndex = lastEntryId

    val appendEntries = JsonSerDes.serialize(AppendEntriesRequest(lastEntryId, data, self.commitIndex))
    val request = RequestOrResponse(RequestKeys.AppendEntriesKey, appendEntries, 0)
    val peers = self.peers()
    val responses: Seq[(Peer, AppendEntriesResponse)] = peers.map(peer ⇒ {
      val response = client.sendReceive(request, peer.address)
      val appendEntriesResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[AppendEntriesResponse])
      (peer, appendEntriesResponse)
    })

    updatePeerMatchIndexes(responses)

    val appendEntryResponses = responses.map(t ⇒ t._2)

    val matchIndexes = allServers.map(p ⇒ p.matchIndex)
    val sorted: Seq[Long] = matchIndexes.sorted
    val matchIndexAtQuorum = sorted((allServers.size - 1) / 2)

    info(s"Peer match indexes are at ${allServers}")
    info(s"CommitIndex from quorum is ${matchIndexAtQuorum}")

    if (self.commitIndex < matchIndexAtQuorum) {
      self.updateCommitIndexAndApplyEntries(matchIndexAtQuorum)
    }
  }
}

