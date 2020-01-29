package org.dist.patterns.replicatedlog

import java.io.File
import java.util.concurrent.atomic.AtomicReference

import org.dist.consensus.zab.Vote
import org.dist.kvstore.InetAddressAndPort

object ServerState extends Enumeration {
  type ServerState = Value
  val LOOKING, FOLLOWING, LEADING = Value
}

case class ServerConfig(id:Int, address:InetAddressAndPort)
case class Config(serverId: Long, serverAddress: InetAddressAndPort, servers:List[ServerConfig], walDir:File) {}

class Server(config:Config) extends Thread {
  val myid = config.serverId
  def setPeerState(newState: ServerState.Value): Unit = {
    state = newState
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
            val electionResult = new LeaderElection(config.servers, new Client(), this).lookForLeader()

          } catch {
            case e: Exception ⇒ {
              e.printStackTrace()
              state = ServerState.LOOKING
            }
          }
        }
      }
    }
  }
}

