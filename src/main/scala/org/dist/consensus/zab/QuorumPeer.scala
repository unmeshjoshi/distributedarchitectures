package org.dist.consensus.zab

class QuorumPeer(config:QuorumPeerConfig) {
  private val myid = config.serverId
  private var state: ServerState.Value = ServerState.LOOKING

  @volatile private var running = true

  def getPeerState: ServerState.Value = state

  def setPeerState(newState: ServerState.Value): Unit = {
    state = newState
  }

  def lookForLeader(): Unit = {

  }

  def run() = {
    while (running) {
      state match {
        case ServerState.LOOKING ⇒ {
          try {
            lookForLeader()
          } catch {
            case _ ⇒ state = ServerState.LOOKING
          }
        }
        case ServerState.LEADING ⇒
        case ServerState.FOLLOWING ⇒
      }
    }
  }

  object ServerState extends Enumeration {
    type ServerState = Value
    val LOOKING, FOLLOWING, LEADING = Value
  }

  def getLastLoggedZxid():Long = {
    0
  }
}
