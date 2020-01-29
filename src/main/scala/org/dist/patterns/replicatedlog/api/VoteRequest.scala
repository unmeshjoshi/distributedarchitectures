package org.dist.patterns.replicatedlog.api

object RequestKeys {
  val RequestVoteKey: Short = 0
  val AppendEntriesKey: Short = 1
}

case class VoteRequest(serverId:Long, lastXid:Long)

case class VoteResponse(serverId:Long, lastXid:Long)