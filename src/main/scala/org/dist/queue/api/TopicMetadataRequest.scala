package org.dist.queue.api

case class TopicMetadataRequest(val versionId: Short,
                                val correlationId: Int,
                                val clientId: String,
                                val topics: Seq[String])

object TopicMetadataRequest {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""

}