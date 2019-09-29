package org.dist.queue.api

import org.dist.queue.{ErrorMapping, MessageSet, TopicAndPartition}
import scala.collection.Map

object FetchResponsePartitionData {
  val headerSize =
    2 + /* error code */
      8 + /* high watermark */
      4 /* messageSetSize */
}
case class FetchResponsePartitionData(error: Short = ErrorMapping.NoError, hw: Long = -1L, messages: MessageSet) {

  val sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes

  def this(messages: MessageSet) = this(ErrorMapping.NoError, -1L, messages)

}

object FetchResponse {

  val headerSize =
    4 + /* correlationId */
      4 /* topic count */
}

case class FetchResponse(correlationId: Int,
                         data: Map[TopicAndPartition, FetchResponsePartitionData])  {



}
