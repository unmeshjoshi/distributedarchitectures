package org.dist.queue.common

object ErrorMapping {
  def codeFor(value: Class[Throwable]): Short = UnknownCode

  val UnknownCode : Short = -1
  val NoError : Short = 0
  val OffsetOutOfRangeCode : Short = 1
  val InvalidMessageCode : Short = 2
  val UnknownTopicOrPartitionCode : Short = 3
  val InvalidFetchSizeCode  : Short = 4
  val LeaderNotAvailableCode : Short = 5
  val NotLeaderForPartitionCode : Short = 6
  val RequestTimedOutCode: Short = 7
  val BrokerNotAvailableCode: Short = 8
  val ReplicaNotAvailableCode: Short = 9
  val MessageSizeTooLargeCode: Short = 10
  val StaleControllerEpochCode: Short = 11
}
