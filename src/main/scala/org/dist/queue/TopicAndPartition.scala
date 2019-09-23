package org.dist.queue

case class TopicAndPartition(topic: String, partition: Int) {

  def this(tuple: (String, Int)) = this(tuple._1, tuple._2)

  def asTuple = (topic, partition)

  override def toString = "[%s,%d]".format(topic, partition)
}


