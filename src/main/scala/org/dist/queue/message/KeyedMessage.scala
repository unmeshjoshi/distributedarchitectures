package org.dist.queue.message

/**
 * A topic, key, and value
 */
case class KeyedMessage[K, V](val topic: String, val key: K, val message: V) {
  if(topic == null)
    throw new IllegalArgumentException("Topic cannot be null.")

  def this(topic: String, message: V) = this(topic, null.asInstanceOf[K], message)

  def hasKey = key != null
}
