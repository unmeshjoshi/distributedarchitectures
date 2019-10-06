package org.dist.kvstore

import java.util.concurrent.atomic.AtomicInteger

object Message {
  private val messageId = new AtomicInteger(0)

  def nextId = {
    var id = 0L
    do id = messageId.incrementAndGet while ( {
      id == 0L
    })
    id
  }
}

case class Message(header:Header, payloadJson:String)
