package org.dist.kvstore

import java.util.concurrent.atomic.AtomicInteger

object Message {
  private val messageId = new AtomicInteger(0)

  def nextId = {
    GuidGenerator.guid
  }
}

case class Message(header:Header, payloadJson:String)
