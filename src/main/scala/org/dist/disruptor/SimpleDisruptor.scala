package org.dist.disruptor

class LongEvent(var value:Long) {
  def set(l:Long):Unit = {
    value = l
  }

  override def toString = s"LongEvent(${value})"
}

import java.nio.ByteBuffer

import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.util.DaemonThreadFactory
import com.lmax.disruptor.{EventFactory, EventHandler, RingBuffer}

class LongEventFactory extends EventFactory[LongEvent] {
  def newInstance = new LongEvent(0)
}


class LongEventHandler extends EventHandler[LongEvent] {
  def onEvent(event: LongEvent, sequence: Long, endOfBatch: Boolean): Unit = {
    System.out.println("Event: " + event)
  }
}


class LongEventProducer(val ringBuffer: RingBuffer[LongEvent]) {
  def onData(bb: ByteBuffer): Unit = {
    val sequence = ringBuffer.next // Grab the next sequence
    try {
      val event = ringBuffer.get(sequence) // Get the entry in the Disruptor
      // for the sequence
      event.set(bb.getLong(0)) // Fill with data
    } finally ringBuffer.publish(sequence)
  }
}

object SimpleDisruptor extends App {

    val bufferSize = 1024
    val factory = new LongEventFactory
    // Construct the Disruptor
    val disruptor = new Disruptor(factory, bufferSize, DaemonThreadFactory.INSTANCE)
    // Connect the handler
    disruptor.handleEventsWith(new LongEventHandler())
    // Start the Disruptor, starts all threads running
    disruptor.start

    import java.nio.ByteBuffer
    val ringBuffer = disruptor.getRingBuffer

    val producer = new LongEventProducer(ringBuffer)

    val bb = ByteBuffer.allocate(8)
    var l = 0
    while ( {
      true
    }) {
      bb.putLong(0, l)
      producer.onData(bb)
      Thread.sleep(1000)

      l += 1
    }
}
