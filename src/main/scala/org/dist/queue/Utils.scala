package org.dist.queue

import java.io.{EOFException, File}
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

import org.dist.queue.network.Processor

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Seq, mutable}

object Utils extends Logging {
  /**
   * Create a daemon thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  def daemonThread(runnable: Runnable): Thread =
    newThread(runnable, true)

  /**
   * Create a daemon thread
   * @param name The name of the thread
   * @param runnable The runnable to execute in the background
   * @return The unstarted thread
   */
  def daemonThread(name: String, runnable: Runnable): Thread =
    newThread(name, runnable, true)

  /**
   * Read some bytes into the provided buffer, and return the number of bytes read. If the
   * channel has been closed or we get -1 on the read for any reason, throw an EOFException
   */
  def read(channel: ReadableByteChannel, buffer: ByteBuffer): Int = {
    channel.read(buffer) match {
      case -1 => throw new EOFException("Received -1 when reading from channel, socket has likely been closed.")
      case n: Int => n
    }
  }

  /**
   * Create a new thread
   *
   * @param name The name of the thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  def newThread(name: String, runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable, name)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      }
    })
    thread
  }

  /**
   * Create a new thread
   * @param runnable The work for the thread to do
   * @param daemon Should the thread block JVM shutdown?
   * @return The unstarted thread
   */
  def newThread(runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      def uncaughtException(t: Thread, e: Throwable) {
        error("Uncaught exception in thread '" + t.getName + "':", e)
      }
    })
    thread
  }


  /**
   * Format a Seq[String] as JSON array.
   */
  def seqToJson(jsonData: Seq[String], valueInQuotes: Boolean): String = {
    val builder = new StringBuilder
    builder.append("[ ")
    if (valueInQuotes)
      builder.append(jsonData.map("\"" + _ + "\"").mkString(", "))
    else
      builder.append(jsonData.mkString(", "))
    builder.append(" ]")
    builder.toString
  }

  /**
   * Format a Map[String, Seq[Int]] as JSON
   */

  def mapWithSeqValuesToJson(jsonDataMap: Map[String, Seq[Int]]): String = {
    mergeJsonFields(mapToJsonFields(jsonDataMap.map(e => (e._1 -> seqToJson(e._2.map(_.toString), valueInQuotes = false))),
      valueInQuotes = false))
  }

  /**
   * Merge JSON fields of the format "key" : value/object/array.
   */
  def mergeJsonFields(objects: Seq[String]): String = {
    val builder = new StringBuilder
    builder.append("{ ")
    builder.append(objects.sorted.map(_.trim).mkString(", "))
    builder.append(" }")
    builder.toString
  }

  /**
   * Format a Map[String, String] as JSON object.
   */
  def mapToJsonFields(jsonDataMap: Map[String, String], valueInQuotes: Boolean): Seq[String] = {
    val jsonFields: mutable.ListBuffer[String] = ListBuffer()
    val builder = new StringBuilder
    for ((key, value) <- jsonDataMap.toList.sorted) {
      builder.append("\"" + key + "\":")
      if (valueInQuotes)
        builder.append("\"" + value + "\"")
      else
        builder.append(value)
      jsonFields += builder.toString
      builder.clear()
    }
    jsonFields
  }

  /**
   * Format a Map[String, String] as JSON object.
   */
  def mapToJson(jsonDataMap: Map[String, String], valueInQuotes: Boolean): String = {
    mergeJsonFields(mapToJsonFields(jsonDataMap, valueInQuotes))
  }

  def swallow(log: (Object, Throwable) => Unit, action: => Unit) = {
    try {
      action
    } catch {
      case e: Throwable => log(e.getMessage(), e)
    }
  }
  def rm(file: File) {
    if (file == null) {
      return
    } else if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        for (f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }
}
