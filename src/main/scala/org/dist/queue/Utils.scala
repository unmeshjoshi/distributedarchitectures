package org.dist.queue

import java.io.File

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Seq, mutable}

object Utils {
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
