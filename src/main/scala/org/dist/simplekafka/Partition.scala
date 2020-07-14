package org.dist.simplekafka

import java.io._

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class Partition(config:Config, topicAndPartition: TopicAndPartition) (implicit system:ActorSystem) {
  def checkEnoughReplicasReachOffset(requiredOffset: Long) = ???

  val LogFileSuffix = ".log"
  val logFile =
    new File(config.logDirs(0), topicAndPartition.topic + "-" + topicAndPartition.partition + LogFileSuffix)

  val sequenceFile = new SequenceFile()
  val reader = new sequenceFile.Reader(logFile.getAbsolutePath)
  val writer = new sequenceFile.Writer(logFile.getAbsolutePath)


  def makeFollower(leaderId:Int) = {
    //TODO: create  a fetcher
  }

  def makeLeader() = {

  }


  val source: Source[(String, String, Promise[Int]), ActorRef] = Source.actorRef(100, OverflowStrategy.dropHead)
  private val (actorRef, s) = source.preMaterialize()

  s.runForeach({ case (k, v, p) ⇒
    val index = append(k, v)
    p.success(index)
  })

  def append2(key: String, message: String) = {
    val p = Promise[Int]()
    actorRef ! (key, message, p)
    Await.result(p.future, 1.second)
  }

  def append(key:String, message:String) = {
    val currentPos = writer.getCurrentPosition
    try writer.append(key, message)
    catch {
      case e: IOException =>
        writer.seek(currentPos)
        throw e
    }
  }

  def read(offset:Long = 0) = {
    val result = new java.util.ArrayList[Row]()
    val offsets = sequenceFile.getAllOffSetsFrom(offset)
    offsets.foreach(offset ⇒ {
      val filePosition = sequenceFile.offsetIndexes.get(offset)

      val ba = new ByteArrayOutputStream()
      val baos = new DataOutputStream(ba)

      reader.seekToOffset(filePosition)
      reader.next(baos)

      val bais = new DataInputStream(new ByteArrayInputStream(ba.toByteArray))
      Try(Row.deserialize(bais)) match {
        case Success(row) => result.add(row)
        case Failure(exception) => None
      }
    })
    result.asScala.toList
  }


  object Row {
    def serialize(row: Row, dos:DataOutputStream): Unit = {
      dos.writeUTF(row.key)
      dos.writeInt(row.value.getBytes().size)
      dos.write(row.value.getBytes) //TODO: as of now only supporting string writes.
    }

    def deserialize(dis: DataInputStream): Row = {
      val key = dis.readUTF()
      val dataSize = dis.readInt()
      val bytes = new Array[Byte](dataSize)
      dis.read(bytes)
      val value = new String(bytes) //TODO:As of now supporting only string values
      Row(key, value)
    }
  }

  case class Row(key: String, value: String)
}
