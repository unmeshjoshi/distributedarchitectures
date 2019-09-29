package org.dist.queue

import java.io.File
import java.net.ServerSocket
import java.util.Random

import org.dist.util.Networks

object TestUtils {

  /* A consistent random number generator to make tests repeatable */
  val seededRandom = new Random(192348092834L)
  val random = new Random()

  def hostName() = new Networks().hostname()

  /**
   * Choose a number of random available ports
   */
  def choosePorts(count: Int): List[Int] = {
    val sockets =
      for(i <- 0 until count)
        yield new ServerSocket(0)
    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList.map(_.close)
    ports
  }

  /**
   * Choose an available port
   */
  def choosePort(): Int = choosePorts(1).head

  def tempDirWithName(name:String): File = {
    val ioDir = System.getProperty("java.io.tmpdir")
    val f = new File(ioDir, "kafka-" + name)
    f.mkdirs()
//    f.deleteOnExit()
    f
  }

  def tempDir(): File = {
    val ioDir = System.getProperty("java.io.tmpdir")
    val f = new File(ioDir, "kafka-" + random.nextInt(1000000))
    f.mkdirs()
    f.deleteOnExit()
    f
  }

  /**
   * Create a temporary file
   */
  def tempFile(): File = {
    val f = File.createTempFile("kafka", ".tmp")
    f.deleteOnExit()
    f
  }
}
