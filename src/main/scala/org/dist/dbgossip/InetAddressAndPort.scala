package org.dist.dbgossip

import java.net.InetAddress
import scala.collection.JavaConverters._

object InetAddressAndPort {
  private val HostKey = "host"
  private val PortKey = "port"
  private val TimestampKey = "timestamp"

  def deserialize(bytes:Array[Byte]): Option[InetAddressAndPort] = {
    val maybeValue = Json.parseBytes(bytes)
    maybeValue.map(jsonValue⇒ {
      val jsonObject = jsonValue.asJsonObject
      val hostIp = jsonObject(HostKey).to[String]
      val port = jsonObject(PortKey).to[Int]
      InetAddressAndPort(InetAddress.getByName(hostIp), port)
    })
  }

  def create(hostIp:String, port:Int) = {
    InetAddressAndPort(InetAddress.getByName(hostIp), port)
  }
}

case class InetAddressAndPort(address: InetAddress, port: Int) {
  def this() {
    this(null, 0)
  }

  import InetAddressAndPort._

  var defaultPort: Int = 7000

  def withPort(port: Int) = new InetAddressAndPort(address, port)

  def toJsonBytes(): Array[Byte] = {
    val jsonMap = collection.mutable.Map(
      HostKey -> address.getHostAddress,
      PortKey -> port,
      TimestampKey -> System.currentTimeMillis().toString
    )
    Json.encodeAsBytes(jsonMap.asJava)
  }

  def serialize():Array[Byte] = {
    toJsonBytes()
  }
}

