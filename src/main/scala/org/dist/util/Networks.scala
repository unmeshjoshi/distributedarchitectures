package org.dist.util

import java.net.{Inet6Address, InetAddress, NetworkInterface}

import scala.jdk.CollectionConverters._

case class NetworkInterfaceNotFound(message: String) extends Exception(message)

class Networks(interfaceName: String, networkProvider: NetworkInterfaceProvider) {

  def this(interfaceName: String) = this(interfaceName, new NetworkInterfaceProvider)

  def this() = this("")

  def hostname(): String = ipv4Address.getHostAddress

  def ipv4Address: InetAddress =
    mappings
      .sortBy(_._1)
      .find(pair => isIpv4(pair._2))
      .getOrElse((0, InetAddress.getLocalHost))
      ._2

  private def isIpv4(addr: InetAddress): Boolean =
    !addr.isLoopbackAddress && !addr.isInstanceOf[Inet6Address]

  private def mappings: Seq[(Int, InetAddress)] =
    for {
      (index, inetAddresses) <- interfaces
      inetAddress            <- inetAddresses
    } yield (index, inetAddress)

  private def interfaces: Seq[(Int, List[InetAddress])] =
    if (interfaceName.isEmpty)
      networkProvider.allInterfaces
    else
      networkProvider.getInterface(interfaceName)

}

class NetworkInterfaceProvider {

  def allInterfaces: Seq[(Int, List[InetAddress])] = {
    NetworkInterface.getNetworkInterfaces.asScala.toList.map(iface => (iface.getIndex, iface.getInetAddresses.asScala.toList))
  }

  def getInterface(interfaceName: String): Seq[(Int, List[InetAddress])] =
    Option(NetworkInterface.getByName(interfaceName)) match {
      case Some(nic) => List((nic.getIndex, nic.getInetAddresses.asScala.toList))
      case None      => throw NetworkInterfaceNotFound(s"Network interface=$interfaceName not found.")
    }
}
