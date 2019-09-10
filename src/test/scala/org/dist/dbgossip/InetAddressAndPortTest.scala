package org.dist.dbgossip

import java.net.InetAddress

import org.scalatest.FunSuite

class InetAddressAndPortTest extends FunSuite {

  test("should serialize to json bytes and deserialize to InetAddress object") {
    val host = new Networks().ipv4Address
    println(host)
    val address = InetAddressAndPort(host, 8080)
    val bytes = address.serialize()
    val maybeAddress = InetAddressAndPort.deserialize(bytes)
    assert(Some(address).equals(maybeAddress))
  }
}
