package org.dist.kvstore.locator

import java.math.BigInteger

import org.dist.kvstore.{InetAddressAndPort, TokenMetadata}
import org.scalatest.FunSuite

class RackUnawareStrategyTest extends FunSuite {

  test("testGetStorageEndPoints") {
    val tokenMetadata = new TokenMetadata()
    tokenMetadata.update(new BigInteger("1"), InetAddressAndPort.create("10.10.10.10", 8000))
    tokenMetadata.update(new BigInteger("2"), InetAddressAndPort.create("10.10.10.11", 8000))
    tokenMetadata.update(new BigInteger("3"), InetAddressAndPort.create("10.10.10.12", 8000))
    val rackUnawareStrategy = new RackUnawareStrategy(tokenMetadata)

    val endpoints = rackUnawareStrategy.getStorageEndPoints(new BigInteger("5"))
    assert(2 == endpoints.size)
    println(endpoints)
  }
}
