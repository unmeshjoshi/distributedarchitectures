package org.dist.kvstore

import java.math.BigInteger
import java.util
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

class TokenMetadata {
  def getToken(ep: InetAddressAndPort) = {
    InetAddressAndPortToTokenMap.get(ep)
  }

  /* Use this lock for manipulating the token map */
  private val lock = new ReentrantReadWriteLock(true)
  private var tokenToInetAddressAndPortMap = new util.HashMap[BigInteger, InetAddressAndPort]()
  private var InetAddressAndPortToTokenMap = new util.HashMap[InetAddressAndPort, BigInteger]

  def update(token: BigInteger, endpoint: InetAddressAndPort) = {
    lock.writeLock.lock()
    try {
      val oldToken = InetAddressAndPortToTokenMap.get(endpoint)
      if (oldToken != null) tokenToInetAddressAndPortMap.remove(oldToken)
      println(s"Updating token ${token} for endpoint ${endpoint}")
      tokenToInetAddressAndPortMap.put(token, endpoint)
      InetAddressAndPortToTokenMap.put(endpoint, token)

      println(s"Token map is now ${InetAddressAndPortToTokenMap}")

    } finally lock.writeLock.unlock()
  }

  def cloneTokenEndPointMap = {
    tokenToInetAddressAndPortMap.clone().asInstanceOf[util.HashMap[BigInteger, InetAddressAndPort]]
  }
}
