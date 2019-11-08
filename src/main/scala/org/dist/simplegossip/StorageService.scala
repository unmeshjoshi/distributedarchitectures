package org.dist.simplegossip

import java.math.BigInteger

import org.dist.kvstore.{FBUtilities, GuidGenerator, InetAddressAndPort}

class StorageService(seed:InetAddressAndPort, localEndPoint:InetAddressAndPort) {
  val token = newToken()
  val gossiper = new Gossiper(seed, localEndPoint, token)

  def start() = {
    val messagingService = new MessagingService(gossiper)
    messagingService.listen(localEndPoint)
    gossiper.start()
  }

  def newToken() = {
    val guid = GuidGenerator.guid
    var token = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token
  }
}
