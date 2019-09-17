package org.dist.kvstore

import java.math.BigInteger
import java.util.concurrent.ScheduledThreadPoolExecutor

class StorageService(listenAddress:InetAddressAndPort, config:DatabaseConfiguration) {
  def start() = {
    new DbManager("/home/unmesh/cassandratest")
    val generationNbr = 1 //need to stored and read for supporting crash failures
    val messagingService = new MessagingService


    val executor = new ScheduledThreadPoolExecutor(1)
    val gossiper = new Gossiper(generationNbr, listenAddress, config, executor, messagingService)

    messagingService.listen(listenAddress) //listen after gossiper is created as there is circular dependency on gossiper from messagingservice
    gossiper.start()
    /* Make sure this token gets gossiped around. */
    gossiper.addApplicationState(ApplicationState.TOKENS, newToken())

  }

  def newToken() = {
    val guid: String = GuidGenerator.guid
    var token: BigInteger = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token.toString
  }
}
