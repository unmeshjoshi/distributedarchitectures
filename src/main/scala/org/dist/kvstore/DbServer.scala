package org.dist.kvstore

import java.util.concurrent.ScheduledThreadPoolExecutor

class DbServer(listenAddress:InetAddressAndPort, config:DatabaseConfiguration) {
  def start(): Unit = {
    val generationNbr = 1 //need to stored and read for supporting crash failures
    val messagingService = new MessagingService


    val executor = new ScheduledThreadPoolExecutor(1)
    val gossiper = new Gossiper(generationNbr, listenAddress, config, executor, messagingService)

    messagingService.listen(listenAddress) //listen after gossiper is created as there is circular dependency on gossiper from messagingservice
    gossiper.start()
  }
}
