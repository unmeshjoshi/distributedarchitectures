package org.dist.simplegossip

import java.math.BigInteger
import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util

import org.dist.kvstore.{GossipDigest, GossipDigestSyn, Header, InetAddressAndPort, JsonSerDes, Message,RowMutation, RowMutationResponse, Stage, Verb}
import org.dist.util.SocketIO
import org.slf4j.LoggerFactory

class TcpListener(localEp: InetAddressAndPort, gossiper: Gossiper, storageService: StorageService, messagingService: MessagingService) extends Thread {
  private val logger = LoggerFactory.getLogger(classOf[TcpListener])

  override def run(): Unit = {
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))

    logger.info(s"Listening on ${localEp}")

    while (true) {
      val socket = serverSocket.accept()
      val message = new SocketIO[Message](socket, classOf[Message]).read()
      logger.debug(s"Got message ${message}")

      if (message.header.verb == Verb.GOSSIP_DIGEST_SYN) {
        new GossipDigestSynHandler(gossiper, messagingService).handleMessage(message)

      } else if (message.header.verb == Verb.GOSSIP_DIGEST_ACK) {
        new GossipDigestSynAckHandler(gossiper, messagingService).handleMessage(message)

      } else if (message.header.verb == Verb.GOSSIP_DIGEST_ACK2) {
        new GossipDigestAck2Handler(gossiper, messagingService).handleMessage(message)

      } else if(message.header.verb == Verb.RESPONSE) {

        val handler = messagingService.callbackMap.get(message.header.id)
        if (handler != null) handler.response(message)

      } else if (message.header.verb == Verb.ROW_MUTATION) {
        new RowMutationHandler(storageService, messagingService).handleMessage(message)
      }
    }
  }



  class RowMutationHandler(storageService: StorageService, messagingService: MessagingService) {
    def handleMessage(rowMutationMessage: Message) = {
      val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
      val success = storageService.apply(rowMutation)
      val response = RowMutationResponse(1, rowMutation.key, success)
      val responseMessage = Message(Header(localEp, Stage.RESPONSE_STAGE, Verb.RESPONSE, rowMutationMessage.header.id), JsonSerDes.serialize(response))
      messagingService.sendTcpOneWay(responseMessage, rowMutationMessage.header.from)
    }
  }

  class GossipDigestSynHandler(gossiper: Gossiper, messagingService: MessagingService) {
    def handleMessage(synMessage: Message): Unit = {
      val gossipDigestSyn = JsonSerDes.deserialize(synMessage.payloadJson.getBytes, classOf[GossipDigestSyn])

      val deltaGossipDigest = new util.ArrayList[GossipDigest]()
      val deltaEndPointStates = new util.HashMap[InetAddressAndPort, BigInteger]()
      gossiper.examineGossiper(gossipDigestSyn.gDigests, deltaGossipDigest, deltaEndPointStates)

      val synAckMessage = gossiper.makeGossipDigestAckMessage(deltaGossipDigest, deltaEndPointStates)
      messagingService.sendTcpOneWay(synAckMessage, synMessage.header.from)
    }
  }


  class GossipDigestSynAckHandler(gossiper: Gossiper, messagingService: MessagingService) {
    def handleMessage(synAckMessage: Message): Unit = {
      val gossipDigestSynAck: GossipDigestAck = JsonSerDes.deserialize(synAckMessage.payloadJson.getBytes, classOf[GossipDigestAck])
      val epStateMap: Map[InetAddressAndPort, BigInteger] = gossipDigestSynAck.epStateMap
      if (epStateMap.size > 0) {
        gossiper.applyStateLocally(epStateMap)
      }

      /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
      val deltaEpStateMap = new util.HashMap[InetAddressAndPort, BigInteger]

      for (gDigest <- gossipDigestSynAck.digestList) {
        val addr = gDigest.endPoint
        val localEpStatePtr = gossiper.getStateFor(addr)
        if (localEpStatePtr != null) deltaEpStateMap.put(addr, localEpStatePtr)
      }

      val ack2Message = gossiper.makeGossipDigestAck2Message(deltaEpStateMap)
      messagingService.sendTcpOneWay(ack2Message, synAckMessage.header.from)
    }
  }

  class GossipDigestAck2Handler(gossiper: Gossiper, messagingService: MessagingService) {
    def handleMessage(ack2Message: Message): Unit = {
      val gossipDigestAck2 = JsonSerDes.deserialize(ack2Message.payloadJson.getBytes, classOf[GossipDigestAck2])
      val epStateMap = gossipDigestAck2.epStateMap
      gossiper.applyStateLocally(epStateMap)
    }
  }

}


trait MessageResponseHandler {
  def response(msg: Message): Unit
}


case class GossipDigestAck(val digestList: List[GossipDigest],
                           val epStateMap: Map[InetAddressAndPort, BigInteger])

case class GossipDigestAck2(val epStateMap: Map[InetAddressAndPort, BigInteger])

class MessagingService(val gossiper: Gossiper, storageService: StorageService) {

  gossiper.setMessageService(this)

  val callbackMap = new util.HashMap[String, MessageResponseHandler]()

  def init(): Unit = {
  }

  def listen(localEp: InetAddressAndPort): Unit = {
    assert(gossiper != null)
    new TcpListener(localEp, gossiper, storageService, this).start()
  }

  def sendRR(message: Message, to: List[InetAddressAndPort], messageResponseHandler: MessageResponseHandler): Unit = {
    callbackMap.put(message.header.id, messageResponseHandler)
    to.foreach(address => sendTcpOneWay(message, address))
  }

  def sendTcpOneWay(message: Message, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[Message](clientSocket, classOf[Message]).write(message)
  }

  def sendUdpOneWay(message: Message, to: InetAddressAndPort) = {
    //for control messages like gossip use udp.
  }
}

