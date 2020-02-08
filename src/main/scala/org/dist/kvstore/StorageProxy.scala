package org.dist.kvstore

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util
import java.util.Map
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import org.dist.util.SocketIO
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._


class StorageProxy(clientRequestIp: InetAddressAndPort, storageService: StorageService, messagingService:MessagingService) {

  def start(): Unit = {
      new TcpClientRequestListner(clientRequestIp, storageService, messagingService).start()
  }
}


class TcpClientRequestListner(localEp: InetAddressAndPort, storageService:StorageService, messagingService:MessagingService) extends Thread {
  private[kvstore] val logger = LoggerFactory.getLogger(classOf[TcpListener])

  override def run(): Unit = {
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
    println(s"Listening for client connections on ${localEp}")
    while (true) {
      val socket = serverSocket.accept()
      val socketIO = new SocketIO[Message](socket, classOf[Message])
      val message = socketIO.readHandleRespond { message =>

        logger.debug(s"Got client message ${message}")

        if(message.header.verb == Verb.ROW_MUTATION) {
          val response: Seq[Message] = new RowMutationHandler(storageService).handleMessage(message)
          val value: Seq[RowMutationResponse] = response.map(message => JsonSerDes.deserialize(message.payloadJson.getBytes, classOf[RowMutationResponse]))
          new Message(message.header, JsonSerDes.serialize(QuorumResponse(value.toList)))

        } else if(message.header.verb == Verb.GET_CF) {
          ""

        } else {
          ""
        }
      }
    }
  }

  class RowMutationHandler(storageService: StorageService) {
    def handleMessage(rowMutationMessage: Message) = {
      val rowMutation = JsonSerDes.deserialize(rowMutationMessage.payloadJson.getBytes, classOf[RowMutation])
      val endpointMap = storageService.getNStorageEndPointMap(rowMutation.key)

      val quorumResponseHandler = new QuorumResponseHandler(endpointMap.values().size(), new WriteResponseResolver())
           messagingService.sendRR(rowMutationMessage, endpointMap.values().asScala.toList, quorumResponseHandler)
      quorumResponseHandler.get()
    }
  }

  trait ResponseResolver {
    def resolve(messages:List[Message]):List[Message]
    def isDataPresent(message:List[Message]):Boolean
  }

  class WriteResponseResolver extends ResponseResolver {
    override def resolve(messages: List[Message]): List[Message] = {
      messages
    }

    override def isDataPresent(message: List[Message]): Boolean = true
  }

  class QuorumResponseHandler(responseCount:Int, resolver:ResponseResolver) extends MessageResponseHandler {
    private val lock = new ReentrantLock
    private val condition = lock.newCondition()
    private val responses = new java.util.ArrayList[Message]()
    private val done = new AtomicBoolean(false)
    override def response(message: Message): Unit = {
      lock.lock()
      try {
        val majority = (responseCount >> 1) + 1
        if (!done.get) {
          responses.add(message)
          logger.info(s"QuorumResponseHandler got message ${message}")
          if (responses.size >= majority && resolver.isDataPresent(responses.asScala.toList)) {
            done.set(true)
            condition.signal()
          }
        }
      } finally {
         lock.unlock()
      }
    }

    def get():List[Message] = {
      val startTime = System.currentTimeMillis
      lock.lock()
      try {
        var bVal = true
        try {
          if (!done.get) bVal = condition.await(5000, TimeUnit.MILLISECONDS)
        }
        catch {
          case ex: InterruptedException =>
            logger.debug(ex.getMessage)
        }

      } finally {
        lock.unlock()
        responses.forEach( m => messagingService.callbackMap.remove(m.header.id))
      }
      resolver.resolve(responses.asScala.toList)
    }
  }
}
