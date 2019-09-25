package org.dist.queue

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.dist.queue.api.{LeaderAndIsrResponse, RequestKeys, RequestOrResponse, UpdateMetadataResponse}
import org.dist.queue.network.{BlockingChannel, Receive}
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.mutable.HashMap

class ControllerChannelManager(val controllerContext: ControllerContext, val config: Config) extends Logging {
  private val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  def startup() = {

  }

  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if (!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)](config.controllerMessageQueueSize)
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    val channel = new BlockingChannel(broker.host, broker.port,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      config.controllerSocketTimeoutMs)
    channel.connect()
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, broker.id, messageQueue, channel)
    requestThread.setDaemon(false)
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(channel, broker, messageQueue, requestThread))
  }

  private def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if (requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerId)
    }
  }

  private def removeExistingBroker(brokerId: Int) {
    try {
      brokerStateInfo(brokerId).channel.disconnect()
      brokerStateInfo(brokerId).requestSendThread.shutdown()
      brokerStateInfo.remove(brokerId)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }

  }

  def sendRequest(brokerId: Int, request: RequestOrResponse, callback: (RequestOrResponse) => Unit = null) {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put((request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }
}


case class ControllerBrokerStateInfo(channel: BlockingChannel,
                                     broker: Broker,
                                     messageQueue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                                     requestSendThread: RequestSendThread)

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val toBrokerId: Int,
                        val queue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                        val channel: BlockingChannel)
  extends ShutdownableThread("Controller-%d-to-broker-%d-send-thread".format(controllerId, toBrokerId)) with Logging {
  private val lock = new Object()

  override def doWork(): Unit = {
    val queueItem: (RequestOrResponse, RequestOrResponse ⇒ Unit) = queue.take()
    val request = queueItem._1
    val callback = queueItem._2

    var receive: Receive = null

    try {
      lock synchronized {
        channel.connect() // establish a socket connection if needed
        channel.send(request)
        receive = channel.receive()
        var response: RequestOrResponse = null
        request.requestId match {
          case RequestKeys.LeaderAndIsrKey =>
            response = LeaderAndIsrResponse.readFrom(receive.buffer)
          case RequestKeys.UpdateMetadataKey =>
            response = UpdateMetadataResponse.readFrom(receive.buffer)
        }
        trace("Controller %d epoch %d received response correlationId %d for a request sent to broker %d"
          .format(controllerId, controllerContext.epoch, response.correlationId, toBrokerId))

        if (callback != null) {
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        warn("Controller %d fails to send a request to broker %d".format(controllerId, toBrokerId), e)
        // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.
        channel.disconnect()
    }
  }
}