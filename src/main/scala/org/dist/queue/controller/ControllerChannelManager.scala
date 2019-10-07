package org.dist.queue.controller

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.queue.network.SocketServer
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker

import scala.collection.mutable.HashMap

class ControllerChannelManager(val controllerContext: ControllerContext, val config: Config, socketServer:SocketServer) extends Logging {
  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => removeExistingBroker(brokerState._1))
    }
  }

  private val brokerStateInfo = new HashMap[Int, Broker]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "
  controllerContext.liveBrokers.foreach(addNewBroker(_)) //this is important as the controller, which is the first broker, will not be added to broker info otherwise


  def startup() = {
  }

  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if (!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
      }
    }
  }

  private def addNewBroker(broker: Broker) {
     brokerStateInfo.put(broker.id, broker)
  }


  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerId)
    }
  }

  private def removeExistingBroker(brokerId: Int) {
    try {
      brokerStateInfo.remove(brokerId)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }

  }

  def sendRequest(brokerId: Int, request: RequestOrResponse, callback: (RequestOrResponse) => Unit = null) {
    brokerLock synchronized {
      val brokerOpt = brokerStateInfo.get(brokerId)
      brokerOpt match {
        case Some(broker) =>
          val inetAddressAndPort = InetAddressAndPort.create(broker.host, broker.port)
          info(s"sending request ${request.requestId} to broker ${broker.id} at ${broker.host}:${broker.port}")
          socketServer.sendReceiveTcp(request, inetAddressAndPort)
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }
}

