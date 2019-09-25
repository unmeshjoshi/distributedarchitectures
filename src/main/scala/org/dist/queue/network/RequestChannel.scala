/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dist.queue.network

import java.net._
import java.nio.ByteBuffer
import java.util.concurrent._

import com.yammer.metrics.core.Gauge
import jdk.jshell.execution.Util
import org.apache.log4j.Logger
import org.dist.kvstore.JsonSerDes
import org.dist.queue.{Logging, SystemTime, TopicAndPartition, Utils}
import org.dist.queue.api.{ProducerRequest, RequestKeys, RequestOrResponse}


object RequestChannel extends Logging {
   //FIXME shutdown receive is not json
//  val AllDone = new Request(1, 2, getShutdownReceive(), 0)

  def getShutdownReceive() = {
    val emptyProducerRequest = new ProducerRequest(0, 0, "", 0, 0, collection.mutable.Map[TopicAndPartition, Array[Byte]]())
    val byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes + 2)
    byteBuffer.put(JsonSerDes.serialize(RequestKeys.ProduceKey).getBytes)
    byteBuffer.rewind()
    byteBuffer
  }

  case class Request(processor: Int, requestKey: Any, private var buffer: ByteBuffer, startTimeMs: Long, remoteAddress: SocketAddress = new InetSocketAddress(0)) {
    @volatile var dequeueTimeMs = -1L
    @volatile var apiLocalCompleteTimeMs = -1L
    @volatile var responseCompleteTimeMs = -1L
    val requestObj: RequestOrResponse = {
      val response = JsonSerDes.deserialize(buffer.array(), classOf[RequestOrResponse])
      println("Got Response ************* ")
      println(s"${response}")
      response
    }
    val requestId =requestObj.requestId
    buffer = null
    private val requestLogger = Logger.getLogger("kafka.request.logger")
    println("Processor %d received request : %s".format(processor, requestObj))

    def updateRequestMetrics() {
      val endTimeMs = SystemTime.milliseconds
      // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes since the remote
      // processing time is really small. In this case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
      if (apiLocalCompleteTimeMs < 0)
        apiLocalCompleteTimeMs = responseCompleteTimeMs
      val queueTime = (dequeueTimeMs - startTimeMs).max(0L)
      val apiLocalTime = (apiLocalCompleteTimeMs - dequeueTimeMs).max(0L)
      val apiRemoteTime = (responseCompleteTimeMs - apiLocalCompleteTimeMs).max(0L)
      val responseSendTime = (endTimeMs - responseCompleteTimeMs).max(0L)
      val totalTime = endTimeMs - startTimeMs

      if(requestLogger.isTraceEnabled)
        requestLogger.trace("Completed request:%s from client %s;totalTime:%d,queueTime:%d,localTime:%d,remoteTime:%d,sendTime:%d"
          .format(requestObj, remoteAddress, totalTime, queueTime, apiLocalTime, apiRemoteTime, responseSendTime))
    }
  }
  
  case class Response(processor: Int, request: Request, responseSend: Send, responseAction: ResponseAction) {
    request.responseCompleteTimeMs = SystemTime.milliseconds

    def this(processor: Int, request: Request, responseSend: Send) =
      this(processor, request, responseSend, if (responseSend == null) NoOpAction else SendAction)

    def this(request: Request, send: Send) =
      this(request.processor, request, send)
  }

  trait ResponseAction
  case object SendAction extends ResponseAction
  case object NoOpAction extends ResponseAction
  case object CloseConnectionAction extends ResponseAction
}

class RequestChannel(val numProcessors: Int, val queueSize: Int) {
  private var responseListeners: List[(Int) => Unit] = Nil
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
  for(i <- 0 until numProcessors)
    responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }
  
  /** Send a response back to the socket server to be sent over the network */ 
  def sendResponse(response: RequestChannel.Response) {
    responseQueues(response.processor).put(response)
    for(onResponse <- responseListeners)
      onResponse(response.processor)
  }

  /** No operation to take for the request, need to read more over the network */
  def noOperation(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.NoOpAction))
    for(onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Close the connection for the request */
  def closeConnection(processor: Int, request: RequestChannel.Request) {
    responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction))
    for(onResponse <- responseListeners)
      onResponse(processor)
  }

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.Request =
    requestQueue.take()

  /** Get a response for the given processor if there is one */
  def receiveResponse(processor: Int): RequestChannel.Response =
    responseQueues(processor).poll()

  def addResponseListener(onResponse: Int => Unit) { 
    responseListeners ::= onResponse
  }

  def shutdown() {
    requestQueue.clear
  }
}