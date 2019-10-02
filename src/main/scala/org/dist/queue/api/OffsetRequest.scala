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

package org.dist.queue.api

import org.dist.queue.common.TopicAndPartition

object OffsetRequest {
  val CurrentVersion = 0.shortValue
  val DefaultClientId = ""

  val SmallestTimeString = "smallest"
  val LargestTimeString = "largest"
  val LatestTime = -1L
  val EarliestTime = -2L
}

case class PartitionOffsetRequestInfo(time: Long, maxNumOffsets: Int)

case class OffsetRequest(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo],
                         versionId: Short = OffsetRequest.CurrentVersion,
                         val correlationId: Int = 0,
                         clientId: String = OffsetRequest.DefaultClientId,
                         replicaId: Int = Request.OrdinaryConsumerId)
{

  def this(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo], correlationId: Int, replicaId: Int) = this(requestInfo, OffsetRequest.CurrentVersion, correlationId, OffsetRequest.DefaultClientId, replicaId)

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  def isFromOrdinaryClient = replicaId == Request.OrdinaryConsumerId
  def isFromDebuggingClient = replicaId == Request.DebuggingConsumerId

}
