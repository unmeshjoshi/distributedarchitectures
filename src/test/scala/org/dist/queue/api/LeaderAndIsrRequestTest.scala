package org.dist.queue.api

import org.dist.kvstore.JsonSerDes
import org.dist.queue.controller.{LeaderAndIsr, LeaderIsrAndControllerEpoch, PartitionStateInfo}
import org.dist.queue.utils.ZkUtils.Broker
import org.scalatest.FunSuite



class LeaderAndIsrRequestTest extends FunSuite {
  private val topic1 = "test1"
  private val topic2 = "test2"
  private val leader1 = 0
  private val isr1 = List(0, 1, 2)
  private val leader2 = 0
  private val isr2 = List(0, 2, 3)

  test("serialize and deserialize leaderandisr request") {
    val leaderAndIsr1 = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader1, 1, isr1, 1), 1)
    val leaderAndIsr2 = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader2, 1, isr2, 2), 1)
    val map: Map[(String, Int), PartitionStateInfo] = Map(("topic1", 0) → PartitionStateInfo(leaderAndIsr1, isr1.toSet),
      ("topic2", 0) → PartitionStateInfo(leaderAndIsr2, isr2.toSet))
    val leaders = Set[Broker](Broker(1, "127.0.0.1", 8000))
    val request = LeaderAndIsrRequest(map, leaders, 1, 0, 1, "client1")
    val str = JsonSerDes.serialize(request)
    val deserializedRequest = JsonSerDes.deserialize(str.getBytes, classOf[LeaderAndIsrRequest])

    assert(deserializedRequest == request)
  }

}
