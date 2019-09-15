package org.dist.kvstore

import org.scalatest.FunSuite

class JsonSerDesTest extends FunSuite {

  test("should be able to serialize and deserialize Gossip messages") {
    val messageJson = "{\"header\":{\"from\":{\"address\":\"192.168.0.120\",\"port\":8000,\"defaultPort\":7000},\"messageType\":\"GOSSIP\",\"verb\":\"GOSSIP_DIGEST_ACK\"},\"payloadJson\":\"{\\\"gDigestList\\\":[{\\\"endPoint\\\":{\\\"address\\\":\\\"192.168.0.120\\\",\\\"port\\\":8001,\\\"defaultPort\\\":7000},\\\"generation\\\":1,\\\"maxVersion\\\":0}],\\\"epStateMap\\\":{}}\"}"
    val message = JsonSerDes.deserialize(messageJson.getBytes, classOf[Message])
    assert(message != null)
    val ack = JsonSerDes.deserialize(message.payloadJson.getBytes, classOf[GossipDigestAck])
    assert(ack != null)
  }

}
