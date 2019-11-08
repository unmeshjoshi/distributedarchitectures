package org.dist.kvstore

import org.dist.kvstore.client.Client
import org.dist.kvstore.testapp.Utils.createDbDir
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class KVStoreQuorumPutAndGetTest extends FunSuite {

  test("should put key value on multiple replicas") {
    val localIpAddress = new Networks().ipv4Address
    val node1Endpoint = InetAddressAndPort(localIpAddress, 8000)
    val node1ClientEndpoint = InetAddressAndPort(localIpAddress, 9000)


    val node1 = new StorageService(node1ClientEndpoint, node1Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node1")))

    val node2Endpoint = InetAddressAndPort(localIpAddress, 8001)
    val node2ClientEndpoint = InetAddressAndPort(localIpAddress, 9001)
    val node2 = new StorageService(node2ClientEndpoint, node2Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node2")))

    val node3Endpoint = InetAddressAndPort(localIpAddress, 8002)
    val node3ClientEndpoint = InetAddressAndPort(localIpAddress, 9003)
    val node3 = new StorageService(node3ClientEndpoint, node3Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node3")))

    val node4Endpoint = InetAddressAndPort(localIpAddress, 8003)
    val node4ClientEndpoint = InetAddressAndPort(localIpAddress, 9004)
    val node4 = new StorageService(node4ClientEndpoint, node4Endpoint, DatabaseConfiguration(Set(node1Endpoint), createDbDir("node3")))

    node1.start()
    node2.start()
    node3.start()
    node4.start()

    TestUtils.waitUntilTrue(()=>{
      node1.tokenMetadata.cloneTokenEndPointMap.size() == 4 &&
      node2.tokenMetadata.cloneTokenEndPointMap.size() == 4 &&
      node3.tokenMetadata.cloneTokenEndPointMap.size() == 4 &&
      node4.tokenMetadata.cloneTokenEndPointMap.size() == 4
    }, "Waiting till metadata is propogated to all the servers", 10000)

    val client = new Client(node1ClientEndpoint)
    val mutationResponses: Seq[RowMutationResponse] = client.put("table1", "key1", "value1")
    assert(mutationResponses.size == 2)
    assert(mutationResponses.map(m => m.success).toSet == Set(true))
  }
}
