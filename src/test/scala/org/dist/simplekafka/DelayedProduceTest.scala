package org.dist.simplekafka

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.simplekafka.ProduceResponse.PartitionResponse
import org.dist.util.Networks
import org.scalatest.FunSuite

/**
 * Keys used for delayed operation metrics recording
 */
trait DelayedOperationKey {
  def keyLabel: String
}

object DelayedOperationKey {
  val globalLabel = "All"
}


/* used by delayed-produce and delayed-fetch operations */
case class TopicPartitionOperationKey(topic: String, partition: Int) extends DelayedOperationKey {


  override def keyLabel = "%s-%d".format(topic, partition)
}

object TopicPartitionOperationKey {
  def apply(topicPartition: TopicAndPartition): TopicPartitionOperationKey = {
    apply(topicPartition.topic, topicPartition.partition)
  }
}


import java.util

class DelayedProduceTest extends ZookeeperTestHarness {

  private val topicPartition: TopicAndPartition = TopicAndPartition("topic1", 0)
  val produceStatus: collection.Map[TopicAndPartition, ProducePartitionStatus]
  = Map(topicPartition -> ProducePartitionStatus(10, new PartitionResponse(Errors.NONE)))

  val responses = new util.ArrayList[PartitionResponse]();
  val responseCallback: collection.Map[TopicAndPartition, PartitionResponse] => Unit = (map) => {
    map.values.foreach(p => {
      responses.add(p)
    })
  }

  val timeout: Long = TimeUnit.SECONDS.toMillis(1)


  test("should expire delayed produce requests if replication does not happen"){
    val delayedProduceLock: Option[Lock] = None
    val ProducerPurgatoryPurgeIntervalRequests = 100
    val delayedProducePurgatory = DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", brokerId = 0,
      purgeInterval = ProducerPurgatoryPurgeIntervalRequests)


    val produceMetadata = ProduceMetadata(1, produceStatus)
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    replicaManager.getOrCreatePartition(topicPartition)


    val delayedProduce = new DelayedProduce(timeout, produceMetadata, replicaManager, responseCallback, delayedProduceLock)

    // create a list of (topic, partition) pairs to use as k    val delayedProroduceMetadata(1, produceStatus)
    val producerRequestKeys = List(TopicPartitionOperationKey(topicPartition))

    // try to complete the request immediately, otherwise put it into the purgatory
    // this is because while the delayed produce operation is being created, new
    // requests may arrive and hence make this operation completable.
    delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

    TestUtils.waitUntilTrue(()=> responses.size() == 1, "Waiting for producer response to expire", 2000)

    assert(responses.get(0).error == Errors.REQUEST_TIMED_OUT)
  }

}
