package org.dist.queue

import org.I0Itec.zkclient.ZkClient


class KafkaApis(val replicaManager: ReplicaManager,
                val zkClient: ZkClient,
                brokerId: Int,
                val controller: Controller) extends Logging {


}
