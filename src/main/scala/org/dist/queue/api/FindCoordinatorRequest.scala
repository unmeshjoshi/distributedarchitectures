package org.dist.queue.api

object CoordinatorType {
   val GROUP = 0
   val TRANSACTION = 1
}

//KeyType is CoordinatorType. Keeping it that way to be closer to how its in Kafka codebase
case class FindCoordinatorRequest(groupId:String, keyType:Int)
