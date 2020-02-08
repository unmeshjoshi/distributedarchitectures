package org.dist.kvstore

import java.util
import java.util.{Collections, Map}

import scala.jdk.CollectionConverters._

case class EndPointState(var heartBeatState: HeartBeatState,
                         applicationStates:Map[ApplicationState, VersionedValue] = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState]),
                         updateTimestamp:Long = System.nanoTime(),
                         isAlive:Boolean = true,
                         updateTimeStamp:Long = System.currentTimeMillis()) {

  def addApplicationState(key: ApplicationState, value: VersionedValue): EndPointState = {
    addApplicationStates(Collections.singletonMap(key, value))
  }

  def addApplicationStates(values: util.Map[ApplicationState, VersionedValue]): EndPointState = {
    val applicationState: util.Map[ApplicationState, VersionedValue] = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState])
    val states = values.keySet().asScala.toList
    for(state <- states) {
      applicationState.put(state, values.get(state))
    }
    EndPointState(heartBeatState, applicationState)
  }

  def getMaxEndPointStateVersion() = {
    val versions = new util.ArrayList[Integer]
    versions.add(heartBeatState.version)
    val appStateMap = applicationStates
    val keys = appStateMap.keySet
    for (key <- keys.asScala) {
      val stateVersion = appStateMap.get(key).version
      versions.add(stateVersion)
    }
    /* sort to get the max version to build GossipDigest for this endpoint */ Collections.sort(versions)
    val maxVersion = versions.get(versions.size - 1)
    versions.clear()
    maxVersion
  }

  def markSuspected(): EndPointState = {
    this.copy(isAlive = false)
  }
}
