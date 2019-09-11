package org.dist.dbgossip

import java.util
import java.util.{EnumMap, Map, UUID}

object DbDaemon extends App {

}

class StorageService {



  def initServer() = {
    prepareToJoin()
  }

  def prepareToJoin() {

    MessagingService.instance.listen()

    var valueFactory = new VersionedValue.VersionedValueFactory(new RandomPartitioner)


  }

  class EndpointStateBuilder(valueFactory:VersionedValue.VersionedValueFactory, generationNbr:1) {
    val appStates = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState])
    val endpointState = new EndpointState(new HeartBeatState(generationNbr))
    def init(generationNbr: 1): Unit = {
      val localHostId = UUID.randomUUID
      appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(localHostId))
      appStates.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueFactory.nativeaddressAndPort(FBUtilities.getBroadcastAddressAndPort))

      return this
    }

    def withAppState(state:ApplicationState, value:VersionedValue): Unit = {
      appStates.put(state, value)
    }

    def build() = {
      endpointState.addApplicationStates(appStates)
      endpointState
    }
  }
}
