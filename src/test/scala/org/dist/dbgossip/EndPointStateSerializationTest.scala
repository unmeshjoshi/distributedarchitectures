package org.dist.dbgossip

import java.util
import java.util.UUID

import org.scalatest.FunSuite

class EndPointStateSerializationTest extends FunSuite {

  test("should serialize and deserialize enpoint state") {

    val endpointState = new EndPointState(new HeartBeatState(1))
    val localHostId = UUID.randomUUID

    val appStates = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState])
    appStates.put(ApplicationState.HOST_ID, new VersionedValue(localHostId.toString, 1))

    endpointState.addApplicationStates(appStates)

    val json = endpointState.serialize()

    val deserializedEndpointState = EndPointState.deserialize(json)
    assert(endpointState.equals(deserializedEndpointState))
  }

}
