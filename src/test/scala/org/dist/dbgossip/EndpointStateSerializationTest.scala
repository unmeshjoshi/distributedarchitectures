package org.dist.dbgossip

import java.util
import java.util.UUID

import org.scalatest.FunSuite

class EndpointStateSerializationTest extends FunSuite {

  test("should serialize and deserialize enpoint state") {
    var valueFactory = new VersionedValue.VersionedValueFactory(new RandomPartitioner)

    val endpointState = new EndpointState(new HeartBeatState(1))
    val localHostId = UUID.randomUUID

    val appStates = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState])
    appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(localHostId))

    endpointState.addApplicationStates(appStates)

    val json = endpointState.serialize()
    println(json)

    val deserializedEndpointState = EndpointState.deserialize(json)
    assert(endpointState.equals(deserializedEndpointState))
  }

}
