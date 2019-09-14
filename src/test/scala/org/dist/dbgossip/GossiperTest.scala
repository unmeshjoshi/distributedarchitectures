package org.dist.dbgossip

import java.net.InetAddress
import java.util
import java.util.UUID

import com.fasterxml.jackson.annotation.{JsonAutoDetect, PropertyAccessor}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.dist.dbgossip.Verb.GOSSIP_DIGEST_SYN
import org.scalatest.FunSuite

class GossiperTest extends FunSuite {

  test("should send gossip syn message") {

    val endpointState = new EndPointState(new HeartBeatState(1))
    val localHostId = UUID.randomUUID

    val appStates = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState])
    appStates.put(ApplicationState.HOST_ID, new VersionedValue(localHostId.toString, 1))

    endpointState.addApplicationStates(appStates)

    val gDigests = new util.ArrayList[GossipDigest]()
    gDigests.add(new GossipDigest(FBUtilities.getBroadcastAddressAndPort, 1, 1))

    val digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName, gDigests)
    val message: Message[GossipDigestSyn] = Message.out(GOSSIP_DIGEST_SYN, digestSynMessage)
    val objectMapper = new ObjectMapper()
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)


    val str = objectMapper.writeValueAsString(message)
    val deserializedMessage = objectMapper.readValue(str.getBytes(), classOf[Message[GossipDigestSyn]])

    assert(deserializedMessage != null)
  }

  test("should send gossip synack message") {

    val endpointState = new EndPointState(new HeartBeatState(1))
    val localHostId = UUID.randomUUID

    val appStates = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState])
    appStates.put(ApplicationState.HOST_ID, new VersionedValue(localHostId.toString, 1))
    endpointState.addApplicationStates(appStates)

    val gDigests = new util.ArrayList[GossipDigest]()
    gDigests.add(new GossipDigest(FBUtilities.getBroadcastAddressAndPort, 1, 1))


    val value = new util.HashMap[InetAddressAndPort, EndPointState]()
    value.put(new InetAddressAndPort(InetAddress.getByName("10.12.10.11"), 8081), endpointState)
    val digestSynMessage = new GossipDigestAck(gDigests, value)
    val message: Message[GossipDigestAck] = Message.out(Verb.GOSSIP_DIGEST_ACK, digestSynMessage)
    val objectMapper = new ObjectMapper()
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)

    val str = objectMapper.writeValueAsString(message)
    val deserializedMessage = objectMapper.readValue(str.getBytes(), classOf[Message[GossipDigestAck]])

    assert(deserializedMessage != null)
  }

  test("should send gossip synack2 message") {
    val endpointState = new EndPointState(new HeartBeatState(1))
    val localHostId = UUID.randomUUID

    val appStates = new util.EnumMap[ApplicationState, VersionedValue](classOf[ApplicationState])
    appStates.put(ApplicationState.HOST_ID, new VersionedValue(localHostId.toString, 1))

    endpointState.addApplicationStates(appStates)

    val gDigests = new util.ArrayList[GossipDigest]()
    gDigests.add(new GossipDigest(FBUtilities.getBroadcastAddressAndPort, 1, 1))


    val value = new util.HashMap[InetAddressAndPort, EndPointState]()
    value.put(new InetAddressAndPort(InetAddress.getByName("10.12.10.11"), 8081), endpointState)
    val digestSynMessage = new GossipDigestAck2(value)
    val message: Message[GossipDigestAck2] = Message.out(Verb.GOSSIP_DIGEST_ACK2, digestSynMessage)
    val objectMapper = new ObjectMapper()
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)

    val str = objectMapper.writeValueAsString(message)
    val deserializedMessage = objectMapper.readValue(str.getBytes(), classOf[Message[GossipDigestAck2]])

    assert(deserializedMessage != null)
  }
}
