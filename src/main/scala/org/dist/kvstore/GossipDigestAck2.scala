package org.dist.kvstore

import java.util

case class GossipDigestAck2(val epStateMap: util.Map[InetAddressAndPort, EndPointState]) {}
