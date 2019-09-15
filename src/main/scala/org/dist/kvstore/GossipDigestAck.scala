package org.dist.kvstore

import java.util

case class GossipDigestAck(val gDigestList: util.List[GossipDigest], val epStateMap: util.Map[InetAddressAndPort, EndPointState]) {}