package org.dist.kvstore

import java.util

case class GossipDigestSyn(clusterName: String, gDigests: util.List[GossipDigest])