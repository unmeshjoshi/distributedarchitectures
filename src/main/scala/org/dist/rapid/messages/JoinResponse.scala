package org.dist.rapid.messages

import org.dist.kvstore.InetAddressAndPort

case class JoinResponse(observers:List[InetAddressAndPort])
