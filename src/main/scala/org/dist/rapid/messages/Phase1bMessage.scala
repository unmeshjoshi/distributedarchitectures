package org.dist.rapid.messages

import org.dist.kvstore.InetAddressAndPort
import java.util

case class Phase1bMessage(configurationId:Int,
                          rnd:Rank,
                          sender:InetAddressAndPort,
                          vrnd:Rank,
                          vval:util.List[InetAddressAndPort])
