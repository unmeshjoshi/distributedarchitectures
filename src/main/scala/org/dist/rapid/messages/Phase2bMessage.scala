package org.dist.rapid.messages

import java.util

import org.dist.kvstore.InetAddressAndPort

case class Phase2bMessage(configurationId:Int, rnd:Rank, sender:InetAddressAndPort, vval: util.List[InetAddressAndPort])
