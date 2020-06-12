package org.dist.rapid.messages

import org.dist.kvstore.InetAddressAndPort

case class Phase1aMessage(configurationId:Int, address:InetAddressAndPort, rank:Rank)
