package org.dist.rapid.messages

import java.util

import org.dist.kvstore.InetAddressAndPort

case class Phase2aMessage(configurationId:Int,
                          sender:InetAddressAndPort,
                          crnd:Rank,
                          chosenProposal:util.List[InetAddressAndPort])
