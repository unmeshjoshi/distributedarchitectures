package org.dist.kvstore

import org.dist.dbgossip.{Stage, Verb}

case class Header(val from: InetAddressAndPort, messageType: Stage, verb: Verb)