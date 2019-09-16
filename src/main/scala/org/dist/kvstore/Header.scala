package org.dist.kvstore

case class Header(val from: InetAddressAndPort, messageType: Stage, verb: Verb)