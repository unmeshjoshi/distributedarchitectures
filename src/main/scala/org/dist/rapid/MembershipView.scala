package org.dist.rapid

import java.util
import java.util.Collection

import org.dist.kvstore.InetAddressAndPort

case class MembershipView(endpoints: util.List[InetAddressAndPort]) {
  def getObservers() = endpoints.get(0)

  def isSafeToJoin() = true

  private val K: Int = 10

}
