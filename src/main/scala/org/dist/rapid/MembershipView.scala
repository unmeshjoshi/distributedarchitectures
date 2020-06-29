package org.dist.rapid

import java.util
import java.util.Collection

import org.dist.kvstore.InetAddressAndPort

case class MembershipView(endpoints: util.List[InetAddressAndPort]) {
  sort

  def addEndpoint(address: InetAddressAndPort) = {
    if (!endpoints.contains(address)) {
      endpoints.add(address)
      sort
    }
  }

  private def sort = {
    endpoints.sort((a1, a2) => {
      a1.hashCode().compareTo(a2.hashCode())
    })
  }

  def getObservers() = endpoints.get(0)

  def isSafeToJoin() = true

  private val K: Int = 10

}
