package org.dist.patterns.distlog

import java.util

import org.dist.kvstore.InetAddressAndPort
import org.dist.patterns.common.JsonSerDes
import org.dist.queue.ZookeeperTestHarness
import org.scalatest.FunSuite

class LedgerMetadataTest extends ZookeeperTestHarness {
  test("should create ledger in zookeeper") {
    val metadata = new LedgerMetadata(3, 2)
    val address1 = InetAddressAndPort.create("10.10.10.10", 8000)
    val address2 = InetAddressAndPort.create("10.10.10.11", 8000)
    val address3 = InetAddressAndPort.create("10.10.10.12", 8000)
    metadata.addEnsemble(0, new util.ArrayList(util.Arrays.asList(address1, address2, address3)))

    val ledgerManager = new LedgerManager(zkClient);
    val path = ledgerManager.newLedgerPath(metadata)
    val ledgerId = ledgerManager.getLedgerId(path)
    assert(ledgerId == 0)
  }
}
