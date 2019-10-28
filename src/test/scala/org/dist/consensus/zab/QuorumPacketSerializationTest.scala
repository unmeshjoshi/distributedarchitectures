package org.dist.consensus.zab

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest.FunSuite

class QuorumPacketSerializationTest extends FunSuite {

  test("should serialize and deserialize quorum packets") {
    val packet = QuorumPacket(Leader.NEWLEADER, 0, Array[Byte]())
    val baos = new ByteArrayOutputStream()
    val os = new BinaryOutputArchive(baos)
    os.writeRecord(packet)

    val is = new BinaryInputArchive(new ByteArrayInputStream(baos.toByteArray))
    val deserializedPacket = is.readRecord()

    assertEquals(packet, deserializedPacket)
  }

  test("should serialize and deserialize multiple quorum packets") {
    val baos = new ByteArrayOutputStream()
    val os = new BinaryOutputArchive(baos)
    val zxid = 1 << 32L
    val p1 = QuorumPacket(Leader.NEWLEADER, zxid, Array[Byte]())
    val p2 = QuorumPacket(Leader.PING, zxid + 1, Array[Byte]())
    val p3 = QuorumPacket(Leader.PROPOSAL, zxid + 2, "Hello World".getBytes())
    os.writeRecord(p1)
    os.writeRecord(p2)
    os.writeRecord(p3)

    val is = new BinaryInputArchive(new ByteArrayInputStream(baos.toByteArray))
    assertEquals(p1, is.readRecord())
    assertEquals(p2, is.readRecord())
    assertEquals(p3, is.readRecord())
  }

  private def assertEquals(p1: QuorumPacket, dp1: QuorumPacket) = {
    assert(p1.recordType == dp1.recordType)
    assert(p1.zxid == dp1.zxid)
    assert(p1.data.sameElements(dp1.data))
  }
}
