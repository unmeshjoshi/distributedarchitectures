package org.dist.simplegossip

import java.math.BigInteger
import java.util

import org.apache.log4j.Logger
import org.dist.kvstore.locator.RackUnawareStrategy
import org.dist.kvstore.{FBUtilities, GuidGenerator, InetAddressAndPort, RowMutation, TokenMetadata}

case class Table(name:String, kv:util.Map[String, String]) {
  def put(key:String, value:String) = kv.put(key, value)
}

class StorageService(seed:InetAddressAndPort, clientListenAddress:InetAddressAndPort, val localEndPoint:InetAddressAndPort) {
  val ReplicationFactor = 2

  def getNStorageEndPointMap(key: String) = {
    val token: BigInteger = FBUtilities.hash(key)
    new RackUnawareStrategy(tokenMetadata).getStorageEndPoints(token, tokenMetadata.cloneTokenEndPointMap)
  }

  private val logger = Logger.getLogger(classOf[StorageService])

  val tables = new util.HashMap[String, Table]()
  def apply(rowMutation: RowMutation) = {
    var table = tables.get(rowMutation.table)
    if (table == null) {
      table = new Table(rowMutation.table, new util.HashMap[String, String]())
      tables.put(rowMutation.table, table)
    }
    table.put(rowMutation.key, rowMutation.value)
    true
  }

  val token = newToken()
  val tokenMetadata = new TokenMetadata()
  val gossiper = new Gossiper(seed, localEndPoint, token, tokenMetadata)
  val messagingService = new MessagingService(gossiper, this)
  val storageProxy = new StorageProxy(clientListenAddress, this, messagingService)

  def start() = {
    messagingService.listen(localEndPoint)
    gossiper.start()
    tokenMetadata.update(token, localEndPoint)
    storageProxy.start()
  }

  def newToken() = {
    val guid = GuidGenerator.guid
    var token = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token
  }
}
