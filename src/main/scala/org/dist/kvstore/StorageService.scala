package org.dist.kvstore

import java.math.BigInteger
import java.util.concurrent.ScheduledThreadPoolExecutor

class StorageService(listenAddress: InetAddressAndPort, config: DatabaseConfiguration) extends IEndPointStateChangeSubscriber {

  private val tokenMetadata = new TokenMetadata()

  def start() = {
    val storageMetadata = new DbManager(config.getSystemDir()).start(listenAddress)
    val generationNbr = storageMetadata.generation //need to stored and read for supporting crash failures
    val messagingService = new MessagingService


    val executor = new ScheduledThreadPoolExecutor(1)
    val gossiper = new Gossiper(generationNbr, listenAddress, config, executor, messagingService)

    messagingService.listen(listenAddress) //listen after gossiper is created as there is circular dependency on gossiper from messagingservice
    gossiper.register(this)
    gossiper.start()
    /* Make sure this token gets gossiped around. */
    val tokenForSelf = newToken()
    gossiper.addApplicationState(ApplicationState.TOKENS, tokenForSelf.toString)
    tokenMetadata.update(tokenForSelf, listenAddress)
  }

  def newToken() = {
    val guid = GuidGenerator.guid
    var token = FBUtilities.hash(guid)
    if (token.signum == -1) token = token.multiply(BigInteger.valueOf(-1L))
    token
  }

  override def onChange(endpoint: InetAddressAndPort, epState: EndPointState): Unit = {
    val tokens = epState.applicationStates.get(ApplicationState.TOKENS)
    if (tokens != null) {
      val newToken = new BigInteger(tokens.value)
      val oldToken = tokenMetadata.getToken(endpoint)
      if (oldToken != null) {
        /*
         * If oldToken equals the newToken then the node had crashed
         * and is coming back up again. If oldToken is not equal to
         * the newToken this means that the node is being relocated
         * to another position in the ring.
        */
        if (!(oldToken == newToken)) {
          tokenMetadata.update(newToken, endpoint)
        }
        else {
          /*
          * This means the node crashed and is coming back up.
          * Deliver the hints that we have for this endpoint.
          */
          //          logger_.debug("Sending hinted data to " + ep)
          //          doBootstrap(endpoint, BootstrapMode.HINT)
        }
      }
      else {
        /*
        * This is a new node and we just update the token map.
        */
        tokenMetadata.update(newToken, endpoint)
      }
    }
  }
}
