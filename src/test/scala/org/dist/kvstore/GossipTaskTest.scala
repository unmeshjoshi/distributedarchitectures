package org.dist.kvstore

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import java.util
import org.mockito.Mockito
import org.scalatest.FunSuite

class TestMessagingService(storageService:StorageService) extends MessagingService(storageService) {
  def this() = this(null)

  var message:Message = _
  var toAddress:util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]()

  override def sendUdpOneWay(message: Message, to: InetAddressAndPort): Unit = {
    this.message = message
    this.toAddress.add(to)
  }

  override def sendTcpOneWay(message: Message, to: InetAddressAndPort): Unit = {
    this.message = message
    this.toAddress.add(to)
  }
}

class TestScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor(1) {
  var scheduledObject: Runnable = null
  var delay:Long = 0
  var period:Long = 0
  var unit = TimeUnit.MILLISECONDS

  override def scheduleAtFixedRate(runnable: Runnable, delay: Long, period: Long, unit: TimeUnit): ScheduledFuture[_] = {
    this.scheduledObject = runnable
    this.delay = delay
    this.period = period
    this.unit = unit
    Mockito.mock(classOf[ScheduledFuture[_]])
  }
}



class GossipTaskTest extends FunSuite {
  test("should schedule gossip task to run every 1 second after a delay of 1 second") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))

    //TODO: Figure out why this does not work.
//    val executor = Mockito.mock(classOf[ScheduledThreadPoolExecutor])
//    Mockito.when(executor.scheduleWithFixedDelay(any(classOf[Runnable]),anyLong, anyLong, any(classOf[TimeUnit]))).thenReturn(Mockito.mock(classOf[ScheduledFuture[_]]))

    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService
    val gossiper = new Gossiper(1, InetAddressAndPort.create("127.0.0.1", 8000),
      DatabaseConfiguration(seeds), executor, messagingService)

    gossiper.start()

    assert(executor.scheduledObject != null)
    assert(executor.delay == 1000)
    assert(executor.period == 1000)
    assert(executor.unit == TimeUnit.MILLISECONDS)

    assert(messagingService.gossiper == gossiper)
  }

  test("should increment heartbeat counter when run") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))

    //TODO: Figure out why this does not work.
    //    val executor = Mockito.mock(classOf[ScheduledThreadPoolExecutor])
    //    Mockito.when(executor.scheduleWithFixedDelay(any(classOf[Runnable]),anyLong, anyLong, any(classOf[TimeUnit]))).thenReturn(Mockito.mock(classOf[ScheduledFuture[_]]))

    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService

    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)
    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)

    assert(0 == gossiper.versionGenerator.currentVersion)

    val gossipTask = new gossiper.GossipTask()
    gossipTask.run()

    val endPointState = gossiper.endpointStatemap.get(localEndpoint)
    assert(gossiper.versionGenerator.currentVersion > 0)
    assert(gossiper.versionGenerator.currentVersion == endPointState.heartBeatState.version)
  }



  test("should send gossipSyn message to live members") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)

    val liveMembers = util.Arrays.asList(InetAddressAndPort.create("127.0.0.1", 8002))

    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService, liveMembers)

    val gossipTask = new gossiper.GossipTask()
    gossipTask.run()

    assert(messagingService.toAddress.contains(InetAddressAndPort.create("127.0.0.1", 8002)))
  }

  test("should not send gossip message if only seed is self") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)

    val liveMembers = util.Collections.emptyList[InetAddressAndPort]()

    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService, liveMembers)

    val gossipTask = new gossiper.GossipTask()
    gossipTask.run()

    assert(messagingService.toAddress.isEmpty)
  }

  test("should send gossip message to unreachable members") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)

    val liveMembers = util.Collections.emptyList[InetAddressAndPort]()
    val unreachableMembers = util.Arrays.asList(InetAddressAndPort.create("127.0.0.1", 9999))

    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService, liveMembers, unreachableMembers)

    val gossipTask = new gossiper.GossipTask()
    gossipTask.run()

    assert(messagingService.toAddress.contains(InetAddressAndPort.create("127.0.0.1", 9999)))
  }

  test("should send gossip message to seed if live members list does not have seed members and seed is not self") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8001)

    val liveMembers = util.Collections.emptyList[InetAddressAndPort]()

    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)

    val gossipTask = new gossiper.GossipTask()
    gossipTask.run()

    assert(messagingService.toAddress.contains(InetAddressAndPort.create("127.0.0.1", 8000)))
  }


  //sendgossip to random live members - Done
  //send gossip to unreachable - Done
  //send gossip to seed - Done
  //handle gossipsyn message
  //handle gossipsynack message
  //handle gossipsynack2 message
  //doStatusCheck
  //handle failure detector.
}
