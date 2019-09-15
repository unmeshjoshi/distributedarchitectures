package org.dist.kvstore

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import java.util
import org.mockito.Mockito
import org.scalatest.FunSuite

class TestMessagingService extends MessagingService {
  var message:Message = _
  var toAddress:util.List[InetAddressAndPort] = new util.ArrayList[InetAddressAndPort]()

  override def sendUdpOneWay(message: Message, to: InetAddressAndPort): Unit = {
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

    assert(0 == VersionGenerator.currentVersion)

    val gossipTask = new gossiper.GossipTask()
    gossipTask.run()

    val endPointState = gossiper.endpointStatemap.get(localEndpoint)
    assert(VersionGenerator.currentVersion > 0)
    assert(VersionGenerator.currentVersion == endPointState.heartBeatState.version)
  }



  test("should send gossipSyn message to live members") {
    val seeds = Set(InetAddressAndPort.create("127.0.0.1", 8000))
    val executor = new TestScheduledThreadPoolExecutor
    val messagingService = new TestMessagingService
    val localEndpoint = InetAddressAndPort.create("127.0.0.1", 8000)

    val gossiper = new Gossiper(1, localEndpoint,
      DatabaseConfiguration(seeds), executor, messagingService)

    assert(0 == VersionGenerator.currentVersion)

    val gossipTask = new gossiper.GossipTask()
    gossipTask.run()

    val endPointState = gossiper.endpointStatemap.get(localEndpoint)
    assert(VersionGenerator.currentVersion > 0)
    assert(VersionGenerator.currentVersion == endPointState.heartBeatState.version)

  }

  //sendgossip to random live members
  //send gossip to unreachable
  //send gossip to seed
  //handle gossipsyn message
  //handle gossipsynack message
  //handle gossipsynack2 message
  //doStatusCheck
  //handle failure detector.
}
