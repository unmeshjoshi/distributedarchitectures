/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package org.dist.akkagossip

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.{Lock, ReentrantLock}
import scala.annotation.tailrec
import scala.collection.immutable.Map

/**
 * A lock-less thread-safe implementation of [[akka.remote.FailureDetectorRegistry]].
 *
 * @param detectorFactory
 *   By-name parameter that returns the failure detector instance to be used by a newly registered resource
 *
 */
class DefaultFailureDetectorRegistry[A](detectorFactory: () ⇒ FailureDetector) extends FailureDetectorRegistry[A] {

  private val resourceToFailureDetector = new AtomicReference[Map[A, FailureDetector]](Map())
  private final val failureDetectorCreationLock: Lock = new ReentrantLock

  final override def isAvailable(resource: A): Boolean = resourceToFailureDetector.get.get(resource) match {
    case Some(r) ⇒ r.isAvailable
    case _       ⇒ true
  }

  final override def isMonitoring(resource: A): Boolean = resourceToFailureDetector.get.get(resource) match {
    case Some(r) ⇒ r.isMonitoring
    case _       ⇒ false
  }

  final override def heartbeat(resource: A): Unit = {

    resourceToFailureDetector.get.get(resource) match {
      case Some(failureDetector) ⇒ failureDetector.heartbeat()
      case None ⇒
        // First one wins and creates the new FailureDetector
        failureDetectorCreationLock.lock()
        try {
          // First check for non-existing key was outside the lock, and a second thread might just released the lock
          // when this one acquired it, so the second check is needed.
          val oldTable = resourceToFailureDetector.get
          oldTable.get(resource) match {
            case Some(failureDetector) ⇒
              failureDetector.heartbeat()
            case None ⇒
              val newDetector: FailureDetector = detectorFactory()
              newDetector.heartbeat()
              println(s"Creating failure detector for ${resource}" )
              resourceToFailureDetector.set(oldTable + (resource → newDetector))
          }
        } finally failureDetectorCreationLock.unlock()
    }
  }

  @tailrec final override def remove(resource: A): Unit = {

    val oldTable = resourceToFailureDetector.get

    if (oldTable.contains(resource)) {
      val newTable = oldTable - resource

      // if we won the race then update else try again
      if (!resourceToFailureDetector.compareAndSet(oldTable, newTable)) remove(resource) // recur
    }
  }

  @tailrec final override def reset(): Unit = {

    val oldTable = resourceToFailureDetector.get
    // if we won the race then update else try again
    if (!resourceToFailureDetector.compareAndSet(oldTable, Map.empty[A, FailureDetector])) reset() // recur

  }

  /**
   * INTERNAL API
   * Get the underlying FailureDetector for a resource.
   */
  private def failureDetector(resource: A): Option[FailureDetector] =
    resourceToFailureDetector.get.get(resource)

}

