package org.apache.openwhisk.core.loadBalancer.allocator

import akka.actor.ActorSystem

import scala.concurrent.Future

class BasicHealthCheck(system: ActorSystem) extends (() => Future[Boolean]) {
  override def apply(): Future[Boolean] = {
    Future.successful(true)
  }
}
