package org.apache.openwhisk.core.loadBalancer.allocator

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.util.Timeout
import Allocation.HttpRequestReceived
import TenantSlot.{SlotReplyError, TenantSlotList}
import spray.json.{JsObject, JsString}

import java.util.concurrent.TimeUnit
import org.apache.openwhisk.common.Logging

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class Forwarder(private val settings: Settings, private val followerActor: ActorRef[Allocation]) {

  /**
   * Get incoming data and forwards based on namespace and package it available tenant's slot.
   * If several slots available then uses Round-Robin slot balancer to choose the next slot.
   *
   * @param namespace: String
   * @param pckg: String
   * @param action: String
   * @param input: JsObject
   * @param headers: Seq[HttpHeader]
   * @param ec: implicit ExecutionContext
   * @param actorSystem: implicit ActorSystem[Nothing]
   * @return Future[JsObject]
   */
  def forward(namespace: String, pckg: String, action: String, input: JsObject, headers: Seq[HttpHeader])(
    implicit ec: ExecutionContext,
    actorSystem: ActorSystem[Nothing],
    logging: Logging): Future[Object] = {

    implicit val timeout: Timeout = 5.seconds
    val tenantName: String = namespace

    val slots: Future[TenantSlot] =
      (followerActor ? (replyTo => HttpRequestReceived(tenantName, replyTo))).mapTo[TenantSlot]
    val result: Future[Object] = slots transform {
      case Success(TenantSlotList(tenantSlots)) => {
        logging.info(this, "Executing action in allocated slot")
        if (!settings.isMockBackends) {
          val startTime = System.nanoTime()
          val slot = SlotBalancer.select(tenantName, tenantSlots)
          val address = slot.host + ":" + slot.port.get
          val slotUri = s"http://$address/api/v1/$namespace/$pckg/$action"
          val httpRequest = HttpEntity(ContentTypes.`application/json`, input.toString())
          val httpResponse =
            Http().singleRequest(HttpRequest(HttpMethods.POST, slotUri, headers, entity = httpRequest))
          val responseTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)
          PrometheusCollectors.httpForwardResponseTime
            .withTag("service", "application")
            .withTag("tenant", tenantName)
            .update(responseTime)
          val response = httpResponse.mapTo[JsObject]
          Success(response)
        } else {
          Success(JsObject("body" -> JsObject("message" -> JsString("Mocked response."))))
        }
      }
      case Success(SlotReplyError(error)) => Failure(new Exception(error))
      case Failure(exception) => Failure(exception)
    }
    result
  }
}
