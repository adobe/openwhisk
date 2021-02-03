package org.apache.openwhisk.core.loadBalancer

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpHeader, HttpResponse}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig.wskApiHost
import org.apache.openwhisk.core.connector.{ActivationMessage, MessagingProvider}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.spi.SpiLoader
import spray.json.DefaultJsonProtocol._
import spray.json.{JsValue, _}

import java.time.Instant
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{Failure, Success}

class TenantIsolatedLoadBalancer(config: WhiskConfig,
                                 feedFactory: FeedFactory,
                                 controllerInstance: ControllerInstanceId,
                                 implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit override val actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  override protected val invokerPool: ActorRef = actorSystem.actorOf(Props.empty)

  /**
   * No action
   */
  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = {}

  /**
   * Returns a message indicating the health of the containers and/or container pool in general.
   * @TODO currently we don't monitor a health for Multi-tenant routing allocator
   *
   * @return a Future[IndexedSeq[InvokerHealth]] representing the health of the pools managed by the loadbalancer.
   */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(IndexedSeq.empty[InvokerHealth])

  /**
   * Publishes activation message on internal bus for an invoker to pick up.
   *
   * @param action  the action to invoke
   * @param msg     the activation message to publish on an invoker topic
   * @param transid the transaction id for the request
   * @return result a nested Future the outer indicating completion of publishing and
   *         the inner the completion of the action (i.e., the result)
   *         if it is ready before timeout (Right) otherwise the activation id (Left).
   *         The future is guaranteed to complete within the declared action time limit
   *         plus a grace period (see activeAckTimeoutGrace).
   */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    val whiskActivation: WhiskActivation = invoke(action, msg)
    Future.successful(Future(Right(whiskActivation)))
  }

  /**
   * Prepares activation message for needed format and calls HTTP Forwarder to invoke remote action.
   *
   * @param action: ExecutableWhiskActionMetaData
   * @param msg: Activation Message
   * @return WhiskActivation
   */
  private def invoke(action: ExecutableWhiskActionMetaData, msg: ActivationMessage): WhiskActivation = {
    val namespace: String = action.namespace.root.asString
    val pkg: String = if (action.namespace.defaultPackage) "default" else action.namespace.last.asString
    val act: String = action.name.asString
    val headers: Seq[HttpHeader] = extractHeaders(msg.content)
    val body: JsObject = extractBody(msg)

    /**
     * @TODO import Forwarder from multitenant allocator
     */
    val forwarder: Forwarder = new Forwarder(Settings(context.system), tenantRouter)
    val whiskActivation: WhiskActivation = {
      val start = Instant.now()
      var activationResponse: ActivationResponse = ActivationResponse.success(Some(JsObject.empty))
      val httpResponse: Future[HttpResponse] = forwarder.forward(namespace, pkg, act, body, headers)
      httpResponse.onComplete {
        case Success(res) => {
          // @TODO check the status of HttpResponse future and set response object accordingly
          val result: JsObject = res.entity.toJson.asJsObject
          activationResponse = ActivationResponse.success(Some(result))
        }
        case Failure(exception) =>
          activationResponse =
            ActivationResponse(ActivationResponse.ApplicationError, Some(new JsString(exception.getMessage)))
      }

      WhiskActivation(
        action.namespace,
        action.name,
        msg.user.subject,
        msg.activationId,
        start,
        end = Instant.now(),
        response = activationResponse)
    }
    whiskActivation
  }

  /**
   * Extracts body from the activation message regardless of the action type and converts it to Json object.
   *
   * @param msg: ActivationMessage
   * @return JsObject
   */
  private def extractBody(msg: ActivationMessage): JsObject = {
    val body: JsObject =
      if (msg.content.get.fields.contains("__ow_body")) msg.content.get.fields("__ow_body").asJsObject
      else msg.content.get.fields.filterKeys(elem => !elem.contains("__ow_")).toJson.asJsObject
    body
  }

  /**
   * Extracts headers from activation message and converts them to Akka HTTP headers
   *
   * @param content: Option[JsObject]
   * @return Seq[HttpHeader]
   */
  private def extractHeaders(content: Option[JsObject]): Seq[HttpHeader] = {

    /**
     * @TODO there is should be a way how to get headers in more simple way
     */
    val headers: Seq[HttpHeader] = {
      if (content.get.fields.contains("__ow_headers"))
        content.get
          .fields("__ow_headers")
          .asJsObject
          .fields
          .map {
            case (key: String, value: JsValue) => RawHeader(key, value.toString())
          }
          .toIndexedSeq
      else Seq.empty[HttpHeader]
      headers
    }
  }
}

object TenantIsolatedLoadBalancer extends LoadBalancerProvider {
  override def requiredProperties: Map[String, String] = ExecManifest.requiredProperties ++ wskApiHost

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer =
    new TenantIsolatedLoadBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), instance)
}
