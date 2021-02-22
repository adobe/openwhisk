package org.apache.openwhisk.core.loadBalancer

import akka.actor.{typed, ActorSystem, Props}
import akka.http.scaladsl.model.HttpHeader
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig.wskApiHost
import org.apache.openwhisk.core.connector.{ActivationMessage, MessagingProvider}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.spi.SpiLoader
import spray.json._
import java.time.Instant
import akka.actor.typed.scaladsl.Behaviors.supervise
import akka.actor.typed.{ActorRef, SupervisorStrategy}
import akka.{actor => classic}
import org.apache.openwhisk.core.loadBalancer.allocator.{
  Allocation,
  AllocationCommand,
  Forwarder,
  KubernetesBackends,
  Settings,
  TenantAllocator,
  TenantRouter
}
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.typed.{ClusterSingleton, SingletonActor}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{Failure, Success}

class TenantIsolatedLoadBalancer(config: WhiskConfig,
                                 feedFactory: FeedFactory,
                                 controllerInstance: ControllerInstanceId,
                                 implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit override val actorSystem: ActorSystem,
  logging: Logging)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  override protected val invokerPool: classic.ActorRef = actorSystem.actorOf(Props.empty)

  private val typedSystem = actorSystem.toTyped
  private val singletonManager = ClusterSingleton(typedSystem)
  protected val kubernetesBackends = new KubernetesBackends(typedSystem)
  protected val tenantAllocator: ActorRef[AllocationCommand] = singletonManager.init(
    SingletonActor(
      supervise(TenantAllocator(kubernetesBackends))
        .onFailure[Exception](SupervisorStrategy.resume),
      "Allocator"))

  protected val tenantRouter: ActorRef[Allocation] = actorSystem.spawn(
    supervise(TenantRouter(tenantAllocator, kubernetesBackends))
      .onFailure[Exception](SupervisorStrategy.resume),
    "Router")

  /**
   * No action
   */
  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = {}

  /**
   * Returns a message indicating the health of the containers and/or container pool in general.
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
    val whiskActivation: Future[Either[ActivationId, WhiskActivation]] = invoke(action, msg)
    Future(whiskActivation)
  }

  /**
   * Prepares activation message for needed format and calls HTTP Forwarder to invoke remote action.
   *
   * @param action: ExecutableWhiskActionMetaData
   * @param msg: Activation Message
   * @return WhiskActivation
   */
  private def invoke(action: ExecutableWhiskActionMetaData,
                     msg: ActivationMessage): Future[Either[ActivationId, WhiskActivation]] = {
    val namespace: String = action.namespace.root.asString
    val pkg: String = if (action.namespace.defaultPackage) "default" else action.namespace.last.asString
    val act: String = action.name.asString

    val forwarder: Forwarder = new Forwarder(Settings(typedSystem), tenantRouter)
    implicit val system: typed.ActorSystem[Nothing] = actorSystem.toTyped
    val start = Instant.now()
    val httpResponse: Future[Object] = forwarder.forward(namespace, pkg, act, msg.content.get, Seq.empty[HttpHeader])
    val resultFuture: Future[Either[ActivationId, WhiskActivation]] = httpResponse.transform {
      case Success(result: JsObject) =>
        val response: ActivationResponse = ActivationResponse.success(Some(result))
        val wsk: scala.Either[ActivationId, WhiskActivation] = getWhiskActivation(action, msg, start, response)
        Success(wsk)
      case Failure(exception: Exception) =>
        val response: ActivationResponse = ActivationResponse(
          ActivationResponse.ApplicationError,
          Some(new JsObject(Map("error" -> new JsString(exception.getMessage)))))
        val wsk: Either[ActivationId, WhiskActivation] = getWhiskActivation(action, msg, start, response)
        Success(wsk)
      case _ =>
        val response: ActivationResponse = ActivationResponse(
          ActivationResponse.ApplicationError,
          Some(new JsObject(Map("error" -> new JsString("Unknown error")))))
        val wsk: Either[ActivationId, WhiskActivation] = getWhiskActivation(action, msg, start, response)
        Success(wsk)
    }
    resultFuture
  }

  /**
   * Prepares Whisk Activation based on Activation Response.
   *
   * @param action: ExecutableWhiskActionMetaData
   * @param msg: ActivationMessage
   * @param start: Instant
   * @param response: ActivationResponse
   * @return Either[ActivationId, WhiskActivation]
   */
  private def getWhiskActivation(action: ExecutableWhiskActionMetaData,
                                 msg: ActivationMessage,
                                 start: Instant,
                                 response: ActivationResponse): Either[ActivationId, WhiskActivation] = {
    val wsk: Either[ActivationId, WhiskActivation] = Right[ActivationId, WhiskActivation](
      WhiskActivation(
        action.namespace,
        action.name,
        msg.user.subject,
        msg.activationId,
        start,
        end = Instant.now(),
        response = response))
    wsk
  }
}

object TenantIsolatedLoadBalancer extends LoadBalancerProvider {
  override def requiredProperties: Map[String, String] = ExecManifest.requiredProperties ++ wskApiHost

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): LoadBalancer =
    new TenantIsolatedLoadBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), instance)
}
