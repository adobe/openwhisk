/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool.kubernetes

import akka.actor.ActorSystem

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import akka.stream.StreamLimitReachedException
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.fabric8.kubernetes.client.PortForward

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.docker.{CompleteAfterOccurrences, OccurrencesNotFoundException}
import org.apache.openwhisk.core.entity.{ByteSize, WhiskAction, WhiskActivation}
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.invoker.InvokerReactive
import org.apache.openwhisk.http.Messages
import spray.json.JsObject

import java.net.SocketTimeoutException
import scala.util.Failure

object KubernetesContainer {

  /**
   * Creates a container running in kubernetes
   *
   * @param transid transaction creating the container
   * @param image image to create the container from
   * @param userProvidedImage whether the image is provided by the user
   *     or is an OpenWhisk provided image
   * @param labels labels to set on the container
   * @param name optional name for the container
   * @return a Future which either completes with a KubernetesContainer or a failure to create a container
   */
  def create(transid: TransactionId,
             name: String,
             image: String,
             userProvidedImage: Boolean = false,
             memory: ByteSize = 256.MB,
             environment: Map[String, String] = Map.empty,
             labels: Map[String, String] = Map.empty)(implicit kubernetes: KubernetesApi,
                                                      ec: ExecutionContext,
                                                      log: Logging): Future[KubernetesContainer] = {
    implicit val tid = transid

    // Kubernetes naming rule allows maximum length of 63 character and ended with character only.
    val origName = name.replace("_", "-").replaceAll("[()]", "").toLowerCase.take(63)
    val podName = if (origName.endsWith("-")) origName.reverse.dropWhile(_ == '-').reverse else origName

    for {
      container <- kubernetes.run(podName, image, memory, environment, labels, userProvidedImage).recoverWith {
        case e: KubernetesPodApiException =>
          //apiserver call failed - this will expose a different error to users
          // Signal invoker to advertise unhealthy in case SocketTimeoutExceptions start occurring
          if (e.getCause.isInstanceOf[SocketTimeoutException]) {
            InvokerReactive.kubernetesSocketTimeout = true
            log.error(this, s"Signal to invoker: SocketTimeoutException has occurred")
          }
          cleanupFailedPod(e, podName, WhiskContainerStartupError(Messages.resourceProvisionError))
        case e: BlackboxStartupError =>
          //for blackbox startup, keep the error type but reset the message from "timed out" to "cound not run with image".
          cleanupFailedPod(e, podName, BlackboxStartupError(s"Failed to run container with image '${image}'."))
        case e: Throwable =>
          cleanupFailedPod(e, podName, WhiskContainerStartupError(s"Failed to run container with image '${image}'."))
      }
    } yield container
  }
  private def cleanupFailedPod(e: Throwable, podName: String, failureCause: Exception)(
    implicit kubernetes: KubernetesApi,
    ec: ExecutionContext,
    tid: TransactionId,
    log: Logging) = {
    log.info(this, s"Deleting failed pod '$podName' after: ${e.getClass} - ${e.getMessage}")
    kubernetes
      .rm(podName)
      .andThen {
        case Failure(e) =>
          log.error(this, s"Failed delete pod for '$podName': ${e.getClass} - ${e.getMessage}")
      }
      .transformWith { _ =>
        Future.failed(failureCause)
      }
  }
}

/**
 * Represents a container as run by kubernetes.
 *
 * This class contains OpenWhisk specific behavior and as such does not necessarily
 * use kubernetes commands to achieve the effects needed.
 *
 * @constructor
 * @param id the id of the container
 * @param addr the ip & port of the container
 * @param workerIP the ip of the workernode on which the container is executing
 * @param nativeContainerId the docker/containerd lowlevel id for the container
 */
class KubernetesContainer(protected[core] val id: ContainerId,
                          protected[core] val addr: ContainerAddress,
                          protected[core] val workerIP: String,
                          protected[core] val nativeContainerId: String,
                          portForward: Option[PortForward] = None)(implicit kubernetes: KubernetesApi,
                                                                   override protected val as: ActorSystem,
                                                                   protected val ec: ExecutionContext,
                                                                   protected val logging: Logging)
    extends Container {

  /** The last read timestamp in the log file */
  private val lastTimestamp = new AtomicReference[Option[Instant]](None)

  protected val waitForLogs: FiniteDuration = 2.seconds

  override val hostNode = Some(workerIP)

  override def suspend()(implicit transid: TransactionId): Future[Unit] = {
    super.suspend().flatMap(_ => kubernetes.suspend(this))
  }

  override def resume()(implicit transid: TransactionId): Future[Unit] =
    kubernetes.resume(this).flatMap(_ => super.resume())

  override def destroy(checkErrors: Boolean = false)(implicit transid: TransactionId,
                                                     activation: Option[WhiskActivation]): Future[Unit] = {
    super.destroy()
    portForward.foreach(_.close())
    val logErrors = if (checkErrors) {
      //get the pod container status to log for easier analysis
      //make sure this does not prevent the rm() call to delete the pod
      kubernetes.logPodStatus(this).recover {
        case t: Throwable =>
          logging.error(this, s"failed to log pod status ${t}")
      }
    } else {
      Future.successful({})
    }
    logErrors.flatMap(_ => kubernetes.rm(this))
  }

  override def initialize(initializer: JsObject,
                          timeout: FiniteDuration,
                          maxConcurrent: Int,
                          entity: Option[WhiskAction] = None)(implicit transid: TransactionId): Future[Interval] = {
    entity match {
      case Some(e) => {
        kubernetes
          .addLabel(this, Map("openwhisk/action" -> e.name.toString, "openwhisk/namespace" -> e.namespace.toString))
          .map(return super.initialize(initializer, timeout, maxConcurrent, entity))
      }
      case None => super.initialize(initializer, timeout, maxConcurrent, entity)
    }
  }

  def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Source[ByteString, Any] = {

    kubernetes
      .logs(this, lastTimestamp.get, waitForSentinel)
      .limitWeighted(limit.toBytes) { obj =>
        // Adding + 1 since we know there's a newline byte being read
        obj.jsonSize.toLong + 1
      }
      .map { line =>
        lastTimestamp.set(Option(line.time))
        line
      }
      .via(new CompleteAfterOccurrences(_.log == Container.ACTIVATION_LOG_SENTINEL, 2, waitForSentinel))
      .recover {
        case _: StreamLimitReachedException =>
          // While the stream has already ended by failing the limitWeighted stage above, we inject a truncation
          // notice downstream, which will be processed as usual. This will be the last element of the stream.
          TypedLogLine(Instant.now, "stderr", Messages.truncateLogs(limit))
        case _: OccurrencesNotFoundException | _: FramingException =>
          // Stream has already ended and we insert a notice that data might be missing from the logs. While a
          // FramingException can also mean exceeding the limits, we cannot decide which case happened so we resort
          // to the general error message. This will be the last element of the stream.
          TypedLogLine(Instant.now, "stderr", Messages.logFailure)
      }
      .takeWithin(waitForLogs)
      .map { _.toByteString }
  }
}
