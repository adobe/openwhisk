package org.apache.openwhisk.core.loadBalancer.allocator

import java.net.InetAddress
import java.nio.file.{Files, Paths}

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.{DeadLetterSuppression, NoSerializationVerificationNeeded}
import akka.discovery.kubernetes.KubernetesApiServiceDiscovery.KubernetesApiException
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.util.HashCode
import com.typesafe.sslconfig.ssl.TrustStoreConfig
import org.apache.openwhisk.common.Https
import org.apache.openwhisk.common.Https.HttpsConfig
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

final case class PodList(items: immutable.Seq[PodList.Pod])

object PodList {

  final case class Metadata(deletionTimestamp: Option[String])

  final case class ContainerPort(name: Option[String], containerPort: Int)

  final case class Container(name: String, ports: Option[immutable.Seq[ContainerPort]])

  final case class PodSpec(containers: immutable.Seq[Container], nodeName: String)

  final case class PodStatus(podIP: Option[String], phase: Option[String])

  final case class Pod(spec: Option[PodSpec], status: Option[PodStatus], metadata: Option[Metadata])

}

/**
 * Future returned by resolve(name, timeout) should be failed with this exception
 * if the underlying mechanism was unable to resolve the name within the given timeout.
 *
 * It is up to each implementation to implement timeouts.
 */
final class DiscoveryTimeoutException(reason: String) extends RuntimeException(reason)

object Resolved {

  def apply(): Resolved = {
    new Resolved(Seq.empty[ResolvedTarget])
  }

  def apply(addresses: immutable.Seq[ResolvedTarget]): Resolved = {
    new Resolved(addresses)
  }

  def unapply(resolved: Resolved): Option[immutable.Seq[ResolvedTarget]] =
    Some(resolved.addresses)
}

/** Result of a successful resolve request */
final class Resolved(val addresses: immutable.Seq[ResolvedTarget])
    extends DeadLetterSuppression
    with NoSerializationVerificationNeeded {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  def getNodes = {}

  def filter(node: Int): Resolved = {
    this
  }

  val sortedAddresses: Map[Int, ResolvedTarget] =
    Map(addresses.sortBy(_.hashCode()) map { a =>
      a.hashCode() -> a
    }: _*)

  val groupedByNode: Map[Int, List[Int]] =
    sortedAddresses.foldLeft(Map.empty[Int, List[Int]])((map: Map[Int, List[Int]], target: (Int, ResolvedTarget)) => {
      map + (target._2.nodeName.hashCode -> (map
        .get(target._2.nodeName.hashCode) match {
        case None               => List(target.hashCode())
        case Some(a: List[Int]) => a :+ target.hashCode()
      }))
    })

  val orderedKeys: Vector[Int] = sortedAddresses.keySet.toVector

  /**
   * Java API
   */
  def getAddresses: Map[Int, ResolvedTarget] = {
    sortedAddresses
  }

  def getByIndex(index: Int): ResolvedTarget = {
//    log.info("Ordered keys {}", orderedKeys)
//    log.info("Sorted addresses {}", sortedAddresses)
    sortedAddresses(orderedKeys(index))
  }

  def getKeyByIndex(index: Int): Int = {
    //    log.info("Ordered keys {}", orderedKeys)
    //    log.info("Sorted addresses {}", sortedAddresses)
    orderedKeys(index)
  }

  override def toString: String = s"Resolved($sortedAddresses)"

  override def equals(obj: Any): Boolean = obj match {
    case other: Resolved => sortedAddresses == other.sortedAddresses
    case _               => false
  }

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, sortedAddresses)
    result
  }

}

object ResolvedTarget {

  /**
   * @param host    the hostname or the IP address of the target
   * @param port    optional port number
   * @param address IP address of the target. This is used during cluster bootstap when available.
   * @param nodeName Name of the node where pod is located
   */
  def apply(host: String, port: Option[Int], address: Option[InetAddress], nodeName: String): ResolvedTarget =
    new ResolvedTarget(host, port, address, nodeName)

}

/**
 * Resolved target host, with optional port and the IP address.
 *
 * @param host    the hostname or the IP address of the target
 * @param port    optional port number
 * @param address optional IP address of the target. This is used during cluster bootstap when available.
 * @param nodeName Name of the node where pod is located
 */
final class ResolvedTarget(val host: String,
                           val port: Option[Int],
                           val address: Option[InetAddress],
                           val nodeName: String)
    extends NoSerializationVerificationNeeded {

  /**
   * Java API
   */
  def getPort: Option[Int] =
    port

  /**
   * Java API
   */
  def getAddress: Option[InetAddress] =
    address

  override def toString: String = s"ResolvedTarget($host,$port,$address)"

  override def equals(obj: Any): Boolean = obj match {
    case other: ResolvedTarget =>
      host == other.host && port == other.port && address == other.address
    case _ => false
  }

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, host)
    result = HashCode.hash(result, port)
    result = HashCode.hash(result, address)
    result
  }

}

class KubernetesBackends(system: ActorSystem[Nothing]) {

  private val http = Http()(system)

  private val settings = Settings(system)

  private val log = system.log

  private def httpsTrustStoreConfig =
    TrustStoreConfig(data = None, filePath = Some(settings.apiCaPath))
      .withStoreType("PEM")

  private val password: String =
    if (httpsTrustStoreConfig.password.isDefined) httpsTrustStoreConfig.password.get else ""
  private val filePath: String =
    if (httpsTrustStoreConfig.filePath.isDefined) httpsTrustStoreConfig.filePath.get else ""

  private def httpsContext =
    Https.connectionContextClient(HttpsConfig(password, httpsTrustStoreConfig.storeType, filePath, "true"))

  private def apiToken =
    readConfigVarFromFilesystem(settings.apiTokenPath, "api-token")
      .getOrElse("")

  private val podNamespace = settings.podNamespace
    .orElse(readConfigVarFromFilesystem(settings.podNamespacePath, "pod-namespace"))
    .getOrElse("default")

  /**
   * This uses blocking IO, and so should only be used to read configuration at startup.
   */
  private def readConfigVarFromFilesystem(path: String, name: String): Option[String] = {
    val file = Paths.get(path)
    if (Files.exists(file)) {
      try {
        Some(new String(Files.readAllBytes(file), "utf-8"))
      } catch {
        case NonFatal(e) =>
          log.error2("Error reading {} from {}", name, path)
          None
      }
    } else {
      log.warn2("Unable to read {} from {} because it doesn't exist.", name, path)
      None
    }
  }

  def getBackends(resolveTimeout: FiniteDuration): Future[Resolved] = {

    if (settings.isMockBackends) {
      val backends = (1 to settings.mockBackendsNumber).foldLeft(List.empty[ResolvedTarget]) { (a, i) =>
        {
          val address = "111.11.12." + i.toString
          a :+ ResolvedTarget(address, Option(2551), Option(InetAddress.getByName(address)), "node-name-1")
        }
      }
      log.debug(s"Returned {} mocked nodes", backends.length)
      return Future.successful(Resolved(backends))
    }

    val labelSelector = settings.podLabelSelector(settings.nodejsPodAppLabel)
    import JsonFormat._

    implicit val executionContext: ExecutionContext = system.executionContext
    implicit val actorSystem = system

    for {
      request <- optionToFuture(
        podRequest(apiToken, podNamespace, labelSelector),
        s"Unable to form request; check Kubernetes environment (expecting env vars ${settings.apiServiceHostEnvName}, ${settings.apiServicePortEnvName})")

      response <- http.singleRequest(request, httpsContext)

      entity <- response.entity.toStrict(resolveTimeout)

      podList <- {

        response.status match {
          case StatusCodes.OK =>
//            log.debug("Kubernetes API entity: [{}]", entity.data.utf8String)
            val unmarshalled = Unmarshal(entity).to[PodList]
            unmarshalled.failed.foreach { t =>
              log.warn(
                "Failed to unmarshal Kubernetes API response.  Status code: [{}]; Response body: [{}]. Ex: [{}]",
                response.status.value,
                entity,
                t.getMessage)
            }
            unmarshalled
          case StatusCodes.Forbidden =>
            Unmarshal(entity).to[String].foreach { body =>
              log.warn("Forbidden to communicate with Kubernetes API server; check RBAC settings. Response: [{}]", body)
            }
            Future.failed(
              new KubernetesApiException("Forbidden when communicating with the Kubernetes API. Check RBAC settings."))
          case other =>
            Unmarshal(entity).to[String].foreach { body =>
              log.warn2(
                "Non-200 when communicating with Kubernetes API server. Status code: [{}]. Response body: [{}]",
                other,
                body)
            }

            Future.failed(new KubernetesApiException(s"Non-200 from Kubernetes API server: $other"))
        }

      }
    } yield {
      val addresses = targets(podList, settings.podPortName, podNamespace, settings.podDomain, settings.rawIp)
      if (addresses.isEmpty && podList.items.nonEmpty) {
        if (log.isInfoEnabled) {
          val containerPortNames = podList.items
            .flatMap(_.spec)
            .flatMap(_.containers)
            .flatMap(_.ports)
            .flatten
            .toSet
          log.info2(
            "No targets found from pod list. Is the correct port name configured? Current configuration: [{}]. Ports on pods: [{}]",
            settings.podPortName,
            containerPortNames)
        }
      }
      Resolved(addresses)
    }
  }

  def targets(podList: PodList,
              portName: Option[String],
              podNamespace: String,
              podDomain: String,
              rawIp: Boolean): Seq[ResolvedTarget] =
    for {
      item <- podList.items
      if item.metadata.flatMap(_.deletionTimestamp).isEmpty
      itemSpec <- item.spec.toSeq
      itemStatus <- item.status.toSeq
      if itemStatus.phase.contains("Running")
      ip <- itemStatus.podIP.toSeq
      // Maybe port is an Option of a port, and will be None if no portName was requested
      maybePort <- portName match {
        case None =>
          Seq(None)
        case Some(name) =>
          for {
            container <- itemSpec.containers
            ports <- container.ports.toSeq
            port <- ports
            if port.name.contains(name)
          } yield Some(port.containerPort)
      }
    } yield {
      val nodeName = itemSpec.nodeName
      val hostOrIp =
        if (rawIp) ip
        else s"${ip.replace('.', '-')}.$podNamespace.pod.$podDomain"
      ResolvedTarget(hostOrIp, maybePort, Some(InetAddress.getByName(ip)), nodeName)
    }

  private def optionToFuture[T](option: Option[T], failMsg: String): Future[T] =
    option.fold(Future.failed[T](new NoSuchElementException(failMsg)))(Future.successful)

  private def podRequest(token: String, namespace: String, labelSelector: String) =
    for {
      host <- sys.env.get(settings.apiServiceHostEnvName)
      portStr <- sys.env.get(settings.apiServicePortEnvName)
      port <- Try(portStr.toInt).toOption
    } yield {
      val path = Uri.Path.Empty / "api" / "v1" / "namespaces" / namespace / "pods"
      val query = Uri.Query("labelSelector" -> labelSelector)
      val uri = Uri
        .from(scheme = "https", host = host, port = port)
        .withPath(path)
        .withQuery(query)

      HttpRequest(uri = uri, headers = Seq(Authorization(OAuth2BearerToken(token))))
    }

}
