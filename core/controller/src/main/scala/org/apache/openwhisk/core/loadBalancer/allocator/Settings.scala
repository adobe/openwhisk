package org.apache.openwhisk.core.loadBalancer.allocator

import java.util.Optional

import akka.actor._
import com.typesafe.config.Config

import scala.compat.java8.OptionConverters._

final class Settings(system: ExtendedActorSystem) extends Extension {

  /**
   * Copied from AkkaManagementSettings, which we don't depend on.
   */
  private implicit class HasDefined(val config: Config) {
    def hasDefined(key: String): Boolean =
      config.hasPath(key) &&
        config.getString(key).trim.nonEmpty &&
        config.getString(key) != s"<$key>"

    def optDefinedValue(key: String): Option[String] =
      if (hasDefined(key)) Some(config.getString(key)) else None
  }

  private val kubernetesApi =
    system.settings.config.getConfig("akka.isolates.backends")

  val isMockBackends: Boolean = kubernetesApi.getBoolean("mock-backends")
  val mockBackendsNumber: Int = kubernetesApi.getInt("mock-backends-number")

  val apiCaPath: String =
    kubernetesApi.getString("api-ca-path")

  val apiTokenPath: String =
    kubernetesApi.getString("api-token-path")

  val apiServiceHostEnvName: String =
    kubernetesApi.getString("api-service-host-env-name")

  val apiServicePortEnvName: String =
    kubernetesApi.getString("api-service-port-env-name")

  val podNamespacePath: String =
    kubernetesApi.getString("pod-namespace-path")

  val podPortName: Option[String] =
    kubernetesApi.optDefinedValue("pod-port-name")

  val nodejsServiceName: String =
    kubernetesApi.getString("nodejs-service-name")

  val nodejsPodAppLabel: String =
    kubernetesApi.getString("nodejs-pod-app-label")

  /** Scala API */
  val podNamespace: Option[String] =
    kubernetesApi.optDefinedValue("pod-namespace")

  /** Java API */
  def getPodNamespace: Optional[String] = podNamespace.asJava

  val podDomain: String =
    kubernetesApi.getString("pod-domain")

  def podLabelSelector(name: String): String =
    kubernetesApi.getString("pod-label-selector").format(name)

  lazy val rawIp: Boolean = kubernetesApi.getBoolean("use-raw-ip")

  override def toString =
    s"Settings($apiCaPath, $apiTokenPath, $apiServiceHostEnvName, $apiServicePortEnvName, " +
      s"$podNamespacePath, $podNamespace, $podDomain, $isMockBackends, $mockBackendsNumber)"
}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {
  override def get(system: ActorSystem): Settings = super.get(system)

  override def lookup: Settings.type = Settings

  override def createExtension(system: ExtendedActorSystem): Settings =
    new Settings(system)
}
