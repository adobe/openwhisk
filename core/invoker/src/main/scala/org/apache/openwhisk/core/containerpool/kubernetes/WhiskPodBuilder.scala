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

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

import io.fabric8.kubernetes.api.builder.Predicate
import io.fabric8.kubernetes.api.model.policy.{PodDisruptionBudget, PodDisruptionBudgetBuilder}
import io.fabric8.kubernetes.api.model.{
  ContainerBuilder,
  EnvVarBuilder,
  EnvVarSourceBuilder,
  IntOrString,
  LabelSelectorBuilder,
  Pod,
  PodBuilder,
  Quantity
}
import io.fabric8.kubernetes.client.NamespacedKubernetesClient
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.ByteSize

import scala.collection.JavaConverters._

class WhiskPodBuilder(client: NamespacedKubernetesClient, config: KubernetesClientConfig) {
  private val template = config.podTemplate.map(_.value.getBytes(UTF_8))
  private val actionContainerName = "user-action"
  private val actionContainerPredicate: Predicate[ContainerBuilder] = (cb) => cb.getName == actionContainerName

  // Building these here prevents sorting the map multiple times which could be expensive.
  private val blackBoxDiskHints = for {
    ephemeralConfig <- config.ephemeralStorage
    bbConfig <- ephemeralConfig.blackbox
    bbDisk <- bbConfig.sizeHints
  } yield
    bbDisk
      .map((l) => (ByteSize.fromString(l._1), l._2))
      .toSeq
      .sortBy(_._1)

  private val managedBoxDiskHints = for {
    ephemeralConfig <- config.ephemeralStorage
    bbConfig <- ephemeralConfig.managed
    managedDisk <- bbConfig.sizeHints
  } yield
    managedDisk
      .map((l) => (ByteSize.fromString(l._1), l._2))
      .toSeq
      .sortBy(_._1)

  def affinityEnabled: Boolean = config.userPodNodeAffinity.enabled

  def buildPodSpec(
    name: String,
    image: String,
    memory: ByteSize,
    environment: Map[String, String],
    labels: Map[String, String],
    config: KubernetesClientConfig,
    userProvidedImage: Boolean)(implicit transid: TransactionId): (Pod, Option[PodDisruptionBudget]) = {
    val envVars = environment.map {
      case (key, value) => new EnvVarBuilder().withName(key).withValue(value).build()
    }.toSeq ++ config.fieldRefEnvironment
      .map(_.map({
        case (key, value) =>
          new EnvVarBuilder()
            .withName(key)
            .withValueFrom(new EnvVarSourceBuilder().withNewFieldRef().withFieldPath(value).endFieldRef().build())
            .build()
      }).toSeq)
      .getOrElse(Seq.empty)

    val baseBuilder = template match {
      case Some(bytes) =>
        new PodBuilder(loadPodSpec(bytes))
      case None => new PodBuilder()
    }

    val pb1 = baseBuilder
      .editOrNewMetadata()
      .withName(name)
      .addToLabels("name", name)
      .addToLabels("user-action-pod", "true")
      .addToLabels(labels.asJava)
      .endMetadata()

    val specBuilder = pb1.editOrNewSpec().withRestartPolicy("Always")

    if (config.userPodNodeAffinity.enabled) {
      specBuilder
        .editOrNewAffinity()
        .editOrNewNodeAffinity()
        .editOrNewRequiredDuringSchedulingIgnoredDuringExecution()
        .addNewNodeSelectorTerm()
        .addNewMatchExpression()
        .withKey(config.userPodNodeAffinity.key)
        .withOperator("In")
        .withValues(config.userPodNodeAffinity.value)
        .endMatchExpression()
        .endNodeSelectorTerm()
        .endRequiredDuringSchedulingIgnoredDuringExecution()
        .endNodeAffinity()
        .endAffinity()
    }
    if (config.userPodInvokerAntiAffinity.enabled) {
      specBuilder
        .editOrNewAffinity()
        .editOrNewPodAntiAffinity()
        .addNewPreferredDuringSchedulingIgnoredDuringExecution()
        .withWeight(100)
        .withNewPodAffinityTerm()
        .withNewLabelSelector()
        .addToMatchLabels("invoker", labels("invoker"))
        .endLabelSelector()
        .withNewTopologyKey(config.userPodInvokerAntiAffinity.topologyKey)
        .endPodAffinityTerm()
        .endPreferredDuringSchedulingIgnoredDuringExecution()
        .endPodAntiAffinity()
        .endAffinity()
        .buildAffinity()
    }

    val containerBuilder = if (specBuilder.hasMatchingContainer(actionContainerPredicate)) {
      specBuilder.editMatchingContainer(actionContainerPredicate)
    } else specBuilder.addNewContainer()

    //if cpu scaling is enabled, calculate cpu from memory, 100m per 256Mi, min is 100m(.1cpu), max is 10000 (10cpu)
    val (cpuRequests: Map[String, Quantity], cpuLimits: Map[String, Quantity]) = config.cpuScaling
      .map { cpuConfig =>
        val (cpuReq, cpuLimits) = calculateCpu(cpuConfig, memory)
        val calculatedCpuRequests = Map("cpu" -> new Quantity(cpuReq + "m"))
        val calculatedCpuLimits = cpuLimits
          .map(limit => Map("cpu" -> new Quantity(limit + "m")))
          .getOrElse(Map.empty)
        (calculatedCpuRequests, calculatedCpuLimits)
      }
      .getOrElse(Map.empty -> Map.empty)

    val diskLimit = createDiskLimit(memory, userProvidedImage)

    //In container its assumed that env, port, resource limits are set explicitly
    //Here if any value exist in template then that would be overridden
    containerBuilder
      .withNewResources()
      //explicitly set requests and limits to same values
      .withLimits((Map("memory" -> new Quantity(memory.toMB + "Mi")) ++ cpuLimits ++ diskLimit).asJava)
      .withRequests((Map("memory" -> new Quantity(memory.toMB + "Mi")) ++ cpuRequests ++ diskLimit).asJava)
      .endResources()
      .withName(actionContainerName)
      .withImage(image)
      .withEnv(envVars.asJava)
      .addNewPort()
      .withContainerPort(8080)
      .withName("action")
      .endPort()
      //assumes there is no termination message specifically generated, so inclue the last log line as the message
      .withTerminationMessagePolicy("FallbackToLogsOnError")

    //If any existing context entry is present then "update" it else add new
    containerBuilder
      .editOrNewSecurityContext()
      .editOrNewCapabilities()
      .addToDrop("NET_RAW", "NET_ADMIN")
      .endCapabilities()
      .endSecurityContext()

    val pod = containerBuilder
      .endContainer()
      .endSpec()
      .build()
    val pdb = if (config.pdbEnabled) {
      Some(
        new PodDisruptionBudgetBuilder().withNewMetadata
          .withName(name)
          .addToLabels(labels.asJava)
          .endMetadata()
          .withNewSpec()
          .withMinAvailable(new IntOrString(1))
          .withSelector(new LabelSelectorBuilder().withMatchLabels(Map("name" -> name).asJava).build())
          .and
          .build)
    } else {
      None
    }
    (pod, pdb)
  }

  def calculateCpu(c: KubernetesCpuScalingConfig, memory: ByteSize): (Int, Option[Int]) = {
    val cpuPerMemorySegment = c.millicpus
    val cpuMin = c.millicpus
    val cpuMax = c.maxMillicpus
    val memorySegments = memory.toMB / c.memory.toMB
    val cpu = math.min(math.max(memorySegments * cpuPerMemorySegment, cpuMin), cpuMax).toInt

    val cpuLimit = c.cpuLimitScaling.map { limitConfig =>
      (math.max(limitConfig.minScalingFactor, limitConfig.maxScalingFactor - math.max(memorySegments - 1, 0)) * cpu).toInt
    }
    cpu -> cpuLimit
  }

  private def loadPodSpec(bytes: Array[Byte]): Pod = {
    val resources = client.load(new ByteArrayInputStream(bytes))
    resources.get().get(0).asInstanceOf[Pod]
  }

  private def createDiskLimit(memoryRequested: ByteSize, userImage: Boolean): Map[String, Quantity] = {
    config.ephemeralStorage
      .flatMap(possibleConfig => {
        val targetConfig = if (userImage) possibleConfig.blackbox else possibleConfig.managed
        targetConfig
          .map(diskConf => {
            if (diskConf.sizeHints.isDefined) {
              val hints = (if (userImage) blackBoxDiskHints else managedBoxDiskHints).get
              var diskSize = hints(0)
              hints.foreach(item => {
                if (memoryRequested >= item._1) {
                  diskSize = item
                }
              })
              Map("ephemeral-storage" -> new Quantity(diskSize._2.toMB + "Mi"))
            } else {
              val scaleFactor: Double = diskConf.scaleFactor.getOrElse(0)
              if ((scaleFactor > 0) && (scaleFactor * memoryRequested.toMB < diskConf.limit.toMB)) {
                Map("ephemeral-storage" -> new Quantity(scaleFactor * memoryRequested.toMB + "Mi"))
              } else {
                Map("ephemeral-storage" -> new Quantity(diskConf.limit.toMB + "Mi"))
              }
            }
          })
      })
      .getOrElse(Map.empty[String, Quantity])
  }
}
