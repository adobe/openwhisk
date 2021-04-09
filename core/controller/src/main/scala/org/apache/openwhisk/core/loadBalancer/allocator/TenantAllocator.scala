package org.apache.openwhisk.core.loadBalancer.allocator

import java.util.concurrent.TimeUnit

import akka.actor.typed.scaladsl.Behaviors._
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ddata.Replicator.WriteLocal
import akka.cluster.ddata._
import akka.cluster.ddata.typed.scaladsl.DistributedData.withReplicatorMessageAdapter
import akka.cluster.ddata.typed.scaladsl.Replicator.UpdateSuccess
import akka.cluster.ddata.typed.scaladsl.{DistributedData, Replicator, ReplicatorMessageAdapter}
import akka.util.Timeout
import Allocation._
import akka.actor.typed.scaladsl.ActorContext
import kamon.Kamon
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.loadBalancer.allocator.AllocationCommand.{
  AllocateSlot,
  AllocateSlots,
  GetBackendsTick,
  GetPredictionsTick,
  InternalUpdateResponse,
  ReportFragmentedSpace,
  ReportSlotsVersion,
  ReportTenantsAllocated,
  ReportUnallocatedSpace
}

import scala.concurrent.Await
import scala.concurrent.duration._

object TenantAllocator {
  implicit val timeout: Timeout = Timeout(5.seconds)

  val key: LWWMapKey[Int, ResolvedTarget] = LWWMapKey[Int, ResolvedTarget](_id = "itemsMap")

  def apply(kubernetesSystem: KubernetesBackends)(implicit logging: Logging): Behavior[AllocationCommand] = setup {
    context: ActorContext[AllocationCommand] =>
      withTimers[AllocationCommand] { timers =>
        timers.startTimerAtFixedRate(GetBackendsTick, 5.seconds)
        timers.startTimerAtFixedRate(GetPredictionsTick, 5.seconds)

        timers.startTimerWithFixedDelay(ReportSlotsVersion, ReportSlotsVersion, 500.milliseconds)
        timers.startTimerWithFixedDelay(ReportTenantsAllocated, ReportTenantsAllocated, 500.milliseconds)
        timers.startTimerWithFixedDelay(ReportFragmentedSpace, ReportFragmentedSpace, 500.milliseconds)
        timers.startTimerWithFixedDelay(ReportUnallocatedSpace, ReportUnallocatedSpace, 500.milliseconds)
        implicit val actorContext = context

        val slotsBackends: Resolved = Await.result(kubernetesSystem.getBackends(5.seconds), timeout.duration)

        logging.info(
          context.self,
          "[Allocator] Available backends: " + slotsBackends.getAddresses.values
            .mkString(","))
        implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
        withReplicatorMessageAdapter[AllocationCommand, LWWMap[Int, ResolvedTarget]] {
          replicator: ReplicatorMessageAdapter[AllocationCommand, LWWMap[Int, ResolvedTarget]] =>
            tenantLocation(
              kubernetesSystem,
              slotsBackends,
              SlotAllocations(slotsBackends),
              Map.empty[String, Short],
              0,
              replicator)
        }

      }
  }

  def tenantLocation(kubernetesSystem: KubernetesBackends,
                     slots: Resolved,
                     slotAllocations: SlotAllocations,
                     tenantMap: Map[String, Short],
                     lastTenantId: Short,
                     replicator: ReplicatorMessageAdapter[AllocationCommand, LWWMap[Int, ResolvedTarget]])(
    implicit logging: Logging,
    context: ActorContext[AllocationCommand]): Behavior[AllocationCommand] = {

    def allocateSlot(currentSlotAllocations: SlotAllocations, tenantId: Short, action: SlotAllocationAction) = {
      val span = Kamon.spanBuilder("allocate-slot-for-tenant").start()
      val newSlotAllocations = currentSlotAllocations.execute(action)
      val allocatedSlots = newSlotAllocations.get(tenantId)
      span.finish()
      (allocatedSlots, newSlotAllocations)
    }

    def allocateSpecificNumberForExistingTenant(currentSlotAllocations: SlotAllocations,
                                                tenantName: String,
                                                expectedSlots: Int): SlotAllocations = {
      val tenantId = tenantMap(tenantName)
      val action = currentSlotAllocations.actionSpecificNumber(tenantId, expectedSlots)
      val (_, newSlotAllocations) = allocateSlot(currentSlotAllocations, tenantId, action)
      newSlotAllocations
    }

    def getTenantId(tenantName: String) = {
      var newTenantMap = tenantMap
      val tenantId =
        if (tenantMap.contains(tenantName)) tenantMap(tenantName)
        else {
          if (lastTenantId > Math.min(slotAllocations.getSlotsNum, Short.MaxValue)) {
            throw new Exception("There are not enough slots to allocate a tenant")
          }
          val newTenantId = (lastTenantId + 1).toShort
          newTenantMap = tenantMap + (tenantName -> newTenantId)
          newTenantId
        }
      (tenantId, newTenantMap)
    }

    receive {

      case (_, AllocateSlots(tenantNames: Set[String], replyTo: ActorRef[Allocation])) =>
        val span = Kamon.spanBuilder("issuer-allocate-slots").start()
        PrometheusCollectors.requestsCountCounter.withTag("service", "allocator-issuer").increment()

        val slotsAllocated = tenantNames.foldLeft(Map.empty[String, List[Int]]) { (result, tenantName) =>
          {
            if (tenantMap.contains(tenantName)) {
              val tenantId = tenantMap(tenantName)
              if (slotAllocations.hasTenant(tenantId)) {
                val allocatedSlots = slotAllocations.get(tenantId)
                if (allocatedSlots.nonEmpty) {
                  result + (tenantName -> allocatedSlots)
                } else result
              } else result
            } else result
          }
        }

        replyTo ! SlotsAllocated(slotsAllocated, slotAllocations.version)
        span.finish()
        same

      case (_, AllocateSlot(tenantName, replyTo: ActorRef[Allocation])) =>
        PrometheusCollectors.requestsCountCounter
          .withTag("service", "allocator-issuer")
          .withTag("tenant", tenantName)
          .increment()
        val startTime: Long = System.nanoTime()
        logging.info(context.self, "[Allocator] Allocating slot for the tenant : " + tenantName)
        try {
          val (tenantId, newTenantMap) = getTenantId(tenantName)
          val action = slotAllocations.actionBestEffort(tenantId)
          incrementAllocationAction(tenantName, action)
          val (allocatedSlots, newSlotAllocations) = allocateSlot(slotAllocations, tenantId, action)
          val processingTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)
          PrometheusCollectors.requestProcessingGauge
            .withTag("service", "allocator-issuer")
            .withTag("tenant", tenantName)
            .update(processingTime)
          replyTo ! SlotAllocated(tenantName, allocatedSlots, newSlotAllocations.version)
          if (tenantId > lastTenantId) {
            //context.log.info(s"[Issuer] New slot was allocated for tenant [{}]", tenantName)
            tenantLocation(kubernetesSystem, slots, newSlotAllocations, newTenantMap, tenantId, replicator)
          } else {
            //context.log.info(s"[Issuer] Slot for tenant [{}] was read from a map", tenantName)
            same
          }
        } catch {
          case ex: Exception =>
            val processingTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)
            PrometheusCollectors.requestProcessingGauge
              .withTag("service", "allocator-issuer")
              .withTag("tenant", tenantName)
              .update(processingTime)
            replyTo ! ReplyError(ex.getMessage)
            same
        }

      case (_, ReportSlotsVersion) =>
        PrometheusCollectors.versionNumberGauge.withTag("service", "allocator-issuer").update(slotAllocations.version)
        same

      case (_, ReportTenantsAllocated) =>
        PrometheusCollectors.tenantNumberGauge
          .withTag("service", "allocator-issuer")
          .update(slotAllocations.tenantSlots.size)

        val tenantNameById = tenantMap.map(_.swap)
        slotAllocations.tenantSlots.foreach(
          tenantSlot =>
            PrometheusCollectors.slotsNumberGauge
              .withTag("service", "allocator-issuer")
              .withTag("tenant", tenantNameById(tenantSlot._1))
              .update(tenantSlot._2.size))

        same

      case (_, ReportUnallocatedSpace) =>
        PrometheusCollectors.unallocatedSpaceGauge
          .withTag("key", "slots")
          .update(
            (slotAllocations.lastAllocatedSlot._1 - 1) * slotAllocations.nodesLimit + slotAllocations.lastAllocatedSlot._2)
        same

      case (_, ReportFragmentedSpace) =>
        PrometheusCollectors.fragmentedSpaceGauge
          .withTag("service", "allocator-issuer")
          .update(slotAllocations.fragmentedSpace.size)
        same

      case (context, GetBackendsTick) =>
        val slotsBackends: Resolved = Await.result(kubernetesSystem.getBackends(5.seconds), timeout.duration)
        implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

        if (slotsBackends != slotAllocations.backends) {
          context.log.debug("Available backends: " + slotsBackends.getAddresses.values.mkString(","))
          val sortedAddressesLWWM: LWWMap[Int, ResolvedTarget] =
            slotsBackends.sortedAddresses.foldLeft(LWWMap.empty[Int, ResolvedTarget])((map, entry) => map :+ entry)

          replicator.askUpdate(
            Replicator.Update(key, LWWMap.empty[Int, ResolvedTarget], WriteLocal)(_ merge sortedAddressesLWWM),
            InternalUpdateResponse.apply)
          implicit val actorContext = context
          tenantLocation(
            kubernetesSystem,
            slotsBackends,
            slotAllocations ~ slotsBackends,
            tenantMap,
            lastTenantId,
            replicator)
        } else {
          same
        }

      case (context, GetPredictionsTick) =>
        //skip prediction if disabled
        if (!context.system.settings.config.getBoolean("akka.allocator.enable-predictions")) {
          context.log.debug("skipping predictions keys {}")
          same
        } else {

          /**
           *1. eviction: if tenant haven't been seen last 5min & it is not in prediction => evict
           *2. then take total number of tenants left, this would be the minimal number of pods we need.
           *3. upscale: if the minimal number of pods >= 80% of all pods initiate upscaling. 20% would be reserved capacity to allocate new tenants
           *4. split 80% of all pods on total number of tenants. This would be the number of pods we add for every percentage point in prediction.
           * (I.e. we have 250 pods, 100 tenants; divide 200 by 100 => 2; for every percentage point we allocate 2 pods to a tenant.)
           *5. Tenants not in prediction and not evicted would get one percentage point.
           *6. Run re-allocation
           *7. Tenant could be excluded from predictive auto-scaling via configuration, The value in configuration would tell how many pods they need
           */
          implicit val timeout: Timeout = Timeout(5 seconds)

          //@TODO 1. eviction: if tenant haven't been seen last 5min & it is not in prediction => evict

          val predictions: Prediction.Prediction =
            Await.result(new Predictions(context.system).get(timeout.duration), timeout.duration)
          context.log.debug("Predicted values: {}", predictions)

          val predictedTenants = predictions.future.filter { case (tenantId, valuess) => tenantMap.contains(tenantId) }

          //2. then take total number of tenants left, this would be the minimal number of pods we need.
          val totalTenantsSet = tenantMap.keySet

          val totalNumberOfTenants = totalTenantsSet.size
          val podsNeeded = slots.orderedKeys.size * 0.8

          if (podsNeeded < totalNumberOfTenants) {
            //@TODO 3. upscale: if the minimal number of pods >= 80% of all pods initiate upscaling. 20% would be reserved capacity to allocate new tenants
            context.log.error("Not enough backends based on predicted number of tenants")
          }
          //4. split 80% of all pods on total number of tenants. This would be the number of pods we add for every percentage point in prediction.
          val percentagePointValue = podsNeeded / 100

          //5. Tenants not in prediction and not evicted would get one percentage point.
          val existingTenantsMap =
            tenantMap.keySet.diff(predictedTenants.keySet).foldLeft(Map.empty[String, Double]) { (map, tenantName) =>
              map + (tenantName -> 0.01)
            }
          val totalTenantsMap = predictedTenants ++ existingTenantsMap

          //6. Run re-allocation
          //6a. First, downscale
          var newSlotAllocations = slotAllocations
          try {
            totalTenantsMap.foreach {
              case (tenantName, proportion) =>
                val desiredCapacity = Math.floor(proportion * 100 * percentagePointValue).toInt
                if (desiredCapacity < newSlotAllocations.get(tenantMap(tenantName)).size) {
                  context.log.debug(
                    "Shrink down tenant {} from {} to {}",
                    tenantName,
                    newSlotAllocations.get(tenantMap(tenantName)).size.toString,
                    desiredCapacity.toString)
                  newSlotAllocations = allocateSpecificNumberForExistingTenant(
                    newSlotAllocations,
                    tenantName,
                    (proportion * 100 * percentagePointValue).toInt)
                  //slotAllocations.scale(tenantMap(tenantName), (proportion * 100 * percentagePointValue).toInt)
                }

            }

            //6a. Then, upscale
            totalTenantsMap.foreach {
              case (tenantName, proportion) =>
                val desiredCapacity = Math.floor(proportion * 100 * percentagePointValue).toInt
                if (desiredCapacity > newSlotAllocations.get(tenantMap(tenantName)).size) {
                  context.log.debug(
                    "Scale up tenant {} from {} to {}",
                    tenantName,
                    newSlotAllocations.get(tenantMap(tenantName)).size.toString,
                    desiredCapacity.toString)
                  newSlotAllocations = allocateSpecificNumberForExistingTenant(
                    newSlotAllocations,
                    tenantName,
                    (proportion * 100 * percentagePointValue).toInt)
                  //slotAllocations.scale(tenantMap(tenantName), (proportion * 100 * percentagePointValue).toInt)
                }

            }
            implicit val actorContext = context
            tenantLocation(kubernetesSystem, slots, newSlotAllocations, tenantMap, lastTenantId, replicator)
          } catch {
            case ex: Exception =>
              context.log.error(ex.getMessage)
              //roll-back all the changes
              same
          }

          //@TODO 7. Tenant could be excluded from predictive auto-scaling via configuration, The value in configuration would tell how many pods they need

        }

      case (_, InternalUpdateResponse(UpdateSuccess(_))) =>
        same
    }
  }

  def incrementAllocationAction(tenantName: String, action: SlotAllocationAction): Unit = {
    val metric = action match {
      case SlotAllocationAction.NoAction              => PrometheusCollectors.allocationNoActionsCounter
      case SlotAllocationAction.Redistribute(_, _, _) => PrometheusCollectors.allocationRedistributionsCounter
      case SlotAllocationAction.Expand(_, _)          => PrometheusCollectors.allocationExpansionsCounter
      case SlotAllocationAction.Shrink(_, _)          => PrometheusCollectors.allocationExpansionsCounter
    }
    metric
      .withTag("service", "allocator-issuer")
      .withTag("tenant", tenantName)
      .increment()
  }
}
