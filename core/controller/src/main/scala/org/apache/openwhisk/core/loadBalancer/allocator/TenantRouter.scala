package org.apache.openwhisk.core.loadBalancer.allocator

import java.util.concurrent.TimeUnit

import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern.{schedulerFromActorSystem, Askable}
import akka.actor.typed.scaladsl.Behaviors._
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.ddata.typed.scaladsl.DistributedData.withReplicatorMessageAdapter
import akka.cluster.ddata.typed.scaladsl.{DistributedData, Replicator, ReplicatorMessageAdapter}
import akka.cluster.ddata.{LWWMap, LWWMapKey, SelfUniqueAddress}
import akka.util.Timeout
import Allocation._
import TenantSlot.{SlotReplyError, TenantSlotList}
import kamon.Kamon
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.loadBalancer.allocator.AllocationCommand.{AllocateSlot, AllocateSlots}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object TenantRouter {

  val FollowerServiceKey: ServiceKey[Commands.HttpRequestReceived] =
    ServiceKey[Commands.HttpRequestReceived]("followerService")

  @volatile var lastSeenSlotsVersion: Long = 0

  val key: LWWMapKey[Int, ResolvedTarget] = LWWMapKey[Int, ResolvedTarget](_id = "itemsMap")

  def apply(leaderRef: ActorRef[AllocationCommand], kubernetesSystem: KubernetesBackends)(
    implicit logging: Logging): Behavior[Allocation] = setup { context =>
    withTimers[Allocation] { timers =>
      timers.startTimerWithFixedDelay(ReportSlotsVersion, ReportSlotsVersion, 500.milliseconds)
      timers.startTimerAtFixedRate(ClearCache, ClearCache, 50.milliseconds)
      implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
      implicit val timeout: Timeout = Timeout(5.seconds)
      val slotsBackends: Resolved = Await.result(kubernetesSystem.getBackends(5.seconds), timeout.duration)
      logging.info(
        context.self,
        "[Reader] Available backends: " + slotsBackends.getAddresses.values
          .mkString(","))
      withReplicatorMessageAdapter[Allocation, LWWMap[Int, ResolvedTarget]] {
        replicator: ReplicatorMessageAdapter[Allocation, LWWMap[Int, ResolvedTarget]] =>
          replicator.subscribe(key, InternalSubscribeResponse.apply)
          allocationInformer(
            leaderRef,
            Map.empty[String, List[ResolvedTarget]],
            slotsBackends.sortedAddresses,
            replicator)
      }
    }
  }

  def allocationInformer(
    leaderRef: ActorRef[AllocationCommand],
    tenantsSlots: Map[String, List[ResolvedTarget]],
    backends: Map[Int, ResolvedTarget],
    replicator: ReplicatorMessageAdapter[Allocation, LWWMap[Int, ResolvedTarget]]): Behavior[Allocation] =
    receive {
      case (context, InternalSubscribeResponse(chg @ Replicator.Changed(`key`))) =>
        val newBackends: Map[Int, ResolvedTarget] = chg.get(key).entries
        context.log.info(s"[Reader] received updated backends [{}] ", newBackends)
        context.log.debug(
          "[Reader] Available backends: " + newBackends.values
            .mkString(","))
        allocationInformer(leaderRef, tenantsSlots, newBackends, replicator)

      case (_, InternalSubscribeResponse(Replicator.Deleted(_))) =>
        unhandled // no deletes

      case (context, HttpRequestReceived(tenantName: String, replyTo: ActorRef[TenantSlot])) =>
        //        context.log.info(s"[Reader] Allocating slot for tenant [$tenantName]")
        PrometheusCollectors.readersRequestsCounter
          .withTag("service", "allocator-reader")
          .withTag("tenant", tenantName)
          .increment()

        import scala.concurrent.Future
        implicit val timeout: Timeout = 5.seconds
        implicit val system: ActorSystem[Nothing] = context.system

        if (tenantsSlots.contains(tenantName)) {
          replyTo ! TenantSlotList(tenantsSlots(tenantName))
          same
        } else {
          val startTime: Long = System.nanoTime()
          val future: Future[Allocation] = leaderRef.ask[Allocation](replyTo => AllocateSlot(tenantName, replyTo))

          context.pipeToSelf(future) {
            case Success(SlotAllocated(`tenantName`, slots, version)) =>
              val responseTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)
              PrometheusCollectors.currentResponseTime
                .withTag("service", "allocator-reader")
                .withTag("tenant", tenantName)
                .record(responseTime)
              PrometheusCollectors.readerResponseTime
                .withTag("service", "allocation-reader")
                .withTag("tenant", tenantName)
                .update(responseTime)
              lastSeenSlotsVersion = version
              val backendSlots: List[ResolvedTarget] =
                slots.foldLeft(List.empty[ResolvedTarget])((list, slotId) => list :+ backends(slotId))
              replyTo ! TenantSlotList(backendSlots)
              UpdateSlots(tenantName, slots)
            case Success(ReplyError(exception)) => {
              PrometheusCollectors.readerErrorsCounter
                .withTag("service", "allocator-reader")
                .withTag("tenant", tenantName)
                .increment()
              context.log.error2(
                s"[Reader] Slot for the tenant [{}] was not allocated with error: {}",
                tenantName,
                exception)
              val responseTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime)
              PrometheusCollectors.currentResponseTime
                .withTag("service", "allocator-reader")
                .withTag("tenant", tenantName)
                .record(responseTime)
              PrometheusCollectors.readerResponseTime
                .withTag("service", "allocator-reader")
                .withTag("tenant", tenantName)
                .update(responseTime)
              DecorateTenantSlot(SlotReplyError(exception), replyTo)
            }
            case Failure(exception) => {
              PrometheusCollectors.readerErrorsCounter
                .withTag("service", "allocator-reader")
                .withTag("tenant", tenantName)
                .increment()
              context.log.error2(
                s"[Reader] Slot for the tenant [{}] was not allocated with exception: {}",
                tenantName,
                exception)
              DecorateTenantSlot(SlotReplyError(exception.getMessage), replyTo)
            }
            case _ => DecorateTenantSlot(SlotReplyError("Something wrong with the slot allocation process."), replyTo)
          }
          same
        }

      case (_, ReportSlotsVersion) =>
        PrometheusCollectors.versionNumberGauge.withTag("service", "allocator-reader").update(lastSeenSlotsVersion)
        same

      case (context, ClearCache) =>
        if (tenantsSlots.isEmpty) {
          same
        } else {
          val span = Kamon.spanBuilder("reader-clear-cache").start()
          implicit val timeout: Timeout = 5.seconds
          implicit val system: ActorSystem[Nothing] = context.system
          val future: Future[Allocation] =
            leaderRef.ask[Allocation](replyTo => AllocateSlots(tenantsSlots.keySet, replyTo))
          context.pipeToSelf(future) {
            case Success(SlotsAllocated(slots, _)) => UpdateTenants(slots)
            case Failure(exception)                => ProcessException(exception.getMessage)
            case _                                 => ProcessException(s"Something went wrong with the slot allocation process.")
          }
          span.finish()

          allocationInformer(leaderRef, Map.empty[String, List[ResolvedTarget]], backends, replicator)
        }

      case (_, UpdateSlots(tenantName, slots)) =>
        val backendSlots: List[ResolvedTarget] =
          slots.foldLeft(List.empty[ResolvedTarget])((list, slotId) => list :+ backends(slotId))
        allocationInformer(leaderRef, tenantsSlots + (tenantName -> backendSlots), backends, replicator)

      case (_, UpdateTenants(slotsMap)) =>
        val tenantsBackends = slotsMap.foldLeft(Map.empty[String, List[ResolvedTarget]]) {
          case (map, (tenant, slots)) =>
            val backendSlots: List[ResolvedTarget] =
              slots.foldLeft(List.empty[ResolvedTarget])((list, slotId) => list :+ backends(slotId))
            map + (tenant -> backendSlots)
        }
        allocationInformer(leaderRef, tenantsBackends, backends, replicator)

      case (_, DecorateTenantSlot(result: TenantSlot, replyTo: ActorRef[TenantSlot])) =>
        replyTo ! result
        same

      case (context, ProcessException(exception: String)) =>
        context.log.error(exception)
        same
    }

}
