package org.apache.openwhisk.core.loadBalancer.allocator

import akka.actor.typed.ActorRef
import akka.cluster.ClusterEvent.LeaderChanged

trait Commands

object Commands {
  final case object ReallocateSlots extends Commands
  final case class AllocateSlot(replyTo: ActorRef[Commands], tenantId: Short) extends Commands
  final case class SlotAllocation(tenantId: Short, slots: List[(Short, Byte)], version: Long) extends Commands
  final case class HttpRequestReceived(tenantId: String, replyTo: ActorRef[TenantSlot]) extends Commands
  final case object AllocateSlotTick extends Commands
  final case object ReportSlotsVersion extends Commands
  final case object ReportUnprocessedRequests extends Commands
  final case object ReportRequestsCount extends Commands
  final case object ReportTenantsAllocated extends Commands
  final case object ReportFragmentedSpace extends Commands
  final case object ReportUnallocatedSpace extends Commands

  trait InternalCommand extends Commands
  final case object Cleanup extends InternalCommand
  final case object LeaderCleanup extends InternalCommand
  final case class InternalClusterLeaderChanged(msg: LeaderChanged) extends InternalCommand
}
