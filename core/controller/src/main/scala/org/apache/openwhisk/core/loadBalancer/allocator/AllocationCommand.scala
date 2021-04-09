package org.apache.openwhisk.core.loadBalancer.allocator

import akka.actor.typed.ActorRef
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.Replicator.UpdateResponse

sealed trait AllocationCommand

object AllocationCommand {

  final case class AllocateSlots(tenantNames: Set[String], replyTo: ActorRef[Allocation]) extends AllocationCommand

  final case class AllocateSlot(tenantName: String, replyTo: ActorRef[Allocation]) extends AllocationCommand

  final case object GetBackendsTick extends AllocationCommand

  final case object GetPredictionsTick extends AllocationCommand

  final case object ReportTenantsAllocated extends AllocationCommand

  final case object ReportFragmentedSpace extends AllocationCommand

  final case object ReportUnallocatedSpace extends AllocationCommand

  final case object ReportSlotsVersion extends AllocationCommand

  case class InternalUpdateResponse[A <: ReplicatedData](rsp: UpdateResponse[A]) extends AllocationCommand
}
