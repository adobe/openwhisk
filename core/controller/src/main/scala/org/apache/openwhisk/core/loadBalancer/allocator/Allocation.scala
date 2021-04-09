package org.apache.openwhisk.core.loadBalancer.allocator

import akka.actor.typed.ActorRef
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.typed.scaladsl.Replicator

sealed trait Allocation

object Allocation {

  final case class SlotsAllocated(slots: Map[String, List[Int]], version: Long) extends Allocation

  final case class SlotAllocated(tenantName: String, slots: List[Int], version: Long) extends Allocation

  final case class HttpRequestReceived(tenantName: String, replyTo: ActorRef[TenantSlot]) extends Allocation

  final case class ReplyError(exception: String) extends Allocation

  final case class UpdateSlots(tenantName: String, slots: List[Int]) extends Allocation

  final case class UpdateTenants(slots: Map[String, List[Int]]) extends Allocation

  final case class DecorateTenantSlot(result: TenantSlot, replyTo: ActorRef[TenantSlot]) extends Allocation

  final case class ProcessException(exception: String) extends Allocation

  final case object ClearCache extends Allocation

  final case class InternalSubscribeResponse(chg: Replicator.SubscribeResponse[LWWMap[Int, ResolvedTarget]])
      extends Allocation

  final case object ReportSlotsVersion extends Allocation
}
