package org.apache.openwhisk.core.loadBalancer.allocator

sealed trait SlotAllocationAction

object SlotAllocationAction {
  case object NoAction extends SlotAllocationAction
  final case class Expand(tenantId: Short, slotsRequired: Int) extends SlotAllocationAction
  final case class Shrink(tenantId: Short, slotsRequired: Int) extends SlotAllocationAction
  final case class Redistribute(tenantId: Short, slotsRequired: Int, shrinkTenantIds: List[Short])
      extends SlotAllocationAction
}
