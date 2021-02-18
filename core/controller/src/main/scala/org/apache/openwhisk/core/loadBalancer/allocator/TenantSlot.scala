package org.apache.openwhisk.core.loadBalancer.allocator

sealed trait TenantSlot

object TenantSlot {

  case class TenantSlotList(slots: List[ResolvedTarget]) extends TenantSlot

  case class SlotReplyError(result: String) extends TenantSlot
}
