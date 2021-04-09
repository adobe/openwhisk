package org.apache.openwhisk.core.loadBalancer.allocator

import scala.util.Random

import kamon.Kamon
import org.slf4j.{Logger, LoggerFactory}

object SlotAllocations {

  def apply(backends: Resolved): SlotAllocations =
    new SlotAllocations(
      backends,
      Map.empty[Short, List[(Short, Byte)]],
      Map.empty[Short, Map[Byte, Short]],
      List.empty[(Short, Byte)],
      (0, 0),
      0,
      new Random())

  def apply(backends: Resolved, rand: Random): SlotAllocations =
    new SlotAllocations(
      backends,
      Map.empty[Short, List[(Short, Byte)]],
      Map.empty[Short, Map[Byte, Short]],
      List.empty[(Short, Byte)],
      (0, 0),
      0,
      rand)
}

final class SlotAllocations(val backends: Resolved,
                            /**                  tenantId     node   slot */
                            val tenantSlots: Map[Short, List[(Short, Byte)]],
                            /**                      node       slot  tenantId */
                            val slotsAllocation: Map[Short, Map[Byte, Short]],
                            /**                        node    slot */
                            val fragmentedSpace: List[(Short, Byte)],
                            /**                     node    slot */
                            val lastAllocatedSlot: (Short, Byte),
                            val version: Long,
                            val rand: Random) {

  def scale(tenantId: Short, value: Int): Unit = {
    //first, unallocate those which has more than needed

    //then, allocate thos who needs more
  }

  //maximum number of tenants we can allocate
  private val tenantNum: Int =
    Math.min(backends.orderedKeys.size, Short.MaxValue) // 3000
  //number of available pods
  val nodesLimit: Int =
    Math.min(backends.groupedByNode.size, Short.MaxValue) // 5000
  //for now we assume that the number of pods per node is same across all nodes
  //@TODO allow various number of pods across nodes
  private val podsPerNodeLimit: Int =
    if (backends.groupedByNode.nonEmpty) backends.groupedByNode.head._2.size
    else 0
  private val slotsPerTenant = 5 //10
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def get(tenantId: Short): List[Int] = {
    val span = Kamon.spanBuilder("allocator-get-tenant-slots").start()
    val tenantSlots = getTenantBackends(tenantId)
    span.finish()
    tenantSlots
  }

  def actionSpecificNumber(tenantId: Short, slotsRequired: Int): SlotAllocationAction = {
    val currentSize = if (hasTenant(tenantId)) getTenant(tenantId).size else 0
    val addedCapacity = slotsRequired - currentSize
    if (addedCapacity == 0) SlotAllocationAction.NoAction
    else if (addedCapacity > 0) {
      if (!slotsLeft) {
        SlotAllocationAction.Redistribute(
          tenantId,
          addedCapacity,
          rand
            .shuffle(tenantSlots.keys)
            .take(Math.min(5, tenantSlots.size))
            .toList)
      } else {
        SlotAllocationAction.Expand(tenantId, addedCapacity)
      }
    } else {
      SlotAllocationAction.Shrink(tenantId, slotsRequired)
    }
  }

  def actionBestEffort(tenantId: Short): SlotAllocationAction = {
    if (hasTenant(tenantId)) SlotAllocationAction.NoAction
    else if (!slotsLeft) {
      SlotAllocationAction.Redistribute(
        tenantId,
        slotsPerTenant,
        rand
          .shuffle(tenantSlots.keys)
          .take(Math.min(5, tenantSlots.size))
          .toList)
    } else {
      SlotAllocationAction.Expand(tenantId, slotsPerTenant)
    }
  }

  def execute(action: SlotAllocationAction): SlotAllocations = action match {
    case SlotAllocationAction.NoAction => this
    case SlotAllocationAction.Redistribute(tenantId, slotsRequired, shrinkTenantIds) =>
      shrinkTenantIds.foldLeft(this)(_.shrink(_)).expand(tenantId, slotsRequired)
    case SlotAllocationAction.Expand(tenantId, slotsRequired) => this.expand(tenantId, slotsRequired)
    case SlotAllocationAction.Shrink(tenantId, slotsRequired) => this.shrinkToSize(tenantId, slotsRequired)
  }

  val allocate: Short => SlotAllocations = execute _ compose actionBestEffort

  private def expand(tenantId: Short, slotsRequired: Int): SlotAllocations = {
    var (slotsAllocationNew, tenantSlotsNew) = (slotsAllocation, tenantSlots)
    var (node, slot) = lastAllocatedSlot
    var currentVersion = version
    var currentFragmentedSpace = fragmentedSpace
    var allocatedCounter = 0

    //first try to allocate in the fragmented space
    while (currentFragmentedSpace.nonEmpty && allocatedCounter < slotsRequired) {
      val (slotsAllocationUpdated, tenantSlotsUpdated) =
        assignSlotToTenant(slotsAllocationNew, tenantSlotsNew, currentFragmentedSpace.head, tenantId)
      slotsAllocationNew = slotsAllocationUpdated
      tenantSlotsNew = tenantSlotsUpdated
      currentFragmentedSpace = currentFragmentedSpace.tail
      allocatedCounter = allocatedCounter + 1
      currentVersion = currentVersion + 1
    }

    //then, use all available slots to fill the rest of required allocations
    val nLimit = nodesLimit - 1
    val pLimit = podsPerNodeLimit - 1
    while (allocatedCounter < slotsRequired && !(node == nLimit && slot == pLimit)) {
      if (slot < pLimit) {
        slot = (if (currentVersion == 0) 0 else slot + 1).toByte
      } else if (node < nLimit) {
        node = if (currentVersion == 0) 0 else (node + 1).toShort
        slot = 0
      }
//      log.info(s"Current last allocated slot {}", currentLastAllocatedSlot)
      val (slotsAllocationUpdated, tenantSlotsUpdated) =
        assignSlotToTenant(slotsAllocationNew, tenantSlotsNew, (node, slot), tenantId)
      slotsAllocationNew = slotsAllocationUpdated
      tenantSlotsNew = tenantSlotsUpdated
      allocatedCounter = allocatedCounter + 1
      currentVersion = currentVersion + 1
    }

    new SlotAllocations(
      backends,
      tenantSlotsNew,
      slotsAllocationNew,
      currentFragmentedSpace,
      (node, slot),
      currentVersion,
      rand)
  }

  private def assignSlotToTenant(slotsAllocation: Map[Short, Map[Byte, Short]],
                                 tenantSlots: Map[Short, List[(Short, Byte)]],
                                 slotToAllocate: (Short, Byte),
                                 tenant: Short): (Map[Short, Map[Byte, Short]], Map[Short, List[(Short, Byte)]]) = {
    val (node, slot) = slotToAllocate
    if (slotsAllocation.contains(node) && slotsAllocation(node).contains(slot)) {
      log.error("Slot is already allocated ({},{})", node, slot)
      return (slotsAllocation, tenantSlots)
    }

    val nodeAssignments = slotsAllocation.get(node) match {
      case Some(b: Map[Byte, Short]) => b + (slot -> tenant)
      case None                      => Map(slot -> tenant)
    }
    val slotsAllocationNew = slotsAllocation + (node -> nodeAssignments)

    val tenantAssignments = tenantSlots.get(tenant) match {
      case Some(b: List[(Short, Byte)]) => b.::(node, slot)
      case None                         => List((node, slot))
    }
    val tenantSlotsNew = tenantSlots + (tenant -> tenantAssignments)
    (slotsAllocationNew, tenantSlotsNew)
  }

  private def shrinkToSize(tenantId: Short, expectedSize: Int): SlotAllocations = {
    //free up half pods allocated for a tenant
    val slotsToCut = tenantSlots(tenantId).size - expectedSize
    var newSlotsAllocation = slotsAllocation
    var newTenantSlots = tenantSlots
    var newFragmentedSpace = fragmentedSpace
    (1 to slotsToCut).foreach(_ => {
      val (nodeToDecomiss, podToDecomiss) = newTenantSlots(tenantId).head
      newTenantSlots = newTenantSlots + (tenantId -> newTenantSlots(tenantId).tail)
      val targetSlotList = newSlotsAllocation(nodeToDecomiss) - podToDecomiss
      if (targetSlotList.nonEmpty) {
        newSlotsAllocation = newSlotsAllocation + (nodeToDecomiss -> targetSlotList)
      } else {
        newSlotsAllocation -= nodeToDecomiss
      }
      newFragmentedSpace = newFragmentedSpace :+ (nodeToDecomiss, podToDecomiss)
    })
    new SlotAllocations(
      backends,
      newTenantSlots,
      newSlotsAllocation,
      newFragmentedSpace,
      lastAllocatedSlot,
      version + 1,
      rand)
  }

  private def shrink(tenantId: Short): SlotAllocations = {
    //free up half pods allocated for a tenant
    val slotsToCut = Math.min(tenantSlots(tenantId).size, slotsPerTenant) / 2
    val expectedSize = tenantSlots(tenantId).size - slotsToCut
    shrinkToSize(tenantId, expectedSize)
  }

  def hasTenant(tenantId: Short): Boolean = {
    tenantSlots.get(tenantId).isDefined
  }

  private def getTenantBackends(tenantId: Short): List[Int] = {
//    log.info("Backends map {}", backends)
//    log.info("Tenant slots {}", tenantSlots)
    if (!tenantSlots.contains(tenantId)) {
      List()
    } else {
//      tenantSlots(tenantId).foreach(
//        b =>
//          log
//            .info("Locating backends by key {}", b._1 * podsPerNodeLimit + b._2))
      tenantSlots(tenantId).foldLeft(List.empty[Int]) { (a, b) =>
        a :+ backends.getKeyByIndex(b._1 * podsPerNodeLimit + b._2)
      }
    }
  }

  def slotsLeft(): Boolean = {
    fragmentedSpace.nonEmpty || lastAllocatedSlot._1 < nodesLimit - 1 || lastAllocatedSlot._2 < podsPerNodeLimit - 1
  }

  def getSlotsNum: Int = backends.orderedKeys.size

  def ~(newBackends: Resolved): SlotAllocations = {
//    val missingKeys: Set[Int] =
//      backends.sortedAddresses.keySet.diff(newBackends.sortedAddresses.keySet)

    //@TODO implement consistent hashing to minimize number of slots moved when backends changed
    new SlotAllocations(
      newBackends,
      Map.empty[Short, List[(Short, Byte)]],
      Map.empty[Short, Map[Byte, Short]],
      List.empty[(Short, Byte)],
      (0, 0),
      version,
      rand)
  }

  def getTenant(tenantId: Short): List[(Short, Byte)] = {
    tenantSlots(tenantId)
  }

  def getTenants: Set[Short] = {
    tenantSlots.keySet
  }
}
