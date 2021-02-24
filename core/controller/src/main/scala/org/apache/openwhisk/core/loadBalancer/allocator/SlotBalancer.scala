package org.apache.openwhisk.core.loadBalancer.allocator

import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

object SlotBalancer {
  private var slotHash: mutable.Map[String, IndexedSeq[Int]] = mutable.Map()
  private var slotCounter: mutable.Map[String, AtomicLong] = mutable.Map()

  /**
   * Selects a slot based on round-robin algorithm.
   *
   * @param tenant String
   * @param slots List[ResolvedTarget]
   * @return ResolvedTarget
   */
  def select(tenant: String, slots: List[ResolvedTarget]): ResolvedTarget = {
    val slotHashSeq: IndexedSeq[Int] = (for (s <- slots) yield s.hashCode()).toIndexedSeq
    // create new sequence of hashes and counter for tenant
    if (!slotHash.contains(tenant)) {
      slotHash += (tenant -> slotHashSeq)
      slotCounter += (tenant -> new AtomicLong())
    } else if (slotHashSeq != slotHash(tenant)) {
      slotHash(tenant) = slotHashSeq
      slotCounter(tenant) = new AtomicLong()
    }

    val selectedHash = roundRobin(slotCounter(tenant), slotHash(tenant))
    slots.filter(s => s.hashCode() == selectedHash).head
  }

  /**
   * Uses round-robin algorithm to select a slot.
   *
   * @param counter AtomicLong
   * @param hashSeq IndexedSeq[Int]
   * @return Int
   */
  private def roundRobin(counter: AtomicLong, hashSeq: IndexedSeq[Int]): Int = {
    val size = hashSeq.size
    val index = (counter.getAndIncrement() % size).asInstanceOf[Int]
    hashSeq(if (index < 0) size + index else index)
  }
}
