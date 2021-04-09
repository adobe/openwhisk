package org.apache.openwhisk.core.loadBalancer.allocator

import kamon.Kamon
import kamon.metric.Metric

object PrometheusCollectors {

  val currentResponseTime: Metric.Histogram =
    Kamon.histogram("followerResponseTime", "Response time on the follower")
  val readerResponseTime: Metric.Gauge =
    Kamon.gauge("readerResponseTime", "Tenant allocation time on the reader")
  val versionNumberGauge: Metric.Gauge =
    Kamon.gauge("versionNumber", "Current Version Number of the distributed data key")
  val requestsCountCounter: Metric.Counter =
    Kamon.counter("requestsCounter", "Current number of processed requests on Leader")
  val readerErrorsCounter: Metric.Counter =
    Kamon.counter("readerErrorsCounter", "Number of errors on reader when requesting issuer")
  val readersRequestsCounter: Metric.Counter =
    Kamon.counter("readersRequestsCounter", "Current number of requests received by Readers")
  val requestProcessingGauge: Metric.Gauge =
    Kamon.gauge("requestProcessingGauge", "Current request processing time on Leader")
  val allocationNoActionsCounter: Metric.Counter =
    Kamon.counter("allocationNoActionsCounter", description = "Number of allocation requests requiring no action")
  val allocationExpansionsCounter: Metric.Counter =
    Kamon.counter(
      "allocationExpansionsCounter",
      description = "Allocation requests resulting in allocation of empty slots")
  val allocationRedistributionsCounter: Metric.Counter =
    Kamon.counter(
      "allocationRedistributionsCounter",
      description = "Allocation requests resulting in redistributing non-empty slots")
  val tenantNumberGauge: Metric.Gauge =
    Kamon.gauge("tenantNumberGauge", "Current number of allocated tenants")
  val slotsNumberGauge: Metric.Gauge =
    Kamon.gauge("slotsNumberGauge", "Current number of allocated slots")
  val fragmentedSpaceGauge: Metric.Gauge =
    Kamon.gauge("fragmentedSpaceGauge", "Current size of the fragmented space")
  val unallocatedSpaceGauge: Metric.Gauge =
    Kamon.gauge("unallocatedSpaceGauge", "Current size of the unallocated space")
  val httpForwardResponseTime: Metric.Gauge =
    Kamon.gauge("httpForwardResponseTime", "Response time of forwarding HTTP request to the allocated tenant")
}
