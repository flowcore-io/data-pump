import PromClient, { Counter, Gauge } from "prom-client"

export const dataPumpPromRegistry: PromClient.Registry<"text/plain; version=0.0.4; charset=utf-8"> = new PromClient
  .Registry()

const bufferEventCountGauge: Gauge<"tenant" | "data_core" | "flow_type" | "event_type"> = new Gauge({
  name: "flowcore_data_pump_buffer_events_gauge",
  help: "The number of events in the buffer",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const bufferReservedEventCountGauge: Gauge<"tenant" | "data_core" | "flow_type" | "event_type"> = new Gauge({
  name: "flowcore_data_pump_buffer_reserved_events_gauge",
  help: "The number of reserved events in the buffer",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const bufferSizeBytesGauge: Gauge<"tenant" | "data_core" | "flow_type" | "event_type"> = new Gauge({
  name: "flowcore_data_pump_buffer_size_bytes_gauge",
  help: "The size of the buffer in bytes",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const eventsAcknowledgedCounter: Counter<"tenant" | "data_core" | "flow_type" | "event_type"> = new Counter({
  name: "flowcore_data_pump_events_acknowledged_counter",
  help: "The number of events acknowledged",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const eventsFailedCounter: Counter<"tenant" | "data_core" | "flow_type" | "event_type"> = new Counter({
  name: "flowcore_data_pump_events_failed_counter",
  help: "The number of events failed",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const eventsPulledSizeBytesCounter: Counter<"tenant" | "data_core" | "flow_type" | "event_type"> = new Counter({
  name: "flowcore_data_pump_events_pulled_size_bytes_counter",
  help: "The size of the events pulled in bytes",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const sdkCommandsCounter: Counter<"command"> = new Counter({
  name: "flowcore_data_pump_sdk_commands_counter",
  help: "The number of SDK commands",
  labelNames: ["command"],
})

dataPumpPromRegistry.registerMetric(bufferEventCountGauge)
dataPumpPromRegistry.registerMetric(bufferReservedEventCountGauge)
dataPumpPromRegistry.registerMetric(bufferSizeBytesGauge)
dataPumpPromRegistry.registerMetric(eventsAcknowledgedCounter)
dataPumpPromRegistry.registerMetric(eventsFailedCounter)
dataPumpPromRegistry.registerMetric(eventsPulledSizeBytesCounter)
dataPumpPromRegistry.registerMetric(sdkCommandsCounter)

export const metrics = {
  bufferEventCountGauge,
  bufferReservedEventCountGauge,
  bufferSizeBytesGauge,
  eventsAcknowledgedCounter,
  eventsFailedCounter,
  eventsPulledSizeBytesCounter,
  sdkCommandsCounter,
}

// #region Cluster Metrics

const activeWorkersGauge: Gauge<string> = new Gauge({
  name: "flowcore_data_pump_cluster_active_workers_gauge",
  help: "The number of active worker connections",
})

const leaderStatusGauge: Gauge<string> = new Gauge({
  name: "flowcore_data_pump_cluster_leader_status_gauge",
  help: "Whether this instance is the leader (1) or not (0)",
})

const eventsDistributedCounter: Counter<string> = new Counter({
  name: "flowcore_data_pump_cluster_events_distributed_counter",
  help: "The number of events distributed to workers",
})

const workerAcksCounter: Counter<string> = new Counter({
  name: "flowcore_data_pump_cluster_worker_acks_counter",
  help: "The number of successful worker acknowledgements",
})

const workerFailsCounter: Counter<string> = new Counter({
  name: "flowcore_data_pump_cluster_worker_fails_counter",
  help: "The number of failed worker deliveries",
})

dataPumpPromRegistry.registerMetric(activeWorkersGauge)
dataPumpPromRegistry.registerMetric(leaderStatusGauge)
dataPumpPromRegistry.registerMetric(eventsDistributedCounter)
dataPumpPromRegistry.registerMetric(workerAcksCounter)
dataPumpPromRegistry.registerMetric(workerFailsCounter)

export const clusterMetrics = {
  activeWorkersGauge,
  leaderStatusGauge,
  eventsDistributedCounter,
  workerAcksCounter,
  workerFailsCounter,
}

// #endregion
