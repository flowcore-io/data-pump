import PromClient, { Counter, Gauge } from "prom-client"

export const dataPumpPromRegistry = new PromClient.Registry()

const bufferEventCountGauge = new Gauge({
  name: "flowcore_data_pump_buffer_events_gauge",
  help: "The number of events in the buffer",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const bufferReservedEventCountGauge = new Gauge({
  name: "flowcore_data_pump_buffer_reserved_events_gauge",
  help: "The number of reserved events in the buffer",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const bufferSizeBytesGauge = new Gauge({
  name: "flowcore_data_pump_buffer_size_bytes_gauge",
  help: "The size of the buffer in bytes",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const eventsAcknowledgedCounter = new Counter({
  name: "flowcore_data_pump_events_acknowledged_counter",
  help: "The number of events acknowledged",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const eventsFailedCounter = new Counter({
  name: "flowcore_data_pump_events_failed_counter",
  help: "The number of events failed",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const eventsPulledSizeBytesCounter = new Counter({
  name: "flowcore_data_pump_events_pulled_size_bytes_counter",
  help: "The size of the events pulled in bytes",
  labelNames: ["tenant", "data_core", "flow_type", "event_type"],
})

const sdkCommandsCounter = new Counter({
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
