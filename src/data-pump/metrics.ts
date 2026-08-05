import { Counter, Gauge, Histogram, register as defaultPromRegistry, Registry } from "prom-client"

export const dataPumpPromRegistry: Registry<"text/plain; version=0.0.4; charset=utf-8"> = new Registry()

const SOURCE_LABELS = ["tenant", "data_core", "flow_type"] as const
const EVENT_SOURCE_LABELS = [...SOURCE_LABELS, "event_type"] as const

export const REPLAY_DURATION_BUCKETS_SECONDS = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120,
] as const
export const REPLAY_EVENT_COUNT_BUCKETS = [1, 10, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000] as const
export const REPLAY_BATCH_SIZE_BUCKETS = ["empty", "1", "2-10", "11-100", "101-1000", "1001+"] as const
export const REPLAY_FETCH_RESULTS = ["success", "no_events", "error"] as const
export const REPLAY_STAGE_RESULTS = ["success", "error"] as const
export const REPLAY_IDLE_REASONS = ["no_events", "buffer_full", "waiting_for_events"] as const

export type ReplayBatchSizeBucket = (typeof REPLAY_BATCH_SIZE_BUCKETS)[number]
export type ReplayIdleReason = (typeof REPLAY_IDLE_REASONS)[number]
export type ReplaySourceLabels = {
  tenant: string
  data_core: string
  flow_type: string
}

export function replayBatchSizeBucket(size: number): ReplayBatchSizeBucket {
  if (size <= 0) return "empty"
  if (size === 1) return "1"
  if (size <= 10) return "2-10"
  if (size <= 100) return "11-100"
  if (size <= 1_000) return "101-1000"
  return "1001+"
}

export function createDataPumpMetrics(registry: Registry, includeDefaultRegistry = false) {
  const registers = includeDefaultRegistry ? [registry, defaultPromRegistry] : [registry]
  const bufferEventCountGauge = new Gauge({
    name: "flowcore_data_pump_buffer_events_gauge",
    help: "The number of events in the buffer",
    labelNames: EVENT_SOURCE_LABELS,
    registers,
  })

  const bufferReservedEventCountGauge = new Gauge({
    name: "flowcore_data_pump_buffer_reserved_events_gauge",
    help: "The number of reserved events in the buffer",
    labelNames: EVENT_SOURCE_LABELS,
    registers,
  })

  const bufferSizeBytesGauge = new Gauge({
    name: "flowcore_data_pump_buffer_size_bytes_gauge",
    help: "The size of the buffer in bytes",
    labelNames: EVENT_SOURCE_LABELS,
    registers,
  })

  const eventsAcknowledgedCounter = new Counter({
    name: "flowcore_data_pump_events_acknowledged_counter",
    help: "The number of events acknowledged",
    labelNames: EVENT_SOURCE_LABELS,
    registers,
  })

  const eventsFailedCounter = new Counter({
    name: "flowcore_data_pump_events_failed_counter",
    help: "The number of events failed",
    labelNames: EVENT_SOURCE_LABELS,
    registers,
  })

  const eventsPulledSizeBytesCounter = new Counter({
    name: "flowcore_data_pump_events_pulled_size_bytes_counter",
    help: "The size of the events pulled in bytes",
    labelNames: EVENT_SOURCE_LABELS,
    registers,
  })

  const sdkCommandsCounter = new Counter({
    name: "flowcore_data_pump_sdk_commands_counter",
    help: "The number of SDK commands",
    labelNames: ["command"] as const,
    registers,
  })

  const replayFetchDuration = new Histogram({
    name: "flowcore_data_pump_replay_fetch_duration_seconds",
    help: "Replay event fetch duration in seconds",
    labelNames: [...SOURCE_LABELS, "result"] as const,
    buckets: [...REPLAY_DURATION_BUCKETS_SECONDS],
    registers,
  })

  const replayFetchEvents = new Histogram({
    name: "flowcore_data_pump_replay_fetch_events",
    help: "Number of events returned by a replay fetch",
    labelNames: SOURCE_LABELS,
    buckets: [...REPLAY_EVENT_COUNT_BUCKETS],
    registers,
  })

  const replayHandlerDuration = new Histogram({
    name: "flowcore_data_pump_replay_handler_duration_seconds",
    help: "Replay handler duration in seconds",
    labelNames: [...SOURCE_LABELS, "result", "batch_size_bucket"] as const,
    buckets: [...REPLAY_DURATION_BUCKETS_SECONDS],
    registers,
  })

  const replayAcknowledgementDuration = new Histogram({
    name: "flowcore_data_pump_replay_acknowledgement_duration_seconds",
    help: "Replay acknowledgement duration in seconds, excluding checkpointing",
    labelNames: [...SOURCE_LABELS, "result"] as const,
    buckets: [...REPLAY_DURATION_BUCKETS_SECONDS],
    registers,
  })

  const replayCheckpointDuration = new Histogram({
    name: "flowcore_data_pump_replay_checkpoint_duration_seconds",
    help: "Replay state checkpoint duration in seconds",
    labelNames: [...SOURCE_LABELS, "result"] as const,
    buckets: [...REPLAY_DURATION_BUCKETS_SECONDS],
    registers,
  })

  const replayIdleDuration = new Histogram({
    name: "flowcore_data_pump_replay_idle_duration_seconds",
    help: "Time the replay loop spends idle",
    labelNames: [...SOURCE_LABELS, "reason"] as const,
    buckets: [...REPLAY_DURATION_BUCKETS_SECONDS, 300],
    registers,
  })

  return {
    bufferEventCountGauge,
    bufferReservedEventCountGauge,
    bufferSizeBytesGauge,
    eventsAcknowledgedCounter,
    eventsFailedCounter,
    eventsPulledSizeBytesCounter,
    sdkCommandsCounter,
    replayFetchDuration,
    replayFetchEvents,
    replayHandlerDuration,
    replayAcknowledgementDuration,
    replayCheckpointDuration,
    replayIdleDuration,
  }
}

export type DataPumpMetrics = ReturnType<typeof createDataPumpMetrics>

export const metrics = createDataPumpMetrics(dataPumpPromRegistry, true)

export class ReplayStageObserver {
  public constructor(
    private readonly stageMetrics: DataPumpMetrics,
    private readonly source: ReplaySourceLabels,
    private readonly now: () => number = () => performance.now(),
  ) {}

  public async observeFetch<T extends { events: unknown[] }>(operation: () => Promise<T>): Promise<T> {
    const startedAt = this.now()
    try {
      const result = await operation()
      this.stageMetrics.replayFetchDuration.observe(
        { ...this.source, result: result.events.length ? "success" : "no_events" },
        this.elapsedSeconds(startedAt),
      )
      this.stageMetrics.replayFetchEvents.observe(this.source, result.events.length)
      return result
    } catch (error) {
      this.stageMetrics.replayFetchDuration.observe({ ...this.source, result: "error" }, this.elapsedSeconds(startedAt))
      throw error
    }
  }

  public async observeHandler<T>(events: unknown[], operation: () => Promise<T>): Promise<T> {
    const startedAt = this.now()
    const batchSize = replayBatchSizeBucket(events.length)
    try {
      const result = await operation()
      this.stageMetrics.replayHandlerDuration.observe(
        { ...this.source, result: "success", batch_size_bucket: batchSize },
        this.elapsedSeconds(startedAt),
      )
      return result
    } catch (error) {
      this.stageMetrics.replayHandlerDuration.observe(
        { ...this.source, result: "error", batch_size_bucket: batchSize },
        this.elapsedSeconds(startedAt),
      )
      throw error
    }
  }

  public observeAcknowledgement<T>(operation: () => T): T {
    const startedAt = this.now()
    try {
      const result = operation()
      this.stageMetrics.replayAcknowledgementDuration.observe(
        { ...this.source, result: "success" },
        this.elapsedSeconds(startedAt),
      )
      return result
    } catch (error) {
      this.stageMetrics.replayAcknowledgementDuration.observe(
        { ...this.source, result: "error" },
        this.elapsedSeconds(startedAt),
      )
      throw error
    }
  }

  public observeCheckpoint<T>(operation: () => Promise<T> | T): Promise<T> {
    return this.observeResult(this.stageMetrics.replayCheckpointDuration, operation)
  }

  public async observeIdle<T>(reason: ReplayIdleReason, operation: () => Promise<T>): Promise<T> {
    const startedAt = this.now()
    try {
      return await operation()
    } finally {
      this.stageMetrics.replayIdleDuration.observe({ ...this.source, reason }, this.elapsedSeconds(startedAt))
    }
  }

  private async observeResult<T>(
    histogram: Histogram<"tenant" | "data_core" | "flow_type" | "result">,
    operation: () => Promise<T> | T,
  ): Promise<T> {
    const startedAt = this.now()
    try {
      const result = await operation()
      histogram.observe({ ...this.source, result: "success" }, this.elapsedSeconds(startedAt))
      return result
    } catch (error) {
      histogram.observe({ ...this.source, result: "error" }, this.elapsedSeconds(startedAt))
      throw error
    }
  }

  private elapsedSeconds(startedAt: number): number {
    return Math.max(0, this.now() - startedAt) / 1_000
  }
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
