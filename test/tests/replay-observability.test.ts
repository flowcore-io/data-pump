import { describe, expect, it } from "bun:test"
import type { EventListOutput, FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { register as defaultPromRegistry, Registry } from "prom-client"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"
import type { FlowcoreDataPumpState, FlowcoreDataPumpStateManager } from "../../src/data-pump/types.ts"
import {
  REPLAY_BATCH_SIZE_BUCKETS,
  REPLAY_DURATION_BUCKETS_SECONDS,
  REPLAY_FETCH_RESULTS,
  REPLAY_IDLE_REASONS,
  REPLAY_STAGE_RESULTS,
  ReplayStageObserver,
  createDataPumpMetrics,
  metrics,
  replayBatchSizeBucket,
} from "../../src/data-pump/metrics.ts"

const source = {
  tenant: "test-tenant",
  data_core: "test-data-core",
  flow_type: "test-flow-type",
}

function metricValue(
  values: ReadonlyArray<{
    labels: Partial<Record<string, string | number>>
    value: number
    metricName?: string
  }>,
  expectedLabels: Record<string, string>,
  metricName?: string,
): number | undefined {
  return values.find(
    (entry) =>
      (!metricName || entry.metricName === metricName) &&
      Object.entries(expectedLabels).every(([name, value]) => entry.labels[name] === value),
  )?.value
}

describe("replay observability metrics", () => {
  it("registers replay stage and existing bounded buffer metrics in an isolated registry", () => {
    const registry = new Registry()
    const isolatedMetrics = createDataPumpMetrics(registry)

    expect(registry.getMetricsAsArray().map((metric) => metric.name)).toEqual(
      expect.arrayContaining([
        "flowcore_data_pump_replay_fetch_duration_seconds",
        "flowcore_data_pump_replay_fetch_events",
        "flowcore_data_pump_replay_handler_duration_seconds",
        "flowcore_data_pump_replay_acknowledgement_duration_seconds",
        "flowcore_data_pump_replay_checkpoint_duration_seconds",
        "flowcore_data_pump_replay_idle_duration_seconds",
        "flowcore_data_pump_buffer_events_gauge",
        "flowcore_data_pump_buffer_reserved_events_gauge",
        "flowcore_data_pump_buffer_size_bytes_gauge",
      ]),
    )

    expect(defaultPromRegistry.getSingleMetric("flowcore_data_pump_buffer_events_gauge")).toBeDefined()
    expect(defaultPromRegistry.getSingleMetric("flowcore_data_pump_replay_fetch_duration_seconds")).toBeDefined()
    expect((isolatedMetrics.replayFetchDuration as unknown as { labelNames: string[] }).labelNames).toEqual([
      "tenant",
      "data_core",
      "flow_type",
      "result",
    ])
  })

  it("uses finite batch-size buckets and duration buckets that cover slow replays", () => {
    expect(REPLAY_BATCH_SIZE_BUCKETS).toEqual(["empty", "1", "2-10", "11-100", "101-1000", "1001+"])
    expect(REPLAY_FETCH_RESULTS).toEqual(["success", "no_events", "error"])
    expect(REPLAY_STAGE_RESULTS).toEqual(["success", "error"])
    expect(REPLAY_IDLE_REASONS).toEqual(["no_events", "buffer_full", "waiting_for_events"])
    expect([0, 1, 2, 10, 11, 100, 101, 1000, 1001].map(replayBatchSizeBucket)).toEqual([
      "empty",
      "1",
      "2-10",
      "2-10",
      "11-100",
      "11-100",
      "101-1000",
      "101-1000",
      "1001+",
    ])
    expect(REPLAY_DURATION_BUCKETS_SECONDS).toContain(10)
    expect(REPLAY_DURATION_BUCKETS_SECONDS).toContain(60)
    expect(REPLAY_DURATION_BUCKETS_SECONDS[REPLAY_DURATION_BUCKETS_SECONDS.length - 1]).toBeGreaterThanOrEqual(120)
  })

  it("observes fetch duration, finite results, and returned event counts", async () => {
    const registry = new Registry()
    const stageMetrics = createDataPumpMetrics(registry)
    const clockValues = [1_000, 8_000, 10_000, 12_000, 20_000, 23_000]
    const observer = new ReplayStageObserver(stageMetrics, source, () => clockValues.shift()!)

    await observer.observeFetch(() => Promise.resolve({ events: [{}, {}] }))
    await observer.observeFetch(() => Promise.resolve({ events: [] }))
    await expect(observer.observeFetch(() => Promise.reject(new Error("fetch failed")))).rejects.toThrow("fetch failed")

    const duration = await stageMetrics.replayFetchDuration.get()
    expect(metricValue(duration.values, { ...source, result: "success", le: "+Inf" })).toBe(1)
    expect(metricValue(duration.values, { ...source, result: "no_events", le: "+Inf" })).toBe(1)
    expect(metricValue(duration.values, { ...source, result: "error", le: "+Inf" })).toBe(1)
    expect(
      metricValue(
        duration.values,
        { ...source, result: "success" },
        "flowcore_data_pump_replay_fetch_duration_seconds_sum",
      ),
    ).toBe(7)

    const returned = await stageMetrics.replayFetchEvents.get()
    expect(metricValue(returned.values, { ...source, le: "+Inf" })).toBe(2)
    expect(metricValue(returned.values, source, "flowcore_data_pump_replay_fetch_events_sum")).toBe(2)
  })

  it("observes handler result with a finite batch-size bucket", async () => {
    const registry = new Registry()
    const stageMetrics = createDataPumpMetrics(registry)
    const clockValues = [0, 500, 1_000, 2_500]
    const observer = new ReplayStageObserver(stageMetrics, source, () => clockValues.shift()!)

    await observer.observeHandler(new Array(25), () => Promise.resolve())
    await expect(
      observer.observeHandler(new Array(1_001), () => Promise.reject(new Error("handler failed"))),
    ).rejects.toThrow("handler failed")

    const duration = await stageMetrics.replayHandlerDuration.get()
    expect(
      metricValue(duration.values, { ...source, result: "success", batch_size_bucket: "11-100", le: "+Inf" }),
    ).toBe(1)
    expect(metricValue(duration.values, { ...source, result: "error", batch_size_bucket: "1001+", le: "+Inf" })).toBe(1)
  })

  it("observes acknowledgement, checkpoint, and bounded idle reasons", async () => {
    const registry = new Registry()
    const stageMetrics = createDataPumpMetrics(registry)
    let now = 0
    const observer = new ReplayStageObserver(stageMetrics, source, () => {
      now += 250
      return now
    })

    observer.observeAcknowledgement(() => undefined)
    expect(() =>
      observer.observeAcknowledgement(() => {
        throw new Error("ack failed")
      }),
    ).toThrow("ack failed")
    await observer.observeCheckpoint(() => Promise.resolve())
    await expect(observer.observeCheckpoint(() => Promise.reject(new Error("checkpoint failed")))).rejects.toThrow(
      "checkpoint failed",
    )
    await observer.observeIdle("no_events", () => Promise.resolve())
    await observer.observeIdle("buffer_full", () => Promise.resolve())
    await observer.observeIdle("waiting_for_events", () => Promise.resolve())

    const acknowledgement = await stageMetrics.replayAcknowledgementDuration.get()
    expect(metricValue(acknowledgement.values, { ...source, result: "success", le: "+Inf" })).toBe(1)
    expect(metricValue(acknowledgement.values, { ...source, result: "error", le: "+Inf" })).toBe(1)

    const checkpoint = await stageMetrics.replayCheckpointDuration.get()
    expect(metricValue(checkpoint.values, { ...source, result: "success", le: "+Inf" })).toBe(1)
    expect(metricValue(checkpoint.values, { ...source, result: "error", le: "+Inf" })).toBe(1)

    const idle = await stageMetrics.replayIdleDuration.get()
    expect(metricValue(idle.values, { ...source, reason: "no_events", le: "+Inf" })).toBe(1)
    expect(metricValue(idle.values, { ...source, reason: "buffer_full", le: "+Inf" })).toBe(1)
    expect(metricValue(idle.values, { ...source, reason: "waiting_for_events", le: "+Inf" })).toBe(1)
  })

  it("preserves the receiver when checkpointing through a class-based state manager", async () => {
    class ClassBasedStateManager implements FlowcoreDataPumpStateManager {
      public state: FlowcoreDataPumpState | null = null

      public getState(): FlowcoreDataPumpState | null {
        return this.state
      }

      public setState(state: FlowcoreDataPumpState): void {
        this.state = state
      }
    }

    const stateManager = new ClassBasedStateManager()
    const eventId = TimeUuid.now().toString()
    const pump = FlowcoreDataPump.create({
      auth: { apiKey: "fc_testid_testsecret" },
      dataSource: {
        tenant: "test-tenant",
        dataCore: "test-data-core",
        flowType: "test-flow-type",
        eventTypes: ["test.created.0"],
      },
      stateManager,
      notifier: { type: "poller", intervalMs: 60_000 },
      noTranslation: true,
    })

    await (pump as unknown as { updateState: (checkpointEventId: string) => Promise<void> | void }).updateState(eventId)

    expect(stateManager.state?.eventId).toBe(eventId)
  })

  it("wires fetch, handler, acknowledgement, checkpoint, and buffer observations into the pump", async () => {
    const tenant = `replay-observability-${crypto.randomUUID()}`
    const wiredSource = { tenant, data_core: "test-data-core", flow_type: "test-flow-type" }
    const eventType = "test.created.0"
    const eventId = TimeUuid.now().toString()
    const event: FlowcoreEvent = {
      eventId,
      timeBucket: "20260804120000",
      tenant,
      dataCoreId: wiredSource.data_core,
      flowType: wiredSource.flow_type,
      eventType,
      metadata: {},
      validTime: new Date().toISOString(),
      payload: { measured: true },
    }

    class SinglePageDataSource extends FlowcoreDataSource {
      private fetched = false

      public override getClosestTimeBucket(): Promise<string> {
        return Promise.resolve("20260804120000")
      }

      public override getNextTimeBucket(): Promise<null> {
        return Promise.resolve(null)
      }

      public override getEvents(): Promise<EventListOutput> {
        if (this.fetched) return new Promise(() => {})
        this.fetched = true
        return Promise.resolve({ events: [event], nextCursor: undefined })
      }
    }

    const dataSource = new SinglePageDataSource({
      auth: { apiKey: "fc_testid_testsecret" },
      dataSource: {
        tenant,
        dataCore: wiredSource.data_core,
        flowType: wiredSource.flow_type,
        eventTypes: [eventType],
      },
      noTranslation: true,
    })

    let pump!: FlowcoreDataPump
    let completed!: () => void
    let acknowledgementObservedBeforeCheckpoint = false
    let checkpointObservedDuringCheckpoint = false
    const completion = new Promise<void>((resolve) => {
      completed = resolve
    })
    pump = FlowcoreDataPump.create(
      {
        auth: { apiKey: "fc_testid_testsecret" },
        dataSource: {
          tenant,
          dataCore: wiredSource.data_core,
          flowType: wiredSource.flow_type,
          eventTypes: [eventType],
        },
        bufferSize: 1,
        stateManager: {
          getState: () => ({ timeBucket: "20260804120000", eventId }),
          setState: async () => {
            const acknowledgement = await metrics.replayAcknowledgementDuration.get()
            acknowledgementObservedBeforeCheckpoint =
              metricValue(acknowledgement.values, { ...wiredSource, result: "success", le: "+Inf" }) === 1
            const checkpoint = await metrics.replayCheckpointDuration.get()
            checkpointObservedDuringCheckpoint =
              metricValue(checkpoint.values, { ...wiredSource, result: "success", le: "+Inf" }) === 1
            pump.stop()
            setTimeout(completed, 0)
          },
        },
        processor: { concurrency: 1, handler: () => Promise.resolve() },
        notifier: { type: "poller", intervalMs: 60_000 },
        noTranslation: true,
      },
      dataSource,
    )

    void pump.start(() => {})
    await Promise.race([
      completion,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("pump did not complete")), 1_000)),
    ])

    const fetchDuration = await metrics.replayFetchDuration.get()
    expect(metricValue(fetchDuration.values, { ...wiredSource, result: "success", le: "+Inf" })).toBe(1)
    const handlerDuration = await metrics.replayHandlerDuration.get()
    expect(
      metricValue(handlerDuration.values, {
        ...wiredSource,
        result: "success",
        batch_size_bucket: "1",
        le: "+Inf",
      }),
    ).toBe(1)
    const acknowledgementDuration = await metrics.replayAcknowledgementDuration.get()
    expect(metricValue(acknowledgementDuration.values, { ...wiredSource, result: "success", le: "+Inf" })).toBe(1)
    expect(acknowledgementObservedBeforeCheckpoint).toBe(true)
    expect(checkpointObservedDuringCheckpoint).toBe(false)
    const checkpointDuration = await metrics.replayCheckpointDuration.get()
    expect(metricValue(checkpointDuration.values, { ...wiredSource, result: "success", le: "+Inf" })).toBe(1)

    const bufferEvents = await metrics.bufferEventCountGauge.get()
    expect(metricValue(bufferEvents.values, { ...wiredSource, event_type: eventType })).toBe(0)
    const bufferBytes = await metrics.bufferSizeBytesGauge.get()
    expect(metricValue(bufferBytes.values, { ...wiredSource, event_type: eventType })).toBe(0)
  })
})
