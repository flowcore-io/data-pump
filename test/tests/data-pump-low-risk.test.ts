import { afterEach, describe, expect, it } from "bun:test"
import type { EventListOutput, FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { metrics } from "../../src/data-pump/metrics.ts"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"
import type { FlowcoreLogger } from "../../src/data-pump/types.ts"

const pumps: FlowcoreDataPump[] = []

afterEach(() => {
  for (const pump of pumps.splice(0)) pump.stop()
})

function makeEvent(eventType: string, payload: Record<string, unknown>, offsetMs = 0): FlowcoreEvent {
  return {
    eventId: TimeUuid.fromDate(new Date(Date.now() + offsetMs)).toString(),
    timeBucket: "20260804120000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "test-flow-type",
    eventType,
    metadata: {},
    validTime: new Date().toISOString(),
    payload,
  }
}

class OnePageDataSource extends FlowcoreDataSource {
  private fetched = false

  constructor(private readonly page: FlowcoreEvent[]) {
    super({
      auth: { apiKey: "fc_testid_testsecret" },
      dataSource: {
        tenant: "test-tenant",
        dataCore: "test-data-core",
        flowType: "test-flow-type",
        eventTypes: ["alpha.0", "beta.0"],
      },
      noTranslation: true,
    })
  }

  public override getClosestTimeBucket(): Promise<string> {
    return Promise.resolve("20260804120000")
  }

  public override getNextTimeBucket(): Promise<null> {
    return Promise.resolve(null)
  }

  public override getEvents(): Promise<EventListOutput> {
    if (this.fetched) return new Promise(() => {})
    this.fetched = true
    return Promise.resolve({ events: this.page, nextCursor: undefined })
  }
}

async function createStartedPump(
  events: FlowcoreEvent[],
  maxRedeliveryCount = 3,
  setState: (state: { timeBucket: string; eventId?: string }) => Promise<void> | void = () => {},
  logger?: FlowcoreLogger,
  achknowledgeTimeoutMs = 60_000,
): Promise<FlowcoreDataPump> {
  const source = new OnePageDataSource(events)
  const pump = FlowcoreDataPump.create(
    {
      auth: { apiKey: "fc_testid_testsecret" },
      dataSource: {
        tenant: "test-tenant",
        dataCore: "test-data-core",
        flowType: "test-flow-type",
        eventTypes: ["alpha.0", "beta.0"],
      },
      stateManager: { getState: () => null, setState },
      notifier: { type: "poller", intervalMs: 60_000 },
      bufferSize: Math.max(events.length, 1),
      achknowledgeTimeoutMs,
      maxRedeliveryCount,
      noTranslation: true,
      logger,
    },
    source,
  )
  pumps.push(pump)
  void pump.start(() => {})
  for (let attempt = 0; attempt < 100 && pump.getSnapshot()?.bufferDepth !== events.length; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
  expect(pump.getSnapshot()?.bufferDepth).toBe(events.length)
  return pump
}

async function expectWaiterToResolve(waiter: Promise<void>): Promise<void> {
  const result = await Promise.race([
    waiter.then(() => "resolved" as const),
    new Promise<"timed out">((resolve) => setTimeout(() => resolve("timed out"), 20)),
  ])
  expect(result).toBe("resolved")
}

async function gaugeValue(gauge: typeof metrics.bufferEventCountGauge, eventType: string): Promise<number | undefined> {
  await Promise.resolve()
  const result = await gauge.get()
  return result.values.find(
    (value) =>
      value.labels.tenant === "test-tenant" &&
      value.labels.data_core === "test-data-core" &&
      value.labels.flow_type === "test-flow-type" &&
      value.labels.event_type === eventType,
  )?.value
}

async function pulledBytesValue(eventType: string): Promise<number> {
  const result = await metrics.eventsPulledSizeBytesCounter.get()
  return (
    result.values.find(
      (value) =>
        value.labels.tenant === "test-tenant" &&
        value.labels.data_core === "test-data-core" &&
        value.labels.flow_type === "test-flow-type" &&
        value.labels.event_type === eventType,
    )?.value ?? 0
  )
}

function idsWithoutIncludes(ids: string[]): string[] {
  Object.defineProperty(ids, "includes", {
    value: () => {
      throw new Error("linear membership lookup used")
    },
  })
  return ids
}

function setFailedHandler(pump: FlowcoreDataPump, failedHandler: () => Promise<void>): void {
  const internals = pump as unknown as {
    options: {
      processor?: { handler: (events: FlowcoreEvent[]) => Promise<void>; failedHandler?: () => Promise<void> }
    }
  }
  internals.options.processor = { handler: async () => {}, failedHandler }
}

describe("data pump low-risk buffer bookkeeping", () => {
  it("coalesces repeated gauge publications into one microtask", async () => {
    const pump = await createStartedPump([])
    const internals = pump as unknown as {
      publishMetricsGauges: () => void
      updateMetricsGauges: () => void
    }
    const publish = internals.publishMetricsGauges.bind(pump)
    let publications = 0
    internals.publishMetricsGauges = () => {
      publications++
      publish()
    }

    internals.updateMetricsGauges()
    internals.updateMetricsGauges()
    internals.updateMetricsGauges()
    expect(publications).toBe(0)
    await Promise.resolve()
    expect(publications).toBe(1)
  })

  it("caches payload and event encodings once and keeps pulse/gauge snapshots equivalent through transitions", async () => {
    let serializations = 0
    const payload = {
      value: "payload",
      toJSON() {
        serializations++
        return { value: this.value }
      },
    }
    const event = makeEvent("alpha.0", payload)
    const expectedPayloadBytes = JSON.stringify({ value: "payload" }).length
    const pump = await createStartedPump([event])

    expect(serializations).toBe(1)
    expect(pump.getSnapshot()).toMatchObject({
      bufferDepth: 1,
      bufferReserved: 0,
      bufferSizeBytes: expectedPayloadBytes,
    })
    expect(await gaugeValue(metrics.bufferEventCountGauge, "alpha.0")).toBe(1)
    expect(await gaugeValue(metrics.bufferReservedEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferSizeBytesGauge, "alpha.0")).toBe(expectedPayloadBytes)

    const [reserved] = await pump.reserve(1)
    expect(serializations).toBe(2)
    expect(pump.getSnapshot()).toMatchObject({
      bufferDepth: 1,
      bufferReserved: 1,
      bufferSizeBytes: expectedPayloadBytes,
    })
    expect(await gaugeValue(metrics.bufferReservedEventCountGauge, "alpha.0")).toBe(1)

    await pump.fail(idsWithoutIncludes([reserved.eventId]))
    expect(serializations).toBe(2)
    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, bufferReserved: 0, bufferSizeBytes: 0 })
    expect(await gaugeValue(metrics.bufferEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferReservedEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferSizeBytesGauge, "alpha.0")).toBe(0)
  })

  it("uses Set membership for acknowledgement and removes only matching IDs", async () => {
    const events = [makeEvent("alpha.0", { index: 0 }, 0), makeEvent("beta.0", { index: 1 }, 1)]
    const pump = await createStartedPump(events)
    await pump.reserve(2)

    await pump.acknowledge(idsWithoutIncludes([events[0].eventId]))

    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 1, bufferReserved: 1, acknowledgedTotal: 1 })
    expect(await gaugeValue(metrics.bufferEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferEventCountGauge, "beta.0")).toBe(1)
  })

  it("uses Set membership when reopening and refreshes reserved gauges", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event])
    await pump.reserve(1)
    const deliveryId = (pump as unknown as { buffer: Array<{ deliveryId?: string }> }).buffer[0].deliveryId!

    await (pump as unknown as { reOpen: (ids: string[], deliveryId: string) => Promise<void> }).reOpen(
      idsWithoutIncludes([event.eventId]),
      deliveryId,
    )

    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 1, bufferReserved: 0 })
    expect(await gaugeValue(metrics.bufferReservedEventCountGauge, "alpha.0")).toBe(0)
  })

  it("refreshes gauges when timeout reopening finally fails an event", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 0)
    await pump.reserve(1)
    const deliveryId = (pump as unknown as { buffer: Array<{ deliveryId?: string }> }).buffer[0].deliveryId!

    await (pump as unknown as { reOpen: (ids: string[], deliveryId: string) => Promise<void> }).reOpen(
      idsWithoutIncludes([event.eventId]),
      deliveryId,
    )

    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, bufferReserved: 0, bufferSizeBytes: 0 })
    expect(await gaugeValue(metrics.bufferEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferReservedEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferSizeBytesGauge, "alpha.0")).toBe(0)
  })

  it("counts each terminal timeout failure once when requested IDs contain duplicates", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 0)
    await pump.reserve(1)
    const deliveryId = (pump as unknown as { buffer: Array<{ deliveryId?: string }> }).buffer[0].deliveryId!

    await (pump as unknown as { reOpen: (ids: string[], deliveryId: string) => Promise<void> }).reOpen(
      [event.eventId, event.eventId],
      deliveryId,
    )

    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, failedTotal: 1 })
  })

  it("publishes acknowledgement gauges while checkpoint persistence is delayed", async () => {
    let releaseCheckpoint!: () => void
    const checkpoint = new Promise<void>((resolve) => {
      releaseCheckpoint = resolve
    })
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 3, () => checkpoint)
    await pump.reserve(1)

    const acknowledgement = pump.acknowledge([event.eventId])

    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, bufferReserved: 0 })
    expect(await gaugeValue(metrics.bufferEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferReservedEventCountGauge, "alpha.0")).toBe(0)
    releaseCheckpoint()
    await acknowledgement
  })

  it("publishes failure gauges even when checkpoint persistence rejects", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 3, () => Promise.reject(new Error("checkpoint rejected")))
    await pump.reserve(1)

    await expect(pump.fail([event.eventId])).rejects.toThrow("checkpoint rejected")

    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, bufferReserved: 0 })
    expect(await gaugeValue(metrics.bufferEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferReservedEventCountGauge, "alpha.0")).toBe(0)
    expect(await gaugeValue(metrics.bufferSizeBytesGauge, "alpha.0")).toBe(0)
  })

  it("wakes the buffer-empty waiter when acknowledgement checkpoint persistence rejects", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 3, () => Promise.reject(new Error("checkpoint rejected")))
    await pump.reserve(1)
    const waiter = (pump as unknown as { waitForBufferEmpty: () => Promise<void> }).waitForBufferEmpty()

    await expect(pump.acknowledge([event.eventId])).rejects.toThrow("checkpoint rejected")
    await expectWaiterToResolve(waiter)
  })

  it("wakes the buffer-empty waiter when failure checkpoint persistence rejects", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 3, () => Promise.reject(new Error("checkpoint rejected")))
    await pump.reserve(1)
    const waiter = (pump as unknown as { waitForBufferEmpty: () => Promise<void> }).waitForBufferEmpty()

    await expect(pump.fail([event.eventId])).rejects.toThrow("checkpoint rejected")
    await expectWaiterToResolve(waiter)
  })

  it("awaits and propagates a rejecting failedHandler on direct failure without checkpointing", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const checkpoints: Array<{ timeBucket: string; eventId?: string }> = []
    const pump = await createStartedPump(
      [event],
      3,
      (state) => {
        checkpoints.push(state)
      },
      undefined,
      60_000,
    )
    await pump.reserve(1)
    setFailedHandler(pump, () => Promise.reject(new Error("failed handler rejected")))
    const waiter = (pump as unknown as { waitForBufferEmpty: () => Promise<void> }).waitForBufferEmpty()

    await expect(pump.fail([event.eventId])).rejects.toThrow("failed handler rejected")

    expect(checkpoints).toEqual([])
    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, failedTotal: 1 })
    await expectWaiterToResolve(waiter)
  })

  it("wakes the buffer-empty waiter and propagates terminal reOpen checkpoint rejection", async () => {
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 0, () => Promise.reject(new Error("checkpoint rejected")))
    await pump.reserve(1)
    const internals = pump as unknown as {
      buffer: Array<{ deliveryId?: string }>
      reOpen: (ids: string[], deliveryId: string) => Promise<void>
      waitForBufferEmpty: () => Promise<void>
    }
    const waiter = internals.waitForBufferEmpty()

    await expect(internals.reOpen([event.eventId], internals.buffer[0].deliveryId!)).rejects.toThrow(
      "checkpoint rejected",
    )
    await expectWaiterToResolve(waiter)
  })

  it("logs timer-driven terminal reOpen checkpoint rejection instead of leaving it unhandled", async () => {
    const errors: Array<{ message: string | Error; metadata?: Record<string, unknown> }> = []
    const logger: FlowcoreLogger = {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (message, metadata) => errors.push({ message, metadata }),
    }
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump([event], 0, () => Promise.reject(new Error("checkpoint rejected")), logger, 1)

    await pump.reserve(1)
    for (let attempt = 0; attempt < 100 && !errors.length; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 1))
    }

    expect(errors).toEqual([
      {
        message: "Failed to reopen events after acknowledgement timeout",
        metadata: { error: expect.any(Error) },
      },
    ])
    expect((errors[0].metadata?.error as Error).message).toBe("checkpoint rejected")
  })

  it("awaits and handles a rejecting failedHandler during timer-driven terminal reOpen", async () => {
    const errors: Array<{ message: string | Error; metadata?: Record<string, unknown> }> = []
    const checkpoints: Array<{ timeBucket: string; eventId?: string }> = []
    const logger: FlowcoreLogger = {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (message, metadata) => errors.push({ message, metadata }),
    }
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump(
      [event],
      0,
      (state) => {
        checkpoints.push(state)
      },
      logger,
      1,
    )
    setFailedHandler(pump, () => Promise.reject(new Error("failed handler rejected")))
    const waiter = (pump as unknown as { waitForBufferEmpty: () => Promise<void> }).waitForBufferEmpty()

    await pump.reserve(1)
    for (let attempt = 0; attempt < 100 && !errors.length; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 1))
    }

    expect((errors[0].metadata?.error as Error).message).toBe("failed handler rejected")
    expect(checkpoints).toEqual([])
    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, failedTotal: 1 })
    await expectWaiterToResolve(waiter)
  })

  it("awaits and handles a rejecting onFinalyFailed callback during timer-driven terminal reOpen", async () => {
    const errors: Array<{ message: string | Error; metadata?: Record<string, unknown> }> = []
    const checkpoints: Array<{ timeBucket: string; eventId?: string }> = []
    const logger: FlowcoreLogger = {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (message, metadata) => errors.push({ message, metadata }),
    }
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump(
      [event],
      0,
      (state) => {
        checkpoints.push(state)
      },
      logger,
      1,
    )
    pump.onFinalyFailed(() => Promise.reject(new Error("finally failed handler rejected")))
    const waiter = (pump as unknown as { waitForBufferEmpty: () => Promise<void> }).waitForBufferEmpty()

    await pump.reserve(1)
    for (let attempt = 0; attempt < 100 && !errors.length; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 1))
    }

    expect((errors[0].metadata?.error as Error).message).toBe("finally failed handler rejected")
    expect(checkpoints).toEqual([])
    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, failedTotal: 1 })
    await expectWaiterToResolve(waiter)
  })

  it("delivers both terminal failure callbacks when failedHandler rejects without an unhandled rejection", async () => {
    const errors: Array<{ message: string | Error; metadata?: Record<string, unknown> }> = []
    const checkpoints: Array<{ timeBucket: string; eventId?: string }> = []
    const callbacks: string[] = []
    const logger: FlowcoreLogger = {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (message, metadata) => errors.push({ message, metadata }),
    }
    const event = makeEvent("alpha.0", { index: 0 })
    const pump = await createStartedPump(
      [event],
      0,
      (state) => {
        checkpoints.push(state)
      },
      logger,
      1,
    )
    setFailedHandler(pump, async () => {
      callbacks.push("failedHandler")
      throw new Error("failed handler rejected")
    })
    pump.onFinalyFailed(() => {
      callbacks.push("onFinalyFailed")
    })
    const waiter = (pump as unknown as { waitForBufferEmpty: () => Promise<void> }).waitForBufferEmpty()

    await pump.reserve(1)
    for (let attempt = 0; attempt < 100 && !errors.length; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 1))
    }

    expect(callbacks).toEqual(["failedHandler", "onFinalyFailed"])
    expect((errors[0].metadata?.error as Error).message).toBe("failed handler rejected")
    expect(checkpoints).toEqual([])
    expect(pump.getSnapshot()).toMatchObject({ bufferDepth: 0, failedTotal: 1 })
    await expectWaiterToResolve(waiter)
  })

  it("measures payload and pulled event metrics in UTF-8 bytes", async () => {
    const event = makeEvent("alpha.0", { value: "Halló 👋" })
    const expectedPayloadBytes = new TextEncoder().encode(JSON.stringify(event.payload)).byteLength
    const expectedEventBytes = new TextEncoder().encode(JSON.stringify(event)).byteLength
    const pulledBytesBefore = await pulledBytesValue("alpha.0")
    const pump = await createStartedPump([event])

    expect(pump.getSnapshot()).toMatchObject({ bufferSizeBytes: expectedPayloadBytes })
    expect(await gaugeValue(metrics.bufferSizeBytesGauge, "alpha.0")).toBe(expectedPayloadBytes)

    await pump.reserve(1)

    expect((await pulledBytesValue("alpha.0")) - pulledBytesBefore).toBe(expectedEventBytes)
  })
})
