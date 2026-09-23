import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test"
import type { EventListOutput, FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"
import type { FlowcoreDataPumpState, FlowcoreDataPumpStateManager } from "../../src/data-pump/types.ts"

// #region Helpers

const FAKE_API_KEY = "fc_testid_testsecret"
const BUCKET = "20260331120000"

async function flushMicrotasks() {
  for (let i = 0; i < 20; i++) {
    await Promise.resolve()
  }
}

async function tickAsync(ms: number) {
  await flushMicrotasks()
  jest.advanceTimersByTime(ms)
  await flushMicrotasks()
}

async function waitUntil(condition: () => boolean, message: string) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (condition()) return
    await tickAsync(1)
  }
  throw new Error(message)
}

function makeEvent(index: number): FlowcoreEvent {
  return {
    eventId: TimeUuid.fromDate(new Date(Date.UTC(2026, 2, 31, 12, 0, index))).toString(),
    timeBucket: BUCKET,
    tenant: "test",
    dataCoreId: "test-dc",
    flowType: "test.0",
    eventType: "test.created.0",
    payload: { index },
    validTime: new Date().toISOString(),
  } as unknown as FlowcoreEvent
}

class RecordingStateManager implements FlowcoreDataPumpStateManager {
  public states: FlowcoreDataPumpState[] = []
  getState(): Promise<FlowcoreDataPumpState> {
    return Promise.resolve({ timeBucket: BUCKET })
  }
  setState(state: FlowcoreDataPumpState): void {
    this.states.push(state)
  }
  get last(): FlowcoreDataPumpState | undefined {
    return this.states[this.states.length - 1]
  }
}

class FakeDataSource extends FlowcoreDataSource {
  private pending: FlowcoreEvent[] = []
  constructor(initial: FlowcoreEvent[] = []) {
    super({
      auth: { apiKey: FAKE_API_KEY },
      dataSource: { tenant: "test", dataCore: "test-dc", flowType: "test.0", eventTypes: ["test.created.0"] },
      baseUrlOverride: "http://localhost:9999",
      noTranslation: true,
    })
    this.pending = [...initial]
  }

  /** Make one more batch available to the next fetch. */
  public enqueue(events: FlowcoreEvent[]): void {
    this.pending.push(...events)
  }
  public override getTimeBuckets(): Promise<string[]> {
    return Promise.resolve([BUCKET])
  }
  public override getClosestTimeBucket(): Promise<string | null> {
    return Promise.resolve(BUCKET)
  }
  public override getNextTimeBucket(): Promise<string | null> {
    return Promise.resolve(null)
  }
  public override getEvents(): Promise<EventListOutput> {
    const events = this.pending
    this.pending = []
    return Promise.resolve({ events, nextCursor: undefined } as unknown as EventListOutput)
  }
}

function createPump(
  source: FakeDataSource,
  handler: (events: FlowcoreEvent[]) => Promise<void>,
  stateManager: FlowcoreDataPumpStateManager,
): FlowcoreDataPump {
  return FlowcoreDataPump.create(
    {
      auth: { apiKey: FAKE_API_KEY },
      dataSource: { tenant: "test", dataCore: "test-dc", flowType: "test.0", eventTypes: ["test.created.0"] },
      stateManager,
      processor: { concurrency: 1, handler },
      notifier: { type: "poller", intervalMs: 60_000 },
      baseUrlOverride: "http://localhost:9999",
      noTranslation: true,
    },
    source,
  )
}

/** A handler that blocks on its first batch until released, so a pause lands mid-flight. */
function gatedHandler(delivered: number[]) {
  let entered!: () => void
  const firstBatch = new Promise<void>((resolve) => {
    entered = resolve
  })
  let release!: () => void
  const gate = new Promise<void>((resolve) => {
    release = resolve
  })
  let batches = 0
  const handler = async (events: FlowcoreEvent[]) => {
    batches++
    for (const event of events) delivered.push((event.payload as { index: number }).index)
    if (batches === 1) {
      entered()
      await gate
    }
  }
  return { handler, firstBatch, release: () => release() }
}

// #endregion

describe("data pump pause / resume", () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  it("stops delivery on pause and resumes from the same position", async () => {
    const delivered: number[] = []
    const state = new RecordingStateManager()
    const source = new FakeDataSource([makeEvent(0), makeEvent(1), makeEvent(2)])
    const { handler, firstBatch, release } = gatedHandler(delivered)
    const pump = createPump(source, handler, state)

    void pump.start(() => {})
    await firstBatch
    // The pause lands while the first batch is still inside the handler.
    pump.pause()
    release()
    await tickAsync(60_000)

    expect(pump.isPaused).toBe(true)
    expect(delivered).toEqual([0])

    pump.resume()
    await waitUntil(() => delivered.length === 3, "resume did not deliver the retained buffer")

    expect(pump.isPaused).toBe(false)
    expect(delivered).toEqual([0, 1, 2])
    pump.stop()
  })

  it("keeps fetching, buffering and pulsing while paused", async () => {
    const delivered: number[] = []
    const state = new RecordingStateManager()
    const source = new FakeDataSource([makeEvent(0), makeEvent(1)])
    const { handler, firstBatch, release } = gatedHandler(delivered)
    const pump = createPump(source, handler, state)

    // Count pulse emitter stops. `pause()` must never stop it — that is the exact
    // shape of the April 2026 outage where a pump looked alive and delivered nothing.
    const internals = pump as unknown as { pulseEmitter?: { start(): void; stop(): void } }
    let pulseStops = 0
    let pulseStarts = 0
    internals.pulseEmitter = {
      start: () => {
        pulseStarts++
      },
      stop: () => {
        pulseStops++
      },
    }

    void pump.start(() => {})
    await firstBatch
    pump.pause()
    release()
    await tickAsync(60_000)

    const bufferedAtPause = pump.getSnapshot()!.bufferDepth

    // The fetch loop must still be alive: events that appear AFTER the pause must
    // still be pulled into the buffer. Without this the test would pass even if
    // pause() killed the fetch loop.
    source.enqueue([makeEvent(2), makeEvent(3)])
    await tickAsync(60_000)

    const snapshot = pump.getSnapshot()
    expect(snapshot).not.toBeNull()
    expect(snapshot?.paused).toBe(true)
    expect(pump.isRunning).toBe(true)
    expect(snapshot!.bufferDepth).toBe(bufferedAtPause + 2)
    // Nothing was delivered while paused, despite the new events.
    expect(delivered).toEqual([0])
    // The emitter was started once by start(), and pause() never stopped it.
    expect(pulseStarts).toBe(1)
    expect(pulseStops).toBe(0)
    pump.stop()
  })

  it("acknowledges the in-flight batch so the checkpoint stays accurate", async () => {
    const state = new RecordingStateManager()
    const source = new FakeDataSource([makeEvent(0), makeEvent(1), makeEvent(2)])
    let inHandler!: () => void
    const entered = new Promise<void>((resolve) => {
      inHandler = resolve
    })
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    let delivered = 0

    const pump = createPump(
      source,
      async () => {
        delivered++
        if (delivered === 1) {
          inHandler()
          await gate
        }
      },
      state,
    )

    void pump.start(() => {})
    await entered
    // Pause lands while the first batch is still inside the handler.
    pump.pause()
    release()
    await tickAsync(1_000)

    // The in-flight batch still checkpointed, and nothing after it was delivered.
    expect(state.states.length).toBeGreaterThan(0)
    expect(delivered).toBe(1)
    pump.stop()
  })

  it("stays paused across a restart and resumes at the repositioned cursor", async () => {
    const delivered: number[] = []
    const state = new RecordingStateManager()
    const source = new FakeDataSource([makeEvent(0)])
    const { handler, firstBatch, release } = gatedHandler(delivered)
    const pump = createPump(source, handler, state)

    void pump.start(() => {})
    await firstBatch
    pump.pause()
    release()
    await tickAsync(1_000)

    pump.restart({ timeBucket: BUCKET })
    source.enqueue([makeEvent(1), makeEvent(2)])
    await tickAsync(60_000)

    // A reposition does not clear the pause, and the refetched events wait in the buffer.
    expect(pump.isPaused).toBe(true)
    expect(delivered).toEqual([0])
    expect(pump.getSnapshot()?.bufferDepth).toBe(2)

    pump.resume()
    await waitUntil(() => delivered.length > 1, "resume after restart delivered nothing")
    pump.stop()
  })

  it("is idempotent: a double pause still needs only one resume to deliver", async () => {
    const delivered: number[] = []
    const state = new RecordingStateManager()
    const source = new FakeDataSource([makeEvent(0), makeEvent(1), makeEvent(2)])
    const { handler, firstBatch, release } = gatedHandler(delivered)
    const pump = createPump(source, handler, state)

    void pump.start(() => {})
    await firstBatch
    pump.pause()
    pump.pause()
    release()
    await tickAsync(60_000)
    expect(delivered).toEqual([0])

    pump.resume()
    pump.resume()
    await waitUntil(() => delivered.length === 3, "a double pause was not cleared by one resume")
    expect(pump.isPaused).toBe(false)
    pump.stop()
  })

  it("can be constructed already paused so no event escapes before the pause applies", async () => {
    const delivered: number[] = []
    const state = new RecordingStateManager()
    const source = new FakeDataSource([makeEvent(0), makeEvent(1), makeEvent(2)])
    const pump = FlowcoreDataPump.create(
      {
        auth: { apiKey: FAKE_API_KEY },
        dataSource: { tenant: "test", dataCore: "test-dc", flowType: "test.0", eventTypes: ["test.created.0"] },
        stateManager: state,
        processor: {
          concurrency: 1,
          handler: async (events: FlowcoreEvent[]) => {
            for (const event of events) delivered.push((event.payload as { index: number }).index)
          },
        },
        notifier: { type: "poller", intervalMs: 60_000 },
        paused: true,
        baseUrlOverride: "http://localhost:9999",
        noTranslation: true,
      },
      source,
    )

    void pump.start(() => {})
    await tickAsync(60_000)

    // Restoring a durable pause must not leak a single delivery on the way up.
    expect(pump.isPaused).toBe(true)
    expect(delivered).toEqual([])
    expect(pump.getSnapshot()!.bufferDepth).toBe(3)

    pump.resume()
    await waitUntil(() => delivered.length === 3, "resume did not start delivery")
    expect(delivered).toEqual([0, 1, 2])
    pump.stop()
  })

  it("refuses to pause a pump with no processor, so the pulse never claims a false pause", async () => {
    const state = new RecordingStateManager()
    const warnings: string[] = []
    const pump = FlowcoreDataPump.create(
      {
        auth: { apiKey: FAKE_API_KEY },
        dataSource: { tenant: "test", dataCore: "test-dc", flowType: "test.0", eventTypes: ["test.created.0"] },
        stateManager: state,
        notifier: { type: "poller", intervalMs: 60_000 },
        logger: {
          debug: () => {},
          info: () => {},
          warn: (message: string) => warnings.push(message),
          error: () => {},
        },
        baseUrlOverride: "http://localhost:9999",
        noTranslation: true,
      },
      new FakeDataSource([makeEvent(0), makeEvent(1)]),
    )

    void pump.start(() => {})
    await tickAsync(1_000)
    pump.pause()

    // A puller-mode pump keeps handing out events through reserve(), so claiming
    // `paused: true` on the pulse would be a lie to the control plane.
    expect(pump.isPaused).toBe(false)
    expect(pump.getSnapshot()?.paused).toBe(false)
    expect(warnings.some((w) => w.includes("no processor"))).toBe(true)
    pump.stop()
  })

  it("resume clears an armed process-loop backoff instead of staying dark", async () => {
    const delivered: number[] = []
    const state = new RecordingStateManager()
    const source = new FakeDataSource([makeEvent(0), makeEvent(1), makeEvent(2)])
    const { handler, firstBatch, release } = gatedHandler(delivered)
    const pump = createPump(source, handler, state)

    const internals = pump as unknown as {
      processLoopBackoffGeneration?: number
      processLoopRestartTimer?: ReturnType<typeof setTimeout>
      processLoopGeneration: number
    }

    void pump.start(() => {})
    await firstBatch
    pump.pause()
    release()
    await tickAsync(1_000)
    expect(delivered).toEqual([0])

    // A pump that was failing before the operator paused it leaves a backoff armed.
    // Without the clear in resume(), delivery stays dark for up to 30s after resume —
    // the exact symptom operators are trained to read as a wedged pump.
    internals.processLoopBackoffGeneration = internals.processLoopGeneration
    internals.processLoopRestartTimer = setTimeout(() => {}, 30_000)

    pump.resume()

    expect(internals.processLoopBackoffGeneration).toBeUndefined()
    expect(internals.processLoopRestartTimer).toBeUndefined()
    await waitUntil(() => delivered.length === 3, "resume did not clear the backoff, delivery stayed dark")
    pump.stop()
  })
})

/** Emits a fixed, recorded set of events so assertions can reference the exact ids. */
class RecordedSource extends FlowcoreDataSource {
  public emitted: FlowcoreEvent[] = []
  private counter = 0
  constructor() {
    super({
      auth: { apiKey: FAKE_API_KEY },
      dataSource: { tenant: "test", dataCore: "test-dc", flowType: "test.0", eventTypes: ["test.created.0"] },
      baseUrlOverride: "http://localhost:9999",
      noTranslation: true,
    })
  }
  override getTimeBuckets() {
    return Promise.resolve([BUCKET])
  }
  override getClosestTimeBucket() {
    return Promise.resolve(BUCKET)
  }
  override getNextTimeBucket() {
    return Promise.resolve(null)
  }
  override getEvents(_s: unknown, amount: number): Promise<EventListOutput> {
    const events: FlowcoreEvent[] = []
    for (let i = 0; i < amount; i++) {
      const index = this.counter++
      const event = {
        eventId: TimeUuid.fromDate(new Date(Date.UTC(2026, 2, 31, 12, 0, index))).toString(),
        timeBucket: BUCKET,
        tenant: "test",
        dataCoreId: "test-dc",
        flowType: "test.0",
        eventType: "test.created.0",
        payload: { index },
        validTime: new Date().toISOString(),
      } as unknown as FlowcoreEvent
      events.push(event)
      this.emitted.push(event)
    }
    return Promise.resolve({ events, nextCursor: undefined } as unknown as EventListOutput)
  }
}

describe("pause checkpoint invariant", () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  it("never checkpoints past an un-delivered event", async () => {
    const delivered: string[] = []
    const state = new RecordingStateManager()
    const source = new RecordedSource()
    let entered!: () => void
    const first = new Promise<void>((r) => {
      entered = r
    })
    let release!: () => void
    const gate = new Promise<void>((r) => {
      release = r
    })
    let batches = 0
    const pump = FlowcoreDataPump.create(
      {
        auth: { apiKey: FAKE_API_KEY },
        dataSource: { tenant: "test", dataCore: "test-dc", flowType: "test.0", eventTypes: ["test.created.0"] },
        stateManager: state,
        bufferSize: 8,
        processor: {
          concurrency: 1,
          handler: async (evs: FlowcoreEvent[]) => {
            batches++
            for (const e of evs) delivered.push(e.eventId)
            if (batches === 1) {
              entered()
              await gate
            }
          },
        },
        notifier: { type: "poller", intervalMs: 60_000 },
        baseUrlOverride: "http://localhost:9999",
        noTranslation: true,
      },
      source,
    )

    void pump.start(() => {})
    await first
    pump.pause()
    release()
    await tickAsync(120_000)

    expect(delivered.length).toBe(1)
    expect(delivered[0]).toBe(source.emitted[0]!.eventId)

    // The checkpoint is the last delivered event. The source resumes exclusively
    // after it, so the first un-delivered event remains eligible after a restart.
    const checkpoint = state.last!.eventId!
    const checkpointIndex = source.emitted.findIndex((e) => e.eventId === checkpoint)
    expect(checkpointIndex).toBe(0)

    // Resuming the in-memory processor continues at the next event with no gap or duplicate.
    pump.resume()
    for (let i = 0; i < 300 && delivered.length < 10; i++) await tickAsync(1)
    const expected = source.emitted.slice(0, delivered.length).map((e) => e.eventId)
    expect(delivered).toEqual(expected)
    pump.stop()
  })
})
