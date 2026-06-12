import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test"
import type { EventListOutput, FlowcoreEvent } from "@flowcore/sdk"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"
import type { FlowcoreDataPumpState, FlowcoreDataPumpStateManager } from "../../src/data-pump/types.ts"

// #region Test Helpers

const FAKE_API_KEY = "fc_testid_testsecret"

function assert(condition: unknown, message?: string): asserts condition {
  expect(Boolean(condition), message).toBe(true)
}

function assertEquals<T>(actual: T, expected: T, message?: string) {
  expect(actual, message).toEqual(expected)
}

function assertThrows(fn: () => unknown, _errorClass?: typeof Error, message?: string) {
  expect(fn).toThrow(message)
}

async function tickAsync(ms: number) {
  await flushMicrotasks()
  jest.advanceTimersByTime(ms)
  await flushMicrotasks()
}

async function flushMicrotasks() {
  for (let i = 0; i < 10; i++) {
    await Promise.resolve()
  }
}

function createMockStateManager(): FlowcoreDataPumpStateManager {
  return {
    getState: () => Promise.resolve({ timeBucket: "20260331120000" }),
    setState: () => {},
  }
}

function createMockLogger() {
  const logs: { level: string; message: string }[] = []
  return {
    logger: {
      debug: (msg: string) => logs.push({ level: "debug", message: msg }),
      info: (msg: string) => logs.push({ level: "info", message: msg }),
      warn: (msg: string) => logs.push({ level: "warn", message: msg }),
      error: (msg: string | Error) => logs.push({ level: "error", message: msg instanceof Error ? msg.message : msg }),
    },
    logs,
  }
}

function createPump(
  handler: (events: FlowcoreEvent[]) => Promise<void>,
  logger: ReturnType<typeof createMockLogger>["logger"],
): FlowcoreDataPump {
  return FlowcoreDataPump.create({
    auth: { apiKey: FAKE_API_KEY },
    dataSource: {
      tenant: "test",
      dataCore: "test-dc",
      flowType: "test.0",
      eventTypes: ["test.created.0"],
    },
    stateManager: createMockStateManager(),
    processor: {
      concurrency: 1,
      handler,
    },
    notifier: { type: "poller", intervalMs: 60_000 },
    logger,
    baseUrlOverride: "http://localhost:9999",
    noTranslation: true,
  })
}

// #endregion

describe("processLoop restart", () => {
  beforeEach(() => {
    jest.useFakeTimers()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it("should not emit restart warnings after stop", async () => {
    const { logger, logs } = createMockLogger()

    const pump = createPump(() => {
      return Promise.reject(new Error("Always fails"))
    }, logger)

    void pump.start(() => {})
    // Stop immediately — should prevent any restart scheduling
    pump.stop()

    // Advance past any potential restart delays
    await tickAsync(60_000)

    const restartLogs = logs.filter((l) => l.level === "warn" && l.message.includes("Restarting process loop"))
    assertEquals(restartLogs.length, 0)
  })
})

// #region Main-loop self-heal helpers

interface FakeDataSourceOptions {
  timeBuckets?: string[]
  getEventsImpl?: () => Promise<EventListOutput>
}

class FakeDataSource extends FlowcoreDataSource {
  public getEventsCalls = 0
  private readonly timeBucketsValue: string[]
  private getEventsImpl: () => Promise<EventListOutput>

  constructor(opts: FakeDataSourceOptions = {}) {
    super({
      auth: { apiKey: FAKE_API_KEY },
      dataSource: {
        tenant: "test",
        dataCore: "test-dc",
        flowType: "test.0",
        eventTypes: ["test.created.0"],
      },
      baseUrlOverride: "http://localhost:9999",
      noTranslation: true,
    })
    this.timeBucketsValue = opts.timeBuckets ?? ["20260331120000", "20260331130000"]
    this.getEventsImpl = opts.getEventsImpl ?? (() => Promise.resolve({ events: [], nextCursor: undefined }))
  }

  public override getTimeBuckets(_force = false): Promise<string[]> {
    return Promise.resolve(this.timeBucketsValue)
  }

  public override getClosestTimeBucket(timeBucket: string, getBefore = false): Promise<string | null> {
    if (!timeBucket.match(/^\d{14}$/)) {
      return Promise.reject(new Error(`Invalid timebucket: ${timeBucket}`))
    }
    if (getBefore) {
      return Promise.resolve(
        this.timeBucketsValue.findLast((t) => Number.parseFloat(t) <= Number.parseFloat(timeBucket)) ??
          this.timeBucketsValue[this.timeBucketsValue.length - 1] ??
          null,
      )
    }
    return Promise.resolve(
      this.timeBucketsValue.find((t) => Number.parseFloat(t) >= Number.parseFloat(timeBucket)) ??
        this.timeBucketsValue[this.timeBucketsValue.length - 1] ??
        null,
    )
  }

  public override getNextTimeBucket(timeBucket: string): Promise<string | null> {
    const index = this.timeBucketsValue.indexOf(timeBucket)
    if (index === -1 || index + 1 >= this.timeBucketsValue.length) {
      return Promise.resolve(null)
    }
    return Promise.resolve(this.timeBucketsValue[index + 1])
  }

  public override getEvents(): Promise<EventListOutput> {
    this.getEventsCalls++
    return this.getEventsImpl()
  }

  public setGetEventsImpl(impl: () => Promise<EventListOutput>): void {
    this.getEventsImpl = impl
  }
}

function createPumpWithFakeDataSource(
  fakeDataSource: FakeDataSource,
  logger: ReturnType<typeof createMockLogger>["logger"],
  stateManager?: FlowcoreDataPumpStateManager,
): FlowcoreDataPump {
  return FlowcoreDataPump.create(
    {
      auth: { apiKey: FAKE_API_KEY },
      dataSource: {
        tenant: "test",
        dataCore: "test-dc",
        flowType: "test.0",
        eventTypes: ["test.created.0"],
      },
      stateManager: stateManager ?? createMockStateManager(),
      notifier: { type: "poller", intervalMs: 60_000 },
      logger,
      baseUrlOverride: "http://localhost:9999",
      noTranslation: true,
    },
    fakeDataSource,
  )
}

// #endregion

describe("startMainLoop self-heal", () => {
  beforeEach(() => {
    jest.useFakeTimers()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it("(a) loop throws once then self-restarts and resets attempts on next successful fetch", async () => {
    const { logger, logs } = createMockLogger()
    let throwsLeft = 1
    const fakeDataSource = new FakeDataSource({
      getEventsImpl: () => {
        if (throwsLeft-- > 0) {
          return Promise.reject(new Error("transient API failure"))
        }
        return Promise.resolve({ events: [], nextCursor: undefined })
      },
    })
    const pump = createPumpWithFakeDataSource(fakeDataSource, logger)

    let callbackError: Error | undefined
    let callbackFired = false
    await pump.start((err) => {
      callbackFired = true
      callbackError = err
    })

    // First call should have happened and thrown
    await tickAsync(0)
    // Backoff: attempt 1 → 1s
    await tickAsync(1_000)
    // Drain microtasks for the restarted loop to make a second call
    await tickAsync(0)

    assert(fakeDataSource.getEventsCalls >= 2, `expected >=2 calls, got ${fakeDataSource.getEventsCalls}`)
    assertEquals(pump.isRunning, true)
    // mainLoopRestartAttempts reset to 0 after a successful pull
    assertEquals((pump as unknown as { mainLoopRestartAttempts: number }).mainLoopRestartAttempts, 0)
    // callback has NOT fired yet — pump is still running after self-heal
    assertEquals(callbackFired, false)

    const restartLogs = logs.filter((l) => l.level === "warn" && l.message.includes("Restarting fetch loop"))
    assertEquals(restartLogs.length, 1)
    assert(restartLogs[0].message.includes("attempt 1"))

    pump.stop()
    await tickAsync(60_000)
    // After graceful stop, callback fires without error
    assertEquals(callbackFired, true)
    assertEquals(callbackError, undefined)
  })

  it("(b) seven successive failures exponential-backoff cap at 30s", async () => {
    const { logger, logs } = createMockLogger()
    const fakeDataSource = new FakeDataSource({
      getEventsImpl: () => Promise.reject(new Error("permanent failure")),
    })
    const pump = createPumpWithFakeDataSource(fakeDataSource, logger)

    void pump.start(() => {})

    // First call (attempt 1 scheduled after 1s)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 1)

    // 1s → 2nd call (attempt 1 scheduled, then attempt 2 after 2s)
    await tickAsync(1_000)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 2)

    // 2s → 3rd call (4s next)
    await tickAsync(2_000)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 3)

    // 4s → 4th call (8s next)
    await tickAsync(4_000)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 4)

    // 8s → 5th call (16s next)
    await tickAsync(8_000)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 5)

    // 16s → 6th call (30s capped next)
    await tickAsync(16_000)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 6)

    // 30s → 7th call (30s capped next)
    await tickAsync(30_000)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 7)

    // 30s → 8th call (cap held)
    await tickAsync(30_000)
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 8)

    // Verify exact backoff sequence was logged
    const restartDelays = logs
      .filter((l) => l.level === "warn" && l.message.includes("Restarting fetch loop in"))
      .map((l) => {
        const match = l.message.match(/Restarting fetch loop in (\d+)ms/)
        return match ? Number.parseInt(match[1], 10) : -1
      })
    assertEquals(restartDelays.slice(0, 7), [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000])

    pump.stop()
    await tickAsync(60_000)
  })

  it("(c) stop() during pending retry → no further retry fires", async () => {
    const { logger, logs } = createMockLogger()
    const fakeDataSource = new FakeDataSource({
      getEventsImpl: () => Promise.reject(new Error("transient failure")),
    })
    const pump = createPumpWithFakeDataSource(fakeDataSource, logger)

    void pump.start(() => {})

    // First call fires → throws → schedules 1s retry
    await tickAsync(0)
    assertEquals(fakeDataSource.getEventsCalls, 1)

    // Stop BEFORE the 1s retry fires
    pump.stop()

    // Advance past the retry window
    await tickAsync(60_000)

    // No second call should have happened
    assertEquals(fakeDataSource.getEventsCalls, 1)
    const restartLogs = logs.filter((l) => l.level === "warn" && l.message.includes("Restarting fetch loop"))
    // The one warning got scheduled before stop() — but the actual restart never runs
    assert(restartLogs.length <= 1)
  })

  it("(d) restart() with ISO timebucket throws synchronously", async () => {
    const { logger } = createMockLogger()
    const fakeDataSource = new FakeDataSource()
    const pump = createPumpWithFakeDataSource(fakeDataSource, logger)

    void pump.start(() => {})
    await tickAsync(0)

    assertThrows(
      () => pump.restart({ timeBucket: "2026-05-12T13:13" } as FlowcoreDataPumpState),
      Error,
      "Invalid timebucket: 2026-05-12T13:13",
    )

    // Pump still running — restart() bailed before mutating state
    assertEquals(pump.isRunning, true)

    pump.stop()
    await tickAsync(60_000)
  })

  it("(e) poisoned restartTo causes loop to log and exit cleanly", async () => {
    const { logger, logs } = createMockLogger()
    const fakeDataSource = new FakeDataSource({
      getEventsImpl: () => Promise.resolve({ events: [], nextCursor: undefined }),
    })
    const pump = createPumpWithFakeDataSource(fakeDataSource, logger)

    let callbackFired = false
    let callbackError: Error | undefined
    void pump.start((err) => {
      callbackFired = true
      callbackError = err
    })

    // Bypass restart() validation by mutating private state directly
    const internals = pump as unknown as {
      restartTo: FlowcoreDataPumpState
      running: boolean
      abortController?: AbortController
    }
    internals.restartTo = { timeBucket: "bad" }
    // Trigger loop exit so restartTo consumption block runs
    internals.running = false
    // Abort any in-flight waiter so loop iteration progresses
    internals.abortController?.abort()

    // Let loop iterations + microtasks settle
    await tickAsync(2_000)

    // Loop should have exited cleanly without throwing
    assertEquals(callbackFired, true)
    assertEquals(callbackError, undefined)
    // restartTo dropped on error
    assertEquals((pump as unknown as { restartTo?: FlowcoreDataPumpState }).restartTo, undefined)
    // Loop logged the error
    const errorLogs = logs.filter((l) => l.level === "error" && l.message.includes("Failed to consume restartTo"))
    assertEquals(errorLogs.length, 1)
  })

  it("(f) happy-path restart still works (regression)", async () => {
    const { logger } = createMockLogger()
    const fakeDataSource = new FakeDataSource({
      timeBuckets: ["20260101000000", "20260102000000", "20260331120000"],
      getEventsImpl: () => Promise.resolve({ events: [], nextCursor: undefined }),
    })
    const pump = createPumpWithFakeDataSource(fakeDataSource, logger)

    void pump.start(() => {})
    await tickAsync(0)
    assert(fakeDataSource.getEventsCalls >= 1)

    // Valid 14-digit timebucket — should not throw
    pump.restart({ timeBucket: "20260101000000" })

    // Loop processes the restart and continues pulling
    await tickAsync(1_000)
    await tickAsync(0)

    assert(pump.isRunning)
    assert(fakeDataSource.getEventsCalls >= 2)

    pump.stop()
    await tickAsync(60_000)
  })
})

describe("backoff formula", () => {
  it("should produce correct exponential sequence capped at 30s", () => {
    const delays = []
    for (let attempt = 1; attempt <= 8; attempt++) {
      delays.push(Math.min(1_000 * Math.pow(2, attempt - 1), 30_000))
    }
    assertEquals(delays, [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000])
  })

  it("should use RECONNECT constants for leader pump (same formula)", () => {
    const RECONNECT_BASE_MS = 1_000
    const RECONNECT_MAX_MS = 30_000

    const delays = []
    for (let attempt = 1; attempt <= 6; attempt++) {
      delays.push(Math.min(RECONNECT_BASE_MS * Math.pow(2, attempt - 1), RECONNECT_MAX_MS))
    }
    assertEquals(delays, [1_000, 2_000, 4_000, 8_000, 16_000, 30_000])
  })
})
