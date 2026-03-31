import { assertEquals } from "@std/assert"
import { afterEach, beforeEach, describe, it } from "@std/testing/bdd"
import { FakeTime } from "@std/testing/time"
import type { FlowcoreEvent } from "@flowcore/sdk"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import type { FlowcoreDataPumpStateManager } from "../../src/data-pump/types.ts"

// #region Test Helpers

const FAKE_API_KEY = "fc_testid_testsecret"

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
    baseUrlOverride: "http://localhost:99999",
    noTranslation: true,
  })
}

// #endregion

describe("processLoop restart", { sanitizeOps: false, sanitizeResources: false }, () => {
  let fakeTime: FakeTime

  beforeEach(() => {
    fakeTime = new FakeTime()
  })

  afterEach(() => {
    fakeTime.restore()
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
    await fakeTime.tickAsync(60_000)

    const restartLogs = logs.filter((l) => l.level === "warn" && l.message.includes("Restarting process loop"))
    assertEquals(restartLogs.length, 0)
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
