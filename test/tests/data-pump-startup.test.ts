import { afterEach, describe, expect, it, mock } from "bun:test"
import { TimeUuid } from "@flowcore/time-uuid"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import type { FlowcoreDataPumpState, FlowcoreDataPumpStateManager } from "../../src/data-pump/types.ts"

const state: FlowcoreDataPumpState = {
  timeBucket: "20260101000000",
  eventId: TimeUuid.fromDate(new Date("2026-01-01T00:00:01Z")).toString(),
}
const stopAt = new Date("2026-01-02T00:00:00Z")
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: Error) => void
  const promise = new Promise<T>((yes, no) => {
    resolve = yes
    reject = no
  })
  return { promise, resolve, reject }
}
async function flush() {
  for (let i = 0; i < 10; i++) await Promise.resolve()
}
const pumps: FlowcoreDataPump[] = []
afterEach(() => {
  for (const pump of pumps.splice(0)) pump.stop()
})

function fixture(strict?: boolean, end?: Date) {
  // Intersection keeps these behavioral regressions executable against the old API for RED.
  const manager: FlowcoreDataPumpStateManager & { requireExactResumeBucket?: boolean } = {
    requireExactResumeBucket: strict,
    getState: mock(() => Promise.resolve<FlowcoreDataPumpState | null>({ ...state })),
    setState: mock(() => {}),
  }
  const pump = FlowcoreDataPump.create({
    auth: { apiKey: "synthetic-test-key", apiKeyId: "synthetic-id" },
    dataSource: { tenant: "test", dataCore: "test", flowType: "test.0", eventTypes: ["test.created.0"] },
    stateManager: manager,
    processor: { handler: async () => {} },
    notifier: { type: "poller", intervalMs: 60_000 },
    noTranslation: true,
    stopAt: end,
  })
  pumps.push(pump)
  // Isolate startup from the already-covered fetch/process loops, and observe every activation boundary.
  const inner = pump as unknown as {
    bufferState: FlowcoreDataPumpState
    stopAtState?: FlowcoreDataPumpState
    nextCursor?: string
    isLive: boolean
    startedAt: number
    updateMetricsGauges: () => void
    pulseEmitter: { start: () => void; stop: () => void }
    startProcessLoop: () => void
    loop: () => Promise<void>
  }
  inner.nextCursor = "previous-cursor"
  inner.isLive = true
  const metrics = mock(() => {})
  const pulse = mock(() => {})
  const process = mock(() => {})
  const loop = mock(() => Promise.resolve())
  inner.updateMetricsGauges = metrics
  inner.pulseEmitter = { start: pulse, stop: () => {} }
  inner.startProcessLoop = process
  inner.loop = loop
  const bucket = mock((_bucket: string, _before?: boolean) => Promise.resolve<string | null>(state.timeBucket))
  pump.dataSource.getClosestTimeBucket = bucket
  const callback = mock((_error?: Error) => {})
  function snapshot() {
    return {
      buffer: { ...inner.bufferState },
      stop: inner.stopAtState,
      cursor: inner.nextCursor,
      live: inner.isLive,
      started: inner.startedAt,
      running: pump.isRunning,
      metrics: metrics.mock.calls.length,
      pulse: pulse.mock.calls.length,
      process: process.mock.calls.length,
      loop: loop.mock.calls.length,
      callback: callback.mock.calls.length,
    }
  }
  return { pump, manager, inner, metrics, pulse, process, loop, bucket, callback, snapshot }
}

describe("exact resume startup", () => {
  for (const replacement of [null, "20251231230000", "20260101010000"]) {
    it(`rejects an unavailable or changed strict bucket (${replacement}) before activation`, async () => {
      const f = fixture(true)
      f.bucket.mockImplementation(() => Promise.resolve(replacement))
      const before = f.snapshot()
      await expect(f.pump.start(f.callback)).rejects.toThrow("Required resume bucket is unavailable")
      expect(f.snapshot()).toEqual(before)
      expect(f.manager.setState).not.toHaveBeenCalled()
      f.bucket.mockImplementation(() => Promise.resolve(state.timeBucket))
      await f.pump.start(f.callback)
      expect(f.pump.isRunning).toBe(true)
    })
  }
  it("requires a saved state in strict mode", async () => {
    const f = fixture(true)
    f.manager.getState = () => null
    const before = f.snapshot()
    await expect(f.pump.start(f.callback)).rejects.toThrow("Required resume bucket state is absent")
    expect(f.snapshot()).toEqual(before)
    expect(f.bucket).not.toHaveBeenCalled()
  })
  it("resumes the exact saved bucket and exclusive event cursor", async () => {
    const f = fixture(true, stopAt)
    await f.pump.start(f.callback)
    expect(f.inner.bufferState).toEqual(state)
    expect(f.inner.nextCursor).toBeUndefined()
    expect(TimeUuid.fromString(f.inner.stopAtState!.eventId!).getDate()).toEqual(stopAt)
    expect(f.bucket.mock.calls).toEqual([[state.timeBucket], ["20260102000000", true]])
    expect(f.pulse).toHaveBeenCalledTimes(1)
    expect(f.process).toHaveBeenCalledTimes(1)
    expect(f.loop).toHaveBeenCalledTimes(1)
    expect(f.manager.setState).not.toHaveBeenCalled()
  })
  for (const strict of [false, undefined]) {
    for (const replacement of [null, "20251231230000", "20260101010000"]) {
      it(`preserves legacy fallback with strict=${strict}, bucket=${replacement}`, async () => {
        const f = fixture(strict)
        f.bucket.mockImplementation(() => Promise.resolve(replacement))
        await f.pump.start(f.callback)
        expect(f.pump.isRunning).toBe(true)
        expect(f.inner.bufferState.eventId).toBe(state.eventId)
        if (replacement) expect(f.inner.bufferState.timeBucket).toBe(replacement)
        else expect(f.inner.bufferState.timeBucket).toMatch(/^\d{10}0000$/)
      })
    }
    it(`preserves fresh startup without state, strict=${strict}`, async () => {
      const f = fixture(strict)
      f.manager.getState = () => null
      await f.pump.start(f.callback)
      expect(f.pump.isRunning).toBe(true)
      expect(f.inner.bufferState.eventId).toBeDefined()
      expect(f.bucket).not.toHaveBeenCalled()
    })
  }
})

type Stage = "state" | "bucket" | "stopAt"
function hold(f: ReturnType<typeof fixture>, stage: Stage) {
  const gate = deferred<FlowcoreDataPumpState | string | null>()
  if (stage === "state") f.manager.getState = () => gate.promise as Promise<FlowcoreDataPumpState | null>
  else
    f.bucket.mockImplementation((_bucket, before) =>
      stage === "bucket" || before ? (gate.promise as Promise<string | null>) : Promise.resolve(state.timeBucket),
    )
  return { ...gate, finish: () => gate.resolve(stage === "state" ? { ...state } : state.timeBucket) }
}

describe("startup ownership", () => {
  for (const stage of ["state", "bucket", "stopAt"] as const) {
    it(`leaves all startup state inactive when ${stage} lookup rejects and permits retry`, async () => {
      const f = fixture(true, stopAt)
      const gate = hold(f, stage)
      const before = f.snapshot()
      const result = f.pump.start(f.callback).catch((error) => error)
      await flush()
      expect(f.snapshot()).toEqual(before)
      const error = new Error("lookup failed")
      gate.reject(error)
      expect(await result).toBe(error)
      expect(f.snapshot()).toEqual(before)
      f.manager.getState = () => ({ ...state })
      f.bucket.mockImplementation(() => Promise.resolve(state.timeBucket))
      await f.pump.start(f.callback)
      expect(f.pump.isRunning).toBe(true)
    })
    it(`stop cancels pending ${stage} startup without late effects`, async () => {
      const f = fixture(true, stopAt)
      const gate = hold(f, stage)
      const startup = f.pump.start(f.callback)
      await flush()
      f.pump.stop()
      const stopped = f.snapshot()
      const bucketCalls = f.bucket.mock.calls.length
      gate.finish()
      await startup
      expect(f.snapshot()).toEqual(stopped)
      expect(f.bucket.mock.calls.length).toBe(bucketCalls)
      expect(f.manager.setState).not.toHaveBeenCalled()
    })
    for (const reject of [false, true]) {
      for (const newerActive of [false, true]) {
        it(`stale ${stage} ${reject ? "rejection" : "completion"} cannot disturb a newer ${newerActive ? "active" : "pending"} start`, async () => {
          const f = fixture(true, stopAt)
          const old = hold(f, stage)
          const oldResult = f.pump.start(f.callback).catch((error) => error)
          await flush()
          f.pump.stop()
          const newer = deferred<FlowcoreDataPumpState | null>()
          f.manager.getState = () => newer.promise
          f.bucket.mockImplementation(() => Promise.resolve(state.timeBucket))
          const newStart = f.pump.start(f.callback)
          if (newerActive) {
            newer.resolve({ ...state })
            await newStart
            await flush()
          }
          const before = f.snapshot()
          const error = new Error("stale lookup failed")
          if (reject) old.reject(error)
          else old.finish()
          expect(await oldResult).toBe(reject ? error : undefined)
          expect(f.snapshot()).toEqual(before)
          await expect(f.pump.start(f.callback)).rejects.toThrow("already running")
          if (!newerActive) {
            newer.resolve({ ...state })
            await newStart
          }
          expect(f.pulse).toHaveBeenCalledTimes(1)
          expect(f.loop).toHaveBeenCalledTimes(1)
        })
      }
    }
  }
  it("reserves startup synchronously, rejects overlaps without resetting live state", async () => {
    const f = fixture()
    const gate = hold(f, "state")
    const first = f.pump.start(f.callback)
    await expect(f.pump.start(f.callback)).rejects.toThrow("already running")
    expect(f.pump.isRunning).toBe(false)
    gate.finish()
    await first
    f.inner.isLive = true
    const before = f.snapshot()
    await expect(f.pump.start(f.callback)).rejects.toThrow("already running")
    expect(f.snapshot()).toEqual(before)
  })
  it("preserves awaited-loop and callback completion semantics", async () => {
    for (const withCallback of [false, true]) {
      const f = fixture()
      const loop = deferred<void>()
      f.inner.loop = () => loop.promise
      let settled = false
      const start = f.pump.start(withCallback ? f.callback : undefined).then(() => {
        settled = true
      })
      await flush()
      expect(settled).toBe(withCallback)
      expect(f.callback).not.toHaveBeenCalled()
      loop.resolve()
      await start
      await flush()
      expect(f.callback.mock.calls.length).toBe(withCallback ? 1 : 0)
    }
  })
})
