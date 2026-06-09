import { assert, assertEquals, assertNotStrictEquals, assertStrictEquals } from "@std/assert"
import { afterEach, describe, it } from "@std/testing/bdd"
import { stub } from "@std/testing/mock"
import type { NotificationClient, NotificationEvent } from "@flowcore/sdk"
import type { Subject } from "rxjs"
import { _internals, FlowcoreNotifier } from "../../src/data-pump/notifier.ts"
import type { FlowcoreLogger } from "../../src/data-pump/types.ts"

// #region Test Helpers

const FAKE_API_KEY = "fc_testid_testsecret"

interface FakeClientHandle {
  client: NotificationClient
  subject: Subject<NotificationEvent>
  options: unknown
  connectCalls: number
  disconnectCalls: number
  // Triggers fired automatically as part of connect(). The factory runs each
  // entry after the connect() promise has resolved but before the awaited
  // tick — letting tests drive WS-error / next-event scenarios deterministically.
  pendingPostConnect: Array<(handle: FakeClientHandle) => void>
  // If set, connect() throws synchronously instead of resolving.
  connectError?: Error
  // If true, subject.error fires BEFORE connect() resolves (synchronous race).
  errorBeforeConnect?: Error
  // If true, connect() returns a promise that never resolves (simulates a
  // hung websocket reconnect — half-open / black-holed socket). This is the
  // production wedge: pre-fix, `await connect()` blocks the loop forever.
  hangConnect?: boolean
}

type FakeFactoryConfig = {
  onCreate: (handle: FakeClientHandle) => void
}

function createFakeFactory(config: FakeFactoryConfig) {
  const handles: FakeClientHandle[] = []
  const factory = (
    observer: Subject<NotificationEvent>,
    _auth: unknown,
    _spec: unknown,
    opts: unknown,
  ): NotificationClient => {
    const handle: FakeClientHandle = {
      client: null as unknown as NotificationClient,
      subject: observer,
      options: opts,
      connectCalls: 0,
      disconnectCalls: 0,
      pendingPostConnect: [],
    }
    config.onCreate(handle)
    const client = {
      async connect() {
        handle.connectCalls++
        if (handle.errorBeforeConnect) {
          handle.subject.error(handle.errorBeforeConnect)
        }
        if (handle.hangConnect) {
          // Never resolves — simulates a hung reconnect.
          await new Promise<void>(() => {})
        }
        if (handle.connectError) {
          throw handle.connectError
        }
        // Use a microtask so callers can observe `await connect()` first.
        await Promise.resolve()
        for (const fn of handle.pendingPostConnect) {
          fn(handle)
        }
      },
      disconnect() {
        handle.disconnectCalls++
      },
    } as unknown as NotificationClient
    handle.client = client
    handles.push(handle)
    return client
  }
  return { factory, handles }
}

function createRecordingLogger(): FlowcoreLogger & { calls: Record<keyof FlowcoreLogger, string[]> } {
  const calls: Record<keyof FlowcoreLogger, string[]> = {
    debug: [],
    info: [],
    warn: [],
    error: [],
  }
  return {
    calls,
    debug: (message) => calls.debug.push(message),
    info: (message) => calls.info.push(message),
    warn: (message) => calls.warn.push(message),
    error: (message) => calls.error.push(String(message)),
  }
}

function createNotifier(timeoutMs?: number, logger?: FlowcoreLogger): FlowcoreNotifier {
  return new FlowcoreNotifier({
    auth: { apiKey: FAKE_API_KEY, apiKeyId: "testid" },
    dataSource: {
      tenant: "test-tenant",
      dataCore: "test-dc",
      flowType: "test.0",
      eventTypes: ["test.created.0"],
    },
    noTranslation: true,
    timeoutMs,
    logger: logger ?? {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: () => {},
    },
  })
}

// #endregion

describe("FlowcoreNotifier.waitWebSocket — bug fix (v0.20.1)", () => {
  // deno-lint-ignore no-explicit-any
  let factoryStub: any

  afterEach(() => {
    factoryStub?.restore()
    factoryStub = undefined
  })

  it("case 1 — WS error during wait() resolves the promise within 50 ms", async () => {
    const handles: FakeClientHandle[] = []
    const { factory } = createFakeFactory({
      onCreate: (h) => {
        h.pendingPostConnect.push((handle) => {
          handle.subject.error(new Error("ws closed"))
        })
        handles.push(h)
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(20_000)
    // Safety: if the bug is present, wait() will hang 20 s. We assert resolution
    // happens before a 250 ms deadline AND record the actual elapsed.
    let timedOut = true
    const safety = setTimeout(() => {
      // Force-resolve the resolver by firing abort path through a manual signal,
      // so the test fails on assertion (rather than hanging the runner).
      const r = (notifier as unknown as { eventResolver?: () => void }).eventResolver
      r?.()
    }, 250)

    const start = Date.now()
    await notifier.wait()
    const elapsed = Date.now() - start
    clearTimeout(safety)
    timedOut = false

    assertEquals(timedOut, false)
    assert(elapsed < 50, `expected elapsed < 50 ms, got ${elapsed}`)
    assertEquals(handles.length, 1)
    assertEquals(handles[0].disconnectCalls, 1, "disconnect must run via try/finally")
  })

  it("case 2 — subject is recreated on each wait() call", async () => {
    const subjects: Array<Subject<NotificationEvent>> = []
    const { factory } = createFakeFactory({
      onCreate: (h) => {
        subjects.push(h.subject)
        h.pendingPostConnect.push((handle) => handle.subject.error(new Error("close")))
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(20_000)
    await notifier.wait()
    await notifier.wait()

    assertEquals(subjects.length, 2)
    assertNotStrictEquals(subjects[0], subjects[1])
  })

  it("case 3 — after a WS-error cycle, the next wait() delivers events normally", async () => {
    let cycle = 0
    let secondEventCount = 0
    const onNextSpy = (h: FakeClientHandle) => {
      // Increment whenever the SUBSCRIBER's next() fires on the matching event.
      h.subject.subscribe({
        next: (ev: NotificationEvent) => {
          if (ev.data.eventType === "test.created.0") secondEventCount++
        },
      })
    }

    const { factory } = createFakeFactory({
      onCreate: (h) => {
        cycle++
        if (cycle === 1) {
          h.pendingPostConnect.push((handle) => handle.subject.error(new Error("flap")))
        } else {
          onNextSpy(h)
          h.pendingPostConnect.push((handle) => {
            handle.subject.next({
              pattern: "stored.event.notify.0",
              data: {
                tenant: "test-tenant",
                eventId: "event-1",
                dataCoreId: "test-dc",
                flowType: "test.0",
                eventType: "test.created.0",
                validTime: new Date().toISOString(),
              },
            })
          })
        }
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(20_000)
    await notifier.wait() // cycle 1 — WS error path
    const start = Date.now()
    await notifier.wait() // cycle 2 — happy event path
    const elapsed = Date.now() - start

    assertEquals(cycle, 2, "factory invoked twice")
    assert(elapsed < 100, `cycle 2 should resolve quickly via next(), got ${elapsed} ms`)
    assertEquals(secondEventCount, 1, "exactly one matching event observed on the second cycle")
  })

  it("case 4 — timeout-only path resolves around the configured boundary", async () => {
    const handles: FakeClientHandle[] = []
    const { factory } = createFakeFactory({
      onCreate: (h) => handles.push(h),
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(30)

    const start = Date.now()
    await notifier.wait()
    const elapsed = Date.now() - start

    assert(elapsed >= 25, `expected elapsed >= 25 ms, got ${elapsed}`)
    assert(elapsed <= 200, `expected elapsed <= 200 ms (jitter window), got ${elapsed}`)
    assertEquals(handles[0].disconnectCalls, 1)
  })

  it("case 5 — AbortSignal aborts the wait promptly", async () => {
    const handles: FakeClientHandle[] = []
    const { factory } = createFakeFactory({
      onCreate: (h) => handles.push(h),
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(20_000)
    const controller = new AbortController()
    setTimeout(() => controller.abort(), 10)

    const start = Date.now()
    await notifier.wait(controller.signal)
    const elapsed = Date.now() - start

    assert(elapsed < 100, `expected abort to resolve within 100 ms, got ${elapsed}`)
    assertEquals(handles[0].disconnectCalls, 1)
  })

  it("case 6 — disconnect() runs on every exit path (error, timeout, success, abort)", async () => {
    // error path — pre-fix code only resolved via the 20 s timeout, so we
    // bound timing tightly: disconnect must run within 100 ms of the error.
    {
      const handles: FakeClientHandle[] = []
      const { factory } = createFakeFactory({
        onCreate: (h) => {
          handles.push(h)
          h.pendingPostConnect.push((handle) => handle.subject.error(new Error("ws down")))
        },
      })
      const s = stub(_internals, "createNotificationClient", factory)
      const start = Date.now()
      try {
        const notifier = createNotifier(20_000)
        await notifier.wait()
      } finally {
        s.restore()
      }
      const elapsed = Date.now() - start
      assertEquals(handles[0].disconnectCalls, 1, "error path: disconnect must run")
      assert(elapsed < 100, `error path: disconnect must run within 100 ms, got ${elapsed}`)
    }
    // timeout path
    {
      const handles: FakeClientHandle[] = []
      const { factory } = createFakeFactory({
        onCreate: (h) => handles.push(h),
      })
      const s = stub(_internals, "createNotificationClient", factory)
      try {
        const notifier = createNotifier(20)
        await notifier.wait()
      } finally {
        s.restore()
      }
      assertEquals(handles[0].disconnectCalls, 1, "timeout path: disconnect must run")
    }
    // success path (event matches)
    {
      const handles: FakeClientHandle[] = []
      const { factory } = createFakeFactory({
        onCreate: (h) => {
          handles.push(h)
          h.pendingPostConnect.push((handle) => {
            handle.subject.next({
              pattern: "stored.event.notify.0",
              data: {
                tenant: "test-tenant",
                eventId: "e",
                dataCoreId: "test-dc",
                flowType: "test.0",
                eventType: "test.created.0",
                validTime: new Date().toISOString(),
              },
            })
          })
        },
      })
      const s = stub(_internals, "createNotificationClient", factory)
      try {
        const notifier = createNotifier(20_000)
        await notifier.wait()
      } finally {
        s.restore()
      }
      assertEquals(handles[0].disconnectCalls, 1, "success path: disconnect must run")
    }
    // abort path
    {
      const handles: FakeClientHandle[] = []
      const { factory } = createFakeFactory({
        onCreate: (h) => handles.push(h),
      })
      const s = stub(_internals, "createNotificationClient", factory)
      try {
        const notifier = createNotifier(20_000)
        const controller = new AbortController()
        setTimeout(() => controller.abort(), 5)
        await notifier.wait(controller.signal)
      } finally {
        s.restore()
      }
      assertEquals(handles[0].disconnectCalls, 1, "abort path: disconnect must run")
    }
  })

  it("case 7 — no double-resolve crash when next() fires on a terminated subject", async () => {
    let resolveCount = 0
    let crashed: Error | undefined
    const { factory } = createFakeFactory({
      onCreate: (h) => {
        h.pendingPostConnect.push((handle) => {
          handle.subject.error(new Error("ws down"))
          // Try to push a `next()` against the now-terminated subject within
          // the same microtask window. RxJS drops `next()` calls on a subject
          // that has emitted `error()`. The test asserts no crash and a
          // single resolve.
          try {
            handle.subject.next({
              pattern: "stored.event.notify.0",
              data: {
                tenant: "test-tenant",
                eventId: "e",
                dataCoreId: "test-dc",
                flowType: "test.0",
                eventType: "test.created.0",
                validTime: new Date().toISOString(),
              },
            })
          } catch (err) {
            crashed = err as Error
          }
        })
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(20_000)
    // Wrap the eventResolver to count how many times it would have fired.
    // We snapshot it before await resumes by patching post-binding.
    const origSetter = Object.getOwnPropertyDescriptor(notifier, "eventResolver")
    Object.defineProperty(notifier, "eventResolver", {
      configurable: true,
      get() {
        return this._evRes
      },
      set(fn: (() => void) | undefined) {
        if (fn === undefined) {
          this._evRes = undefined
          return
        }
        const orig = fn
        this._evRes = () => {
          resolveCount++
          orig()
        }
      },
    })

    const start = Date.now()
    await notifier.wait()
    const elapsed = Date.now() - start
    // restore for tidiness
    if (origSetter) Object.defineProperty(notifier, "eventResolver", origSetter)

    assertEquals(crashed, undefined, "subject.next on terminated subject must not throw")
    assertEquals(resolveCount, 1, "exactly one resolve must fire")
    // Resolve must come from the error handler (post-fix), not the 20 s timer.
    assert(elapsed < 100, `must resolve via error handler within 100 ms, got ${elapsed}`)
  })

  it("case 8 — synchronous error before connect() resolves: wait() resolves cleanly", async () => {
    // Post-fix invariant: the eventResolver is bound BEFORE connect() runs, so
    // a synchronous subject.error() during connect() fires the resolver via the
    // error handler. wait() must resolve cleanly (no rejection), and the timer
    // path is never armed because connect()'s exception unwinds through the
    // try/finally that runs disconnect().
    let resolvedCleanly = false
    let rejected: Error | undefined
    const handles: FakeClientHandle[] = []
    const { factory } = createFakeFactory({
      onCreate: (h) => {
        handles.push(h)
        h.errorBeforeConnect = new Error("sync ws error during connect")
        h.connectError = new Error("connect failed")
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(20_000)
    const start = Date.now()
    try {
      await notifier.wait()
      resolvedCleanly = true
    } catch (err) {
      rejected = err as Error
    }
    const elapsed = Date.now() - start

    // Either invariant is acceptable per plan, but the implementation routes the
    // connect() error through try/finally, which re-throws. We assert the resolver
    // fired BEFORE connect() threw (proving promise+resolver were bound first),
    // and that disconnect() still ran.
    assert(elapsed < 100, `must not wait for the 20 s timeout, elapsed ${elapsed} ms`)
    assertEquals(handles[0].disconnectCalls, 1, "disconnect() runs in finally")
    // The synchronous error fires through the subject's error handler, which
    // sets the resolver — proving the resolver was bound BEFORE connect().
    // connect() then throws, so wait() rejects. This is the documented invariant.
    assertStrictEquals(rejected?.message, "connect failed")
    assertEquals(resolvedCleanly, false)
  })

  it("case 9 — hung connect() must not wedge: wait() resolves via the pre-armed timeout", async () => {
    // Production wedge: a reconnect whose connect() never resolves. Pre-fix, the
    // 20s re-poll timer is armed only AFTER connect() resolves, so the loop hangs
    // forever. Post-fix, timer + abort are armed BEFORE connect(), so wait()
    // resolves at timeoutMs and the loop re-polls.
    const handles: FakeClientHandle[] = []
    const { factory } = createFakeFactory({
      onCreate: (h) => {
        h.hangConnect = true
        handles.push(h)
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(30)
    // Safety: if the bug is present, wait() hangs. Force-resolve at 250ms so the
    // runner doesn't stall — the elapsed assertion then fails (proving the wedge).
    const safety = setTimeout(() => {
      const r = (notifier as unknown as { eventResolver?: () => void }).eventResolver
      r?.()
    }, 250)

    const start = Date.now()
    await notifier.wait()
    const elapsed = Date.now() - start
    clearTimeout(safety)

    assert(elapsed >= 25, `expected resolve via ~30ms timeout, got ${elapsed} ms`)
    assert(elapsed < 200, `hung connect must not wedge — expected < 200 ms, got ${elapsed} ms`)
    assertEquals(handles.length, 1)
    assertEquals(handles[0].disconnectCalls, 1, "disconnect must run via try/finally even on hung connect")
  })

  it("case 10 — retries while degraded, then returns to WS mode on reconnect", async () => {
    // Cycle 1: connect() hangs (degraded → resolves via timeout re-poll).
    // Cycle 2: connect() succeeds and delivers an event (back in push/WS mode).
    // Proves there is no sticky poll-mode state and each cycle builds a fresh client.
    const handles: FakeClientHandle[] = []
    let cycle = 0
    const { factory } = createFakeFactory({
      onCreate: (h) => {
        cycle++
        if (cycle === 1) {
          h.hangConnect = true
        } else {
          h.pendingPostConnect.push((handle) => {
            handle.subject.next({
              pattern: "stored.event.notify.0",
              data: {
                tenant: "test-tenant",
                eventId: "event-1",
                dataCoreId: "test-dc",
                flowType: "test.0",
                eventType: "test.created.0",
                validTime: new Date().toISOString(),
              },
            })
          })
        }
        handles.push(h)
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(40)
    const safety = setTimeout(() => {
      const r = (notifier as unknown as { eventResolver?: () => void }).eventResolver
      r?.()
    }, 400)

    // Cycle 1 — degraded: hung connect resolves via the ~40ms timeout.
    const start1 = Date.now()
    await notifier.wait()
    const elapsed1 = Date.now() - start1

    // Cycle 2 — recovered: connect succeeds, event delivered, resolves fast.
    const start2 = Date.now()
    await notifier.wait()
    const elapsed2 = Date.now() - start2
    clearTimeout(safety)

    assert(elapsed1 >= 25 && elapsed1 < 200, `cycle 1 should re-poll at ~40ms, got ${elapsed1} ms`)
    assert(elapsed2 < 100, `cycle 2 should resolve fast via WS event (back in WS mode), got ${elapsed2} ms`)
    assertEquals(cycle, 2, "a fresh client is built each cycle")
    assertEquals(handles.length, 2)
    assertNotStrictEquals(handles[0].client, handles[1].client)
    assertEquals(handles[0].disconnectCalls, 1, "degraded cycle still disconnects the dead client")
    assertEquals(handles[1].disconnectCalls, 1)
  })

  it("demotes normal SDK notification lifecycle info logs to debug", async () => {
    const logger = createRecordingLogger()
    const { factory } = createFakeFactory({
      onCreate: (h) => {
        h.pendingPostConnect.push((handle) => {
          const sdkLogger = (handle.options as { logger: FlowcoreLogger }).logger
          sdkLogger.info("WebSocket connection opened.", { source: "sdk" })
          sdkLogger.info("Connection closed: Code [1000], Reason: Disconnected by user", { source: "sdk" })
          sdkLogger.info("Attempting reconnection 1 in 1000 ms...", { source: "sdk" })
          handle.subject.error(new Error("done"))
        })
      },
    })
    factoryStub = stub(_internals, "createNotificationClient", factory)

    const notifier = createNotifier(20_000, logger)
    await notifier.wait()

    assertEquals(logger.calls.debug.includes("WebSocket connection opened."), true)
    assertEquals(logger.calls.debug.includes("Connection closed: Code [1000], Reason: Disconnected by user"), true)
    assertEquals(logger.calls.info.includes("WebSocket connection opened."), false)
    assertEquals(logger.calls.info.includes("Connection closed: Code [1000], Reason: Disconnected by user"), false)
    assertEquals(logger.calls.info.includes("Attempting reconnection 1 in 1000 ms..."), true)
  })
})
