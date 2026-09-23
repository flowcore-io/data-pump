import { afterEach, describe, expect, it } from "bun:test"
import type { EventListOutput, FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"
import type { FlowcoreDataPumpState, FlowcoreDataPumpStateManager } from "../../src/data-pump/types.ts"

const AUTH = { apiKey: "fc_testid_testsecret" }
const DATA_SOURCE = {
  tenant: "test-tenant",
  dataCore: "test-data-core",
  flowType: "test-flow.0",
  eventTypes: ["alpha.0", "beta.0"],
}

const pumps: FlowcoreDataPump[] = []

afterEach(() => {
  for (const pump of pumps.splice(0)) pump.stop()
})

function event(index: number, hour = 12, eventType = index % 2 === 0 ? "alpha.0" : "beta.0"): FlowcoreEvent {
  const date = new Date(Date.UTC(2026, 8, 23, hour, 0, index))
  return {
    eventId: TimeUuid.fromDate(date).toString(),
    timeBucket: `20260923${hour.toString().padStart(2, "0")}0000`,
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "test-flow.0",
    eventType,
    metadata: {},
    validTime: date.toISOString(),
    payload: { index },
  }
}

class OnePageSource extends FlowcoreDataSource {
  private fetched = false

  constructor(private readonly events: FlowcoreEvent[]) {
    super({ auth: AUTH, dataSource: DATA_SOURCE, noTranslation: true })
  }

  override getClosestTimeBucket(timeBucket?: string): Promise<string> {
    return Promise.resolve(timeBucket ?? this.events[0]?.timeBucket ?? "20260923120000")
  }

  override getNextTimeBucket(): Promise<null> {
    return Promise.resolve(null)
  }

  override getEvents(): Promise<EventListOutput> {
    if (this.fetched) return new Promise(() => {})
    this.fetched = true
    return Promise.resolve({ events: this.events, nextCursor: undefined })
  }
}

class CursorPagesSource extends FlowcoreDataSource {
  public calls = 0

  constructor(
    private readonly firstPage: FlowcoreEvent[],
    private readonly secondPage: FlowcoreEvent[],
  ) {
    super({ auth: AUTH, dataSource: DATA_SOURCE, noTranslation: true })
  }

  override getClosestTimeBucket(timeBucket?: string): Promise<string> {
    return Promise.resolve(timeBucket ?? this.firstPage[0]!.timeBucket)
  }

  override getNextTimeBucket(): Promise<null> {
    return Promise.resolve(null)
  }

  override getEvents(
    _from: FlowcoreDataPumpState,
    _amount: number,
    _toEventId?: string,
    cursor?: string,
  ): Promise<EventListOutput> {
    this.calls++
    if (!cursor) return Promise.resolve({ events: this.firstPage, nextCursor: "next" })
    if (cursor === "next") return Promise.resolve({ events: this.secondPage, nextCursor: undefined })
    return new Promise(() => {})
  }
}

class BucketPagesSource extends FlowcoreDataSource {
  private readonly fetchedBuckets = new Set<string>()

  constructor(private readonly pages: Map<string, FlowcoreEvent[]>) {
    super({ auth: AUTH, dataSource: DATA_SOURCE, noTranslation: true })
  }

  override getClosestTimeBucket(timeBucket?: string): Promise<string> {
    return Promise.resolve(timeBucket ?? this.pages.keys().next().value ?? "20260923120000")
  }

  override getNextTimeBucket(timeBucket: string): Promise<string | null> {
    const buckets = [...this.pages.keys()]
    const index = buckets.indexOf(timeBucket)
    return Promise.resolve(index >= 0 ? (buckets[index + 1] ?? null) : null)
  }

  override getEvents(from: FlowcoreDataPumpState): Promise<EventListOutput> {
    if (this.fetchedBuckets.has(from.timeBucket)) return Promise.resolve({ events: [], nextCursor: undefined })
    this.fetchedBuckets.add(from.timeBucket)
    return Promise.resolve({ events: this.pages.get(from.timeBucket) ?? [], nextCursor: undefined })
  }
}

class ExclusiveResumeSource extends FlowcoreDataSource {
  public requestedStates: FlowcoreDataPumpState[] = []
  private fetched = false

  constructor(private readonly orderedEvents: FlowcoreEvent[]) {
    super({ auth: AUTH, dataSource: DATA_SOURCE, noTranslation: true })
  }

  override getClosestTimeBucket(timeBucket?: string): Promise<string> {
    return Promise.resolve(timeBucket ?? this.orderedEvents[0]!.timeBucket)
  }

  override getNextTimeBucket(): Promise<null> {
    return Promise.resolve(null)
  }

  override getEvents(from: FlowcoreDataPumpState): Promise<EventListOutput> {
    this.requestedStates.push({ ...from })
    if (this.fetched) return new Promise(() => {})
    this.fetched = true
    const index = from.eventId ? this.orderedEvents.findIndex((item) => item.eventId === from.eventId) : -1
    return Promise.resolve({ events: this.orderedEvents.slice(index + 1), nextCursor: undefined })
  }
}

class RestartReplaySource extends FlowcoreDataSource {
  public calls = 0

  constructor(private readonly replayedEvent: FlowcoreEvent) {
    super({ auth: AUTH, dataSource: DATA_SOURCE, noTranslation: true })
  }

  override getTimeBuckets(): Promise<string[]> {
    return Promise.resolve([this.replayedEvent.timeBucket])
  }

  override getClosestTimeBucket(timeBucket?: string): Promise<string> {
    return Promise.resolve(timeBucket ?? this.replayedEvent.timeBucket)
  }

  override getNextTimeBucket(): Promise<null> {
    return Promise.resolve(null)
  }

  override getEvents(): Promise<EventListOutput> {
    this.calls++
    return Promise.resolve({ events: [this.replayedEvent], nextCursor: undefined })
  }
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void
  const promise = new Promise<void>((resolver) => {
    resolve = resolver
  })
  return { promise, resolve }
}

async function waitUntil(condition: () => boolean, message: string): Promise<void> {
  for (let attempt = 0; attempt < 1_000; attempt++) {
    if (condition()) return
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
  throw new Error(message)
}

async function startPump(
  events: FlowcoreEvent[],
  stateManager: FlowcoreDataPumpStateManager,
  source: FlowcoreDataSource = new OnePageSource(events),
  minimumBufferDepth = events.length,
): Promise<FlowcoreDataPump> {
  const pump = FlowcoreDataPump.create(
    {
      auth: AUTH,
      dataSource: DATA_SOURCE,
      stateManager,
      notifier: { type: "poller", intervalMs: 60_000 },
      bufferSize: Math.max(events.length, 1),
      achknowledgeTimeoutMs: 60_000,
      noTranslation: true,
    },
    source,
  )
  pumps.push(pump)
  void pump.start(() => {})
  await waitUntil(() => (pump.getSnapshot()?.bufferDepth ?? 0) >= minimumBufferDepth, "pump did not fill its buffer")
  return pump
}

async function startRestartablePump(
  replayedEvent: FlowcoreEvent,
  stateManager: FlowcoreDataPumpStateManager,
  failedHandler: (events: FlowcoreEvent[]) => Promise<void>,
  achknowledgeTimeoutMs = 60_000,
): Promise<{ pump: FlowcoreDataPump; source: RestartReplaySource }> {
  const source = new RestartReplaySource(replayedEvent)
  const pump = FlowcoreDataPump.create(
    {
      auth: AUTH,
      dataSource: DATA_SOURCE,
      stateManager,
      processor: { handler: () => Promise.resolve(), failedHandler },
      paused: true,
      notifier: { type: "poller", intervalMs: 60_000 },
      bufferSize: 1,
      maxRedeliveryCount: 0,
      achknowledgeTimeoutMs,
      noTranslation: true,
    },
    source,
  )
  pumps.push(pump)
  void pump.start(() => {})
  await waitUntil(() => source.calls >= 1 && pump.getSnapshot()?.bufferDepth === 1, "pump did not fetch the event")
  return { pump, source }
}

describe("contiguous checkpoint frontier", () => {
  it("does not checkpoint an unfinished event when a later event is acknowledged first", async () => {
    const events = [event(0), event(1), event(2)]
    const checkpoints: FlowcoreDataPumpState[] = []
    const pump = await startPump(events, {
      getState: () => ({ timeBucket: events[0].timeBucket }),
      setState: (state) => {
        checkpoints.push({ ...state })
      },
    })

    const reserved = await pump.reserve(2)
    await pump.acknowledge([reserved[1]!.eventId])

    expect(checkpoints).toEqual([])
  })

  it("keeps the fetch window bounded while a completion gap is open", async () => {
    const events = [event(0), event(1), event(2)]
    const source = new CursorPagesSource(events.slice(0, 2), events.slice(2))
    const pump = await startPump(
      events.slice(0, 2),
      {
        getState: () => ({ timeBucket: events[0]!.timeBucket }),
        setState: () => {},
      },
      source,
    )
    const reserved = await pump.reserve(2)

    await pump.acknowledge([reserved[1]!.eventId])
    await new Promise((resolve) => setTimeout(resolve, 5))
    expect(source.calls).toBe(1)

    await pump.acknowledge([reserved[0]!.eventId])
    await waitUntil(() => source.calls >= 2, "fetching did not resume after the gap closed")
    await waitUntil(() => pump.getSnapshot()?.bufferDepth === 1, "second page was not buffered")
    const [next] = await pump.reserve(1)
    expect(next?.eventId).toBe(events[2]!.eventId)
  })

  it("advances through already completed later events once the gap closes", async () => {
    const events = [event(0), event(1), event(2)]
    const checkpoints: FlowcoreDataPumpState[] = []
    const pump = await startPump(events, {
      getState: () => ({ timeBucket: events[0].timeBucket }),
      setState: (state) => {
        checkpoints.push({ ...state })
      },
    })

    const reserved = await pump.reserve(2)
    await pump.acknowledge([reserved[1]!.eventId])
    await pump.acknowledge([reserved[0]!.eventId])

    expect(checkpoints).toEqual([{ timeBucket: events[1]!.timeBucket, eventId: events[1]!.eventId }])
  })

  it("checkpoints partial, complete, unknown, and empty acknowledgements at the safe boundary", async () => {
    const events = [event(0), event(1), event(2)]
    const checkpoints: FlowcoreDataPumpState[] = []
    const pump = await startPump(events, {
      getState: () => ({ timeBucket: events[0].timeBucket }),
      setState: (state) => {
        checkpoints.push({ ...state })
      },
    })

    await pump.acknowledge([])
    await pump.acknowledge(["00000000-0000-1000-8000-000000000000"])
    expect(checkpoints).toEqual([])

    const reserved = await pump.reserve(3)
    await pump.acknowledge([reserved[0]!.eventId])
    await pump.acknowledge([reserved[1]!.eventId, reserved[2]!.eventId])

    expect(checkpoints).toEqual([
      { timeBucket: events[0]!.timeBucket, eventId: events[0]!.eventId },
      { timeBucket: events[2]!.timeBucket, eventId: events[2]!.eventId },
    ])
  })

  it("uses source order across event types instead of event-type or lexical ordering", async () => {
    const events = [event(0, 12, "beta.0"), event(1, 12, "alpha.0"), event(2, 12, "beta.0")]
    const checkpoints: FlowcoreDataPumpState[] = []
    const pump = await startPump(events, {
      getState: () => ({ timeBucket: events[0].timeBucket }),
      setState: (state) => {
        checkpoints.push({ ...state })
      },
    })

    const reserved = await pump.reserve(3)
    await pump.acknowledge([reserved[2]!.eventId, reserved[1]!.eventId])
    expect(checkpoints).toEqual([])
    await pump.acknowledge([reserved[0]!.eventId])

    expect(checkpoints).toEqual([{ timeBucket: events[2]!.timeBucket, eventId: events[2]!.eventId }])
  })

  it("persists the completed event's bucket when the frontier crosses buckets", async () => {
    const events = [event(59, 12), event(0, 13), event(1, 13)]
    const checkpoints: FlowcoreDataPumpState[] = []
    const source = new BucketPagesSource(
      new Map([
        [events[0]!.timeBucket, [events[0]!]],
        [events[1]!.timeBucket, events.slice(1)],
      ]),
    )
    const pump = await startPump(
      events,
      {
        getState: () => ({ timeBucket: events[0].timeBucket }),
        setState: (state) => {
          checkpoints.push({ ...state })
        },
      },
      source,
    )

    const reserved = await pump.reserve(3)
    await pump.acknowledge([reserved[1]!.eventId])
    await pump.acknowledge([reserved[0]!.eventId])

    expect(checkpoints).toEqual([{ timeBucket: events[1]!.timeBucket, eventId: events[1]!.eventId }])
  })

  it("resumes after the last contiguous completion without skipping unfinished work", async () => {
    const previous = event(59, 11)
    const events = [event(0), event(1), event(2)]
    let persisted: FlowcoreDataPumpState = { timeBucket: previous.timeBucket, eventId: previous.eventId }
    const stateManager: FlowcoreDataPumpStateManager = {
      getState: () => ({ ...persisted }),
      setState: (state) => {
        persisted = { ...state }
      },
    }
    const firstSource = new ExclusiveResumeSource([previous, ...events])
    const firstPump = await startPump(events, stateManager, firstSource)
    const reserved = await firstPump.reserve(2)

    await firstPump.acknowledge([reserved[1]!.eventId])
    firstPump.stop()

    const resumeSource = new ExclusiveResumeSource([previous, ...events])
    const resumedPump = await startPump(events, stateManager, resumeSource, 1)
    const replayed = await resumedPump.reserve(3)

    expect(persisted).toEqual({ timeBucket: previous.timeBucket, eventId: previous.eventId })
    expect(resumeSource.requestedStates[0]).toEqual(persisted)
    expect(replayed.map((item) => item.eventId)).toEqual(events.map((item) => item.eventId))
  })

  it("serializes checkpoint writes so a slower earlier write cannot overwrite a later frontier", async () => {
    const events = [event(0), event(1)]
    const writes: FlowcoreDataPumpState[] = []
    let releaseFirst!: () => void
    const firstWrite = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
    const pump = await startPump(events, {
      getState: () => ({ timeBucket: events[0].timeBucket }),
      setState: (state) => {
        writes.push({ ...state })
        return writes.length === 1 ? firstWrite : Promise.resolve()
      },
    })
    const reserved = await pump.reserve(2)

    const firstAcknowledgement = pump.acknowledge([reserved[0]!.eventId])
    await waitUntil(() => writes.length === 1, "first checkpoint write did not start")
    const secondAcknowledgement = pump.acknowledge([reserved[1]!.eventId])
    await new Promise((resolve) => setTimeout(resolve, 5))
    expect(writes).toEqual([{ timeBucket: events[0]!.timeBucket, eventId: events[0]!.eventId }])

    releaseFirst()
    await Promise.all([firstAcknowledgement, secondAcknowledgement])
    expect(writes).toEqual([
      { timeBucket: events[0]!.timeBucket, eventId: events[0]!.eventId },
      { timeBucket: events[1]!.timeBucket, eventId: events[1]!.eventId },
    ])
  })

  it("continues with a later contiguous checkpoint after an earlier persistence error", async () => {
    const events = [event(0), event(1)]
    const writes: FlowcoreDataPumpState[] = []
    const pump = await startPump(events, {
      getState: () => ({ timeBucket: events[0].timeBucket }),
      setState: (state) => {
        writes.push({ ...state })
        if (writes.length === 1) return Promise.reject(new Error("checkpoint unavailable"))
      },
    })
    const reserved = await pump.reserve(2)

    await expect(pump.acknowledge([reserved[0]!.eventId])).rejects.toThrow("checkpoint unavailable")
    await pump.acknowledge([reserved[1]!.eventId])

    expect(writes).toEqual([
      { timeBucket: events[0]!.timeBucket, eventId: events[0]!.eventId },
      { timeBucket: events[1]!.timeBucket, eventId: events[1]!.eventId },
    ])
  })

  it("treats terminal failures as completed without advancing past an earlier unfinished event", async () => {
    const events = [event(0), event(1)]
    const checkpoints: FlowcoreDataPumpState[] = []
    const pump = await startPump(events, {
      getState: () => ({ timeBucket: events[0].timeBucket }),
      setState: (state) => {
        checkpoints.push({ ...state })
      },
    })
    const reserved = await pump.reserve(2)

    await pump.fail([reserved[1]!.eventId])
    expect(checkpoints).toEqual([])
    await pump.acknowledge([reserved[0]!.eventId])

    expect(checkpoints).toEqual([{ timeBucket: events[1]!.timeBucket, eventId: events[1]!.eventId }])
  })

  it("does not let a stale direct failure complete a replayed event after restart", async () => {
    const previous = event(59, 11)
    const replayedEvent = event(0)
    const checkpoints: FlowcoreDataPumpState[] = []
    const failedHandler = deferred()
    let failedHandlerCalls = 0
    const { pump, source } = await startRestartablePump(
      replayedEvent,
      {
        getState: () => ({ timeBucket: previous.timeBucket, eventId: previous.eventId }),
        setState: (state) => {
          checkpoints.push({ ...state })
        },
      },
      () => {
        failedHandlerCalls++
        return failedHandler.promise
      },
    )
    const [oldDelivery] = await pump.reserve(1)

    const oldFailure = pump.fail([oldDelivery!.eventId])
    await waitUntil(() => failedHandlerCalls === 1, "old failed handler did not start")
    pump.restart({ timeBucket: previous.timeBucket, eventId: previous.eventId })
    await waitUntil(
      () => source.calls >= 2 && pump.getSnapshot()?.bufferDepth === 1,
      "event was not replayed after restart",
    )
    const [newDelivery] = await pump.reserve(1)

    failedHandler.resolve()
    await oldFailure
    expect(checkpoints).toEqual([])

    await pump.acknowledge([newDelivery!.eventId])
    expect(checkpoints).toEqual([{ timeBucket: replayedEvent.timeBucket, eventId: replayedEvent.eventId }])
  })

  it("does not let a stale terminal reopen complete a replayed event after restart", async () => {
    const previous = event(59, 11)
    const replayedEvent = event(0)
    const checkpoints: FlowcoreDataPumpState[] = []
    const failedHandler = deferred()
    let failedHandlerCalls = 0
    const { pump, source } = await startRestartablePump(
      replayedEvent,
      {
        getState: () => ({ timeBucket: previous.timeBucket, eventId: previous.eventId }),
        setState: (state) => {
          checkpoints.push({ ...state })
        },
      },
      () => {
        failedHandlerCalls++
        return failedHandler.promise
      },
      1,
    )
    const internals = pump as unknown as {
      reOpen: (eventIds: string[], deliveryId: string) => Promise<void>
    }
    const originalReOpen = internals.reOpen.bind(pump)
    let terminalReOpen: Promise<void> | undefined
    internals.reOpen = (eventIds, deliveryId) => {
      terminalReOpen = originalReOpen(eventIds, deliveryId)
      return terminalReOpen
    }

    await pump.reserve(1)
    await waitUntil(
      () => failedHandlerCalls === 1 && terminalReOpen !== undefined,
      "terminal acknowledgement timeout did not start",
    )
    pump.restart({ timeBucket: previous.timeBucket, eventId: previous.eventId })
    await waitUntil(
      () => source.calls >= 2 && pump.getSnapshot()?.bufferDepth === 1,
      "event was not replayed after restart",
    )
    const [newDelivery] = await pump.reserve(1)

    failedHandler.resolve()
    await terminalReOpen
    expect(checkpoints).toEqual([])

    await pump.acknowledge([newDelivery!.eventId])
    expect(checkpoints).toEqual([{ timeBucket: replayedEvent.timeBucket, eventId: replayedEvent.eventId }])
  })
})
