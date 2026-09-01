import { utc } from "@date-fns/utc"
import type { FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { format, startOfHour } from "date-fns"
import { FlowcoreDataSource } from "./data-source.ts"
import { metrics, ReplayStageObserver } from "./metrics.ts"
import { FlowcoreNotifier } from "./notifier.ts"
import { PulseEmitter, type PulseSnapshot } from "./pulse.ts"
import type {
  FlowcoreDataPumpAuth,
  FlowcoreDataPumpDataSource,
  FlowcoreDataPumpProcessor,
  FlowcoreDataPumpState,
  FlowcoreDataPumpStateManager,
  FlowcoreLogger,
} from "./types.ts"

const textEncoder = new TextEncoder()

interface FlowcoreDataPumpNotifierNatsOptions {
  type: "nats"
  servers: string[]
}

interface FlowcoreDataPumpNotifierWebsocketOptions {
  type: "websocket"
}

interface FlowcoreDataPumpNotifierPollerOptions {
  type: "poller"
  /**
   * The interval in milliseconds to poll the data pump state (min 1000 ms)
   */
  intervalMs: number
}

type FlowcoreDataPumpNotifierOptions =
  | FlowcoreDataPumpNotifierNatsOptions
  | FlowcoreDataPumpNotifierWebsocketOptions
  | FlowcoreDataPumpNotifierPollerOptions

export interface FlowcoreDataPumpOptions {
  auth: FlowcoreDataPumpAuth
  dataSource: FlowcoreDataPumpDataSource
  stateManager: FlowcoreDataPumpStateManager
  bufferSize?: number
  maxRedeliveryCount?: number
  achknowledgeTimeoutMs?: number
  includeSensitiveData?: boolean
  processor?: FlowcoreDataPumpProcessor
  notifier?: FlowcoreDataPumpNotifierOptions
  logger?: FlowcoreLogger
  stopAt?: Date
  baseUrlOverride?: string
  noTranslation?: boolean
  directMode?: boolean
  pulse?: {
    url: string
    intervalMs?: number
    pathwayId: string
    sourceId?: string
    /** Log level for successful pulses. Defaults to 'debug'. */
    successLogLevel?: "debug" | "info" | "warn" | "error"
    /** Log level for pulse failures. Defaults to 'warn'. */
    failureLogLevel?: "debug" | "info" | "warn" | "error"
  }
}

interface FlowcoreDataPumpInnerOptions {
  bufferSize: number
  bufferThreshold: number
  maxRedeliveryCount: number
  achknowledgeTimeoutMs: number
  includeSensitiveData: boolean
  processor?: FlowcoreDataPumpProcessor
  stopAt?: Date
}

interface FlowcoreDataPumpBufferItem {
  event: FlowcoreEvent
  status: "open" | "reserved"
  deliveryCount: number
  payloadSizeBytes: number
  eventSizeBytes?: number
  deliveryId?: string
}

interface FlowcoreDataPumpBufferStats {
  eventCount: number
  eventReservedCount: number
  eventSizeBytes: number
}

export class FlowcoreDataPump {
  private nextCursor?: string
  private running = false
  private restartTo?: FlowcoreDataPumpState
  private processLoopRunning = false
  // Invalidates delivery work that crossed a stop/restart boundary. Event IDs
  // can reappear during replay, so `running` alone cannot identify the owner.
  private processLoopGeneration = 0
  private abortController?: AbortController
  private buffer: FlowcoreDataPumpBufferItem[] = []
  private bufferState: FlowcoreDataPumpState
  private stopAtState?: FlowcoreDataPumpState
  private isLive = false
  private finallyFailedHandler?: (events: FlowcoreEvent[]) => Promise<void> | void
  private pulseEmitter?: PulseEmitter
  private startedAt = 0
  private acknowledgedCount = 0
  private failedCount = 0
  private pulledCount = 0
  private processLoopRestartAttempts = 0
  private mainLoopRestartAttempts = 0
  private readonly replayObserver: ReplayStageObserver
  private readonly bufferStats = new Map<string, FlowcoreDataPumpBufferStats>()
  private bufferReservedCount = 0
  private bufferSizeBytes = 0
  private gaugePublicationScheduled = false

  private constructor(
    public readonly dataSource: FlowcoreDataSource,
    private readonly notifier: FlowcoreNotifier,
    private stateManager: FlowcoreDataPumpStateManager,
    private readonly options: FlowcoreDataPumpInnerOptions,
    private readonly logger?: FlowcoreLogger,
  ) {
    this.replayObserver = new ReplayStageObserver(metrics, {
      tenant: this.dataSource.tenant,
      data_core: this.dataSource.dataCore,
      flow_type: this.dataSource.flowType,
    })
    for (const eventType of this.dataSource.eventTypes) {
      this.bufferStats.set(eventType, { eventCount: 0, eventReservedCount: 0, eventSizeBytes: 0 })
    }
    this.bufferState = {
      timeBucket: format(startOfHour(utc(new Date())), "yyyyMMddHH0000"),
      eventId: TimeUuid.now().toString(),
    }
  }

  public get isRunning(): boolean {
    return this.running
  }

  public getSnapshot(): PulseSnapshot | null {
    if (!this.running) return null
    return {
      pathwayId: this.pulseEmitter ? "" : "", // set by caller
      flowType: this.dataSource.flowType,
      timeBucket: this.bufferState.timeBucket,
      eventId: this.bufferState.eventId,
      isLive: this.isLive,
      bufferDepth: this.buffer.length,
      bufferReserved: this.bufferReservedCount,
      bufferSizeBytes: this.bufferSizeBytes,
      acknowledgedTotal: this.acknowledgedCount,
      failedTotal: this.failedCount,
      pulledTotal: this.pulledCount,
      uptimeMs: this.startedAt ? Date.now() - this.startedAt : 0,
    }
  }

  public static create(options: FlowcoreDataPumpOptions, dataSourceOverride?: FlowcoreDataSource): FlowcoreDataPump {
    if ("apiKey" in options.auth && !options.auth.apiKeyId) {
      const parts = options.auth.apiKey.split("_")
      if (parts.length !== 3 || parts[0] !== "fc") {
        throw new Error("Invalid API key")
      }
      options.auth.apiKeyId = parts[1]
    }

    const dataSource =
      dataSourceOverride ??
      new FlowcoreDataSource({
        auth: options.auth,
        dataSource: options.dataSource,
        baseUrlOverride: options.baseUrlOverride,
        noTranslation: options.noTranslation,
        directMode: options.directMode,
      })
    const notifier = new FlowcoreNotifier({
      auth: options.auth,
      dataSource: options.dataSource,
      natsServers: options.notifier?.type === "nats" ? options.notifier.servers : undefined,
      pollerIntervalMs: options.notifier?.type === "poller" ? options.notifier.intervalMs : undefined,
      logger: options.logger,
      directMode: options.directMode,
      noTranslation: options.noTranslation,
    })
    const pump = new FlowcoreDataPump(
      dataSource,
      notifier,
      options.stateManager,
      {
        bufferSize: options.bufferSize ?? 1000,
        bufferThreshold: Math.ceil((options.bufferSize ?? 1000) * 0.1),
        maxRedeliveryCount: options.maxRedeliveryCount ?? 3,
        achknowledgeTimeoutMs: options.achknowledgeTimeoutMs ?? 5_000,
        includeSensitiveData: options.includeSensitiveData ?? false,
        processor: options.processor,
        stopAt: options.stopAt,
      },
      options.logger,
    )

    if (options.pulse) {
      const pathwayId = options.pulse.pathwayId
      const sourceId = options.pulse.sourceId
      pump.pulseEmitter = new PulseEmitter(
        {
          url: options.pulse.url,
          intervalMs: options.pulse.intervalMs,
          auth: options.auth,
          logger: options.logger,
          successLogLevel: options.pulse.successLogLevel,
          failureLogLevel: options.pulse.failureLogLevel,
        },
        () => {
          const snapshot = pump.getSnapshot()
          if (snapshot) {
            snapshot.pathwayId = pathwayId
            snapshot.sourceId = sourceId
          }
          return snapshot
        },
      )
    }

    return pump
  }

  public async start(callback?: (error?: Error) => void): Promise<void> {
    this.isLive = false
    if (this.running) {
      throw new Error("Data pump already running")
    }
    this.running = true
    this.startedAt = Date.now()
    this.nextCursor = undefined
    this.updateMetricsGauges(true)
    this.pulseEmitter?.start()
    const currentState = await this.stateManager.getState()
    const timeBucket = currentState
      ? await this.dataSource.getClosestTimeBucket(currentState.timeBucket)
      : format(startOfHour(utc(new Date())), "yyyyMMddHH0000")
    this.bufferState = {
      timeBucket: timeBucket ?? format(startOfHour(utc(new Date())), "yyyyMMddHH0000"),
      eventId: currentState ? currentState.eventId : TimeUuid.now().toString(),
    }

    if (this.options.stopAt) {
      this.stopAtState = {
        timeBucket:
          (await this.dataSource.getClosestTimeBucket(
            format(startOfHour(utc(this.options.stopAt)), "yyyyMMddHH0000"),
            true,
          )) ?? format(startOfHour(utc(new Date())), "yyyyMMddHH0000"),
        eventId: TimeUuid.fromDate(this.options.stopAt).toString(),
      }
    }

    if (this.options.processor) {
      this.startProcessLoop()
    }

    if (!callback) {
      return this.loop()
    }

    this.startMainLoop(callback)
  }

  private startMainLoop(callback?: (error?: Error) => void): void {
    this.loop()
      .then(() => {
        this.mainLoopRestartAttempts = 0
        callback?.()
      })
      .catch((error) => {
        this.logger?.error("Error in fetch loop", { error })
        if (!this.running) {
          callback?.(error)
          return
        }
        this.mainLoopRestartAttempts++
        const delay = Math.min(1_000 * Math.pow(2, this.mainLoopRestartAttempts - 1), 30_000)
        this.logger?.warn(`Restarting fetch loop in ${delay}ms (attempt ${this.mainLoopRestartAttempts})`)
        setTimeout(() => {
          if (!this.running) {
            callback?.(error)
            return
          }
          this.startMainLoop(callback)
        }, delay)
      })
  }

  public restart(state: FlowcoreDataPumpState, stopAt?: Date | null): void {
    if (typeof state.timeBucket !== "string" || !state.timeBucket.match(/^\d{14}$/)) {
      throw new Error(`Invalid timebucket: ${state.timeBucket}`)
    }
    this.restartTo = state
    if (stopAt !== undefined) {
      this.options.stopAt = stopAt ?? undefined
    }
    this.isLive = false
    this.stop(true)
  }

  public stop(_isRestart = false): void {
    this.running = false
    this.processLoopGeneration++
    this.processLoopRestartAttempts = 0
    this.mainLoopRestartAttempts = 0
    this.buffer = []
    this.resetBufferStats()
    this.updateMetricsGauges(true)
    this.pulseEmitter?.stop()
    this.abortController?.abort()
    this.waiterBufferThreshold?.()
    this.waiterEvents?.()
  }

  private updateState(eventId?: string): Promise<void> | void {
    const stateManager = this.stateManager
    if (!stateManager.setState) {
      return
    }
    const stateEventId = eventId ?? this.buffer[0]?.event.eventId
    if (!stateEventId) {
      return
    }
    const date = TimeUuid.fromString(stateEventId).getDate()
    const timeBucket = format(startOfHour(utc(date)), "yyyyMMddHH0000")
    return this.replayObserver.observeCheckpoint(() => stateManager.setState!({ timeBucket, eventId: stateEventId }))
  }

  private async loop(): Promise<void> {
    do {
      this.ensureProcessLoop()
      const amountToFetch = this.options.bufferSize - this.buffer.length

      if (amountToFetch <= 0) {
        this.logger?.info("Buffer is full, waiting for space")
        await this.replayObserver.observeIdle("buffer_full", () => this.waitForBufferThreshold())
        continue
      }

      this.logger?.debug(`fetching ${amountToFetch} events from ${this.bufferState.timeBucket}(${this.nextCursor})`)

      const { events, nextCursor } = await this.replayObserver.observeFetch(() =>
        this.dataSource.getEvents(
          this.bufferState,
          amountToFetch,
          this.stopAtState?.eventId,
          this.nextCursor,
          this.options.includeSensitiveData,
        ),
      )

      if (!this.running) {
        break
      }

      this.logger?.debug(`fetched ${events.length} events`)

      this.pulledCount += events.length
      this.mainLoopRestartAttempts = 0
      this.addEventsToBuffer(events)
      this.nextCursor = nextCursor
      this.updateMetricsGauges()

      events.length && this.waiterEvents?.()

      this.bufferState.eventId = events[events.length - 1]?.eventId ?? this.bufferState.eventId

      if (
        this.stopAtState?.timeBucket &&
        this.bufferState.timeBucket >= this.stopAtState.timeBucket &&
        !events.length
      ) {
        this.logger?.info("Stopping at stopAt state")
        await this.waitForBufferEmpty()
        this.stop()
        break
      }

      if (!this.nextCursor) {
        const timeBucket = await this.dataSource.getNextTimeBucket(this.bufferState.timeBucket)
        if (!this.running) {
          break
        }
        if (timeBucket) {
          this.bufferState.timeBucket = timeBucket
        } else {
          const previousTimeBucket = this.bufferState.timeBucket
          this.bufferState.timeBucket = format(startOfHour(utc(new Date())), "yyyyMMddHH0000")
          if (previousTimeBucket === this.bufferState.timeBucket && !events.length) {
            this.isLive = true
            this.logger?.debug("Going live...")
            this.abortController = new AbortController()
            await this.replayObserver.observeIdle("no_events", () => this.notifier.wait(this.abortController!.signal))
          } else if (this.isLive) {
            await this.replayObserver.observeIdle(
              "no_events",
              () => new Promise((resolve) => setTimeout(resolve, 1000)),
            )
          }
        }
      }
    } while (this.running)

    if (this.restartTo) {
      try {
        await this.dataSource.getTimeBuckets(true)
        this.restartTo.timeBucket =
          (await this.dataSource.getClosestTimeBucket(this.restartTo.timeBucket)) ??
          format(startOfHour(utc(new Date())), "yyyyMMddHH0000")
        this.nextCursor = undefined
        this.bufferState = this.restartTo
        this.restartTo = undefined
        this.running = true
        // `stop(true)` cleared `running` and stopped the pulse emitter, and the
        // process loop exits whenever `running` is false. Only `start()` used
        // to bring them back, so a pump restarted while it was delivering kept
        // pulling events and never delivered or checkpointed again — it looked
        // alive from the outside. The old loop exits through its generation
        // guard before a replacement takes ownership.
        this.ensureProcessLoop()
        this.pulseEmitter?.start()
        return this.loop()
      } catch (error) {
        this.logger?.error("Failed to consume restartTo, dropping it", { error })
        this.restartTo = undefined
        return
      }
    }

    this.logger?.info("Data pump stopped")
  }

  // #region Puller

  public reserve(amount: number): Promise<FlowcoreEvent[]> {
    return this.reserveInternal(amount)
  }

  private async reserveInternal(amount: number, generation?: number): Promise<FlowcoreEvent[]> {
    if (!this.running || (generation !== undefined && generation !== this.processLoopGeneration)) {
      return []
    }
    const events: FlowcoreEvent[] = []
    const deliveryId = crypto.randomUUID()

    for (const event of this.buffer) {
      if (event.status === "open") {
        event.status = "reserved"
        event.deliveryId = deliveryId
        event.deliveryCount++
        this.updateReservedStats(event, 1)
        events.push(event.event)
        event.eventSizeBytes ??= textEncoder.encode(JSON.stringify(event.event)).byteLength
        this.incMetricsCounter("pulled", event.event.eventType, event.eventSizeBytes)
        if (events.length === amount) {
          break
        }
      }
    }

    if (!events.length) {
      await this.waitForEvents()
      return this.reserveInternal(amount, generation)
    }

    this.updateMetricsGauges()

    setTimeout(() => {
      void this.reOpen(
        events.map((event) => event.eventId),
        deliveryId,
      ).catch((error) => {
        this.logger?.error("Failed to reopen events after acknowledgement timeout", { error })
      })
    }, this.options.achknowledgeTimeoutMs)

    return events
  }

  public onFinalyFailed(handler: (events: FlowcoreEvent[]) => Promise<void> | void) {
    this.finallyFailedHandler = handler
  }

  public async acknowledge(eventIds: string[]) {
    if (!this.running) {
      return
    }
    const eventIdSet = new Set(eventIds)
    const checkpointEventId = this.replayObserver.observeAcknowledgement(() => {
      const lastEventInBuffer = this.buffer[this.buffer.length - 1]
      this.buffer = this.buffer.filter((event) => {
        if (eventIdSet.has(event.event.eventId)) {
          this.incMetricsCounter("acknowledged", event.event.eventType, 1)
          this.acknowledgedCount++
          this.removeFromBufferStats(event)
          return false
        }
        return true
      })

      if (this.buffer.length <= this.options.bufferSize - this.options.bufferThreshold) {
        this.waiterBufferThreshold?.()
      }

      return this.buffer.length ? undefined : lastEventInBuffer?.event.eventId
    })

    this.updateMetricsGauges()

    try {
      await this.updateState(checkpointEventId)
    } finally {
      this.notifyBufferEmpty()
    }
  }

  public async fail(eventIds: string[]) {
    if (!this.running || !eventIds.length) {
      return
    }
    const lastEventInBuffer = this.buffer[this.buffer.length - 1]
    const eventIdSet = new Set(eventIds)
    const failedEvents: FlowcoreEvent[] = []
    this.buffer = this.buffer.filter((event) => {
      if (eventIdSet.has(event.event.eventId)) {
        this.incMetricsCounter("failed", event.event.eventType, 1)
        this.failedCount++
        failedEvents.push(event.event)
        this.removeFromBufferStats(event)
        return false
      }
      return true
    })
    this.logger?.info(`Failed ${failedEvents.length} events`)

    if (this.buffer.length <= this.options.bufferSize - this.options.bufferThreshold) {
      this.waiterBufferThreshold?.()
    }

    this.updateMetricsGauges()

    try {
      await this.options.processor?.failedHandler?.(failedEvents)
      await this.updateState(this.buffer.length ? undefined : lastEventInBuffer?.event.eventId)
    } finally {
      this.notifyBufferEmpty()
    }
  }

  private async reOpen(eventIds: string[], deliveryId: string) {
    const eventIdSet = new Set(eventIds)
    let lastEvent: FlowcoreEvent | undefined
    const failedEvents: FlowcoreEvent[] = []
    const reopenedEvents: FlowcoreEvent[] = []
    this.buffer = this.buffer.filter((event) => {
      if (event.deliveryId !== deliveryId || !eventIdSet.has(event.event.eventId)) {
        return true
      }
      if (this.options.maxRedeliveryCount > -1 && event.deliveryCount > this.options.maxRedeliveryCount) {
        this.incMetricsCounter("failed", event.event.eventType, 1)
        this.failedCount++
        failedEvents.push(event.event)
        lastEvent = event.event
        this.removeFromBufferStats(event)
        return false
      }
      event.status = "open"
      event.deliveryId = undefined
      this.updateReservedStats(event, -1)
      reopenedEvents.push(event.event)
      return true
    })

    this.updateMetricsGauges()

    if (reopenedEvents.length) {
      this.logger?.info(`Reopened ${reopenedEvents.length} events`)
      await this.waiterEvents?.()
    }

    if (!failedEvents.length) {
      return
    }

    this.logger?.info(`Failed ${failedEvents.length} events`)

    if (this.buffer.length <= this.options.bufferSize - this.options.bufferThreshold) {
      this.waiterBufferThreshold?.()
    }

    try {
      const callbackResults = await Promise.allSettled([
        Promise.resolve().then(() => this.options.processor?.failedHandler?.(failedEvents)),
        Promise.resolve().then(() => this.finallyFailedHandler?.(failedEvents)),
      ])
      const callbackFailure = callbackResults.find(
        (result): result is PromiseRejectedResult => result.status === "rejected",
      )
      if (callbackFailure) {
        throw callbackFailure.reason
      }
      await this.updateState(this.buffer.length ? undefined : lastEvent?.eventId)
    } finally {
      this.notifyBufferEmpty()
    }
  }

  // #endregion

  // #region Pusher

  /**
   * Guarantee a live process loop whenever the pump is running with a
   * processor. Called from the fetch loop, so a delivery loop that exited for
   * any reason — most importantly a restart, which clears `running` while the
   * loop is mid-batch — comes back within one fetch iteration instead of
   * leaving a pump that pulls but never delivers.
   */
  private ensureProcessLoop(): void {
    if (!this.options.processor || !this.running || this.processLoopRunning) {
      return
    }
    this.startProcessLoop()
  }

  private startProcessLoop(): void {
    // Guard against a second loop: `restart()` asks for the loop back, but the
    // running one may only have been parked in `reserve()`. Two loops would
    // race over the same buffer.
    if (this.processLoopRunning) {
      return
    }
    const generation = this.processLoopGeneration
    this.processLoopRunning = true
    this.processLoop(generation)
      .then(() => {
        this.processLoopRunning = false
        // The loop exits as soon as `running` goes false. If the pump is
        // running again by the time we get here, a restart brought it back
        // while this loop was finishing its last batch — resume delivery.
        if (this.running && this.options.processor) {
          this.startProcessLoop()
        }
      })
      .catch((error) => {
        this.processLoopRunning = false
        this.logger?.error("Error in processor", { error })
        if (!this.isCurrentProcessLoop(generation)) return
        this.processLoopRestartAttempts++
        const delay = Math.min(1_000 * Math.pow(2, this.processLoopRestartAttempts - 1), 30_000)
        this.logger?.warn(`Restarting process loop in ${delay}ms (attempt ${this.processLoopRestartAttempts})`)
        setTimeout(() => {
          if (!this.isCurrentProcessLoop(generation)) return
          this.startProcessLoop()
        }, delay)
      })
  }

  private isCurrentProcessLoop(generation: number): boolean {
    return this.running && generation === this.processLoopGeneration
  }

  private async processLoop(generation: number) {
    while (this.isCurrentProcessLoop(generation)) {
      try {
        const events = await this.reserveInternal(this.options.processor?.concurrency ?? 1, generation)
        if (!this.isCurrentProcessLoop(generation)) return
        await this.replayObserver.observeHandler(events, async () => {
          await this.options.processor?.handler(events)
        })
        if (!this.isCurrentProcessLoop(generation)) return
        await this.acknowledge(events.map((event) => event.eventId))
        this.processLoopRestartAttempts = 0
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : "Unknown error"
        this.logger?.error(`Failed to process events: ${errorMessage}`)
      }
    }
  }

  // #endregion

  // #region Metrics

  private addEventsToBuffer(events: FlowcoreEvent[]): void {
    for (const event of events) {
      const item: FlowcoreDataPumpBufferItem = {
        event,
        status: "open",
        deliveryCount: 0,
        payloadSizeBytes: textEncoder.encode(JSON.stringify(event.payload)).byteLength,
      }
      this.buffer.push(item)
      this.bufferSizeBytes += item.payloadSizeBytes
      const stat = this.bufferStats.get(event.eventType)
      if (stat) {
        stat.eventCount++
        stat.eventSizeBytes += item.payloadSizeBytes
      }
    }
  }

  private updateReservedStats(item: FlowcoreDataPumpBufferItem, delta: 1 | -1): void {
    this.bufferReservedCount += delta
    const stat = this.bufferStats.get(item.event.eventType)
    if (stat) stat.eventReservedCount += delta
  }

  private removeFromBufferStats(item: FlowcoreDataPumpBufferItem): void {
    this.bufferSizeBytes -= item.payloadSizeBytes
    const stat = this.bufferStats.get(item.event.eventType)
    if (stat) {
      stat.eventCount--
      stat.eventSizeBytes -= item.payloadSizeBytes
    }
    if (item.status === "reserved") this.updateReservedStats(item, -1)
  }

  private resetBufferStats(): void {
    this.bufferReservedCount = 0
    this.bufferSizeBytes = 0
    for (const stat of this.bufferStats.values()) {
      stat.eventCount = 0
      stat.eventReservedCount = 0
      stat.eventSizeBytes = 0
    }
  }

  private updateMetricsGauges(synchronous = false): void {
    if (synchronous) {
      this.gaugePublicationScheduled = false
      this.publishMetricsGauges()
      return
    }
    if (this.gaugePublicationScheduled) return
    this.gaugePublicationScheduled = true
    queueMicrotask(() => {
      if (!this.gaugePublicationScheduled) return
      this.gaugePublicationScheduled = false
      this.publishMetricsGauges()
    })
  }

  private publishMetricsGauges(): void {
    for (const [eventType, stat] of this.bufferStats) {
      const labels = {
        tenant: this.dataSource.tenant,
        data_core: this.dataSource.dataCore,
        flow_type: this.dataSource.flowType,
        event_type: eventType,
      }
      metrics.bufferEventCountGauge.set(labels, stat.eventCount)
      metrics.bufferReservedEventCountGauge.set(labels, stat.eventReservedCount)
      metrics.bufferSizeBytesGauge.set(labels, stat.eventSizeBytes)
    }
  }

  private incMetricsCounter(name: "acknowledged" | "failed" | "pulled", eventType: string, value: number) {
    switch (name) {
      case "acknowledged":
        metrics.eventsAcknowledgedCounter.inc(
          {
            tenant: this.dataSource.tenant,
            data_core: this.dataSource.dataCore,
            flow_type: this.dataSource.flowType,
            event_type: eventType,
          },
          value,
        )
        break
      case "failed":
        metrics.eventsFailedCounter.inc(
          {
            tenant: this.dataSource.tenant,
            data_core: this.dataSource.dataCore,
            flow_type: this.dataSource.flowType,
            event_type: eventType,
          },
          value,
        )
        break
      case "pulled":
        metrics.eventsPulledSizeBytesCounter.inc(
          {
            tenant: this.dataSource.tenant,
            data_core: this.dataSource.dataCore,
            flow_type: this.dataSource.flowType,
            event_type: eventType,
          },
          value,
        )
        break
    }
  }

  // #endregion

  // #region Waiters

  private waiterEvents?: () => void
  private async waitForEvents() {
    const promise = new Promise<void>((resolve) => {
      this.waiterEvents = resolve
    })
    await this.replayObserver.observeIdle("waiting_for_events", () => promise)
  }

  private waiterBufferThreshold?: () => void
  private async waitForBufferThreshold() {
    const promise = new Promise<void>((resolve) => {
      this.waiterBufferThreshold = resolve
    })
    await promise
  }

  private waiterBufferEmpty?: () => void
  private notifyBufferEmpty(): void {
    if (!this.buffer.length) {
      this.waiterBufferEmpty?.()
    }
  }

  private async waitForBufferEmpty() {
    if (!this.buffer.length) {
      return
    }
    const promise = new Promise<void>((resolve) => {
      this.waiterBufferEmpty = resolve
    })
    await promise
  }

  // #endregion
}
