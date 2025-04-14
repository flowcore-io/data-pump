import { utc } from "@date-fns/utc"
import type { FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { format, startOfHour } from "date-fns"
import { FlowcoreDataSource } from "./data-source.ts"
import { metrics } from "./metrics.ts"
import { FlowcoreNotifier } from "./notifier.ts"
import type {
  FlowcoreDataPumpAuth,
  FlowcoreDataPumpDataSource,
  FlowcoreDataPumpProcessor,
  FlowcoreDataPumpState,
  FlowcoreDataPumpStateManager,
  FlowcoreLogger,
} from "./types.ts"

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
  processor?: FlowcoreDataPumpProcessor
  notifier?: FlowcoreDataPumpNotifierOptions
  logger?: FlowcoreLogger
  stopAt?: Date
  baseUrlOverride?: string
  noTranslation?: boolean
  directMode?: boolean
}

interface FlowcoreDataPumpInnerOptions {
  bufferSize: number
  bufferThreshold: number
  maxRedeliveryCount: number
  achknowledgeTimeoutMs: number
  processor?: FlowcoreDataPumpProcessor
  stopAt?: Date
}

interface FlowcoreDataPumpBufferItem {
  event: FlowcoreEvent
  status: "open" | "reserved"
  deliveryCount: number
  deliveryId?: string
}

export class FlowcoreDataPump {
  private running = false
  private restartTo?: FlowcoreDataPumpState
  private abortController?: AbortController
  private buffer: FlowcoreDataPumpBufferItem[] = []
  private bufferState: FlowcoreDataPumpState
  private stopAtState?: FlowcoreDataPumpState
  private isLive = false

  private constructor(
    private readonly dataSource: FlowcoreDataSource,
    private readonly notifier: FlowcoreNotifier,
    private stateManager: FlowcoreDataPumpStateManager,
    private readonly options: FlowcoreDataPumpInnerOptions,
    private readonly logger?: FlowcoreLogger,
  ) {
    this.bufferState = {
      timeBucket: format(startOfHour(utc(new Date())), "yyyyMMddHH0000"),
      eventId: TimeUuid.now().toString(),
    }
  }

  public get isRunning(): boolean {
    return this.running
  }

  public static create(options: FlowcoreDataPumpOptions, dataSourceOverride?: FlowcoreDataSource): FlowcoreDataPump {
    const dataSource = dataSourceOverride ?? new FlowcoreDataSource({
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
    return new FlowcoreDataPump(
      dataSource,
      notifier,
      options.stateManager,
      {
        bufferSize: options.bufferSize ?? 1000,
        bufferThreshold: Math.ceil((options.bufferSize ?? 1000) * 0.1),
        maxRedeliveryCount: options.maxRedeliveryCount ?? 3,
        achknowledgeTimeoutMs: options.achknowledgeTimeoutMs ?? 5_000,
        processor: options.processor,
        stopAt: options.stopAt,
      },
      options.logger,
    )
  }

  public async start(callback?: (error?: Error) => void): Promise<void> {
    this.isLive = false
    if (this.running) {
      throw new Error("Data pump already running")
    }
    this.running = true
    this.updateMetricsGauges()
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
        timeBucket: (await this.dataSource.getClosestTimeBucket(
          format(startOfHour(utc(this.options.stopAt)), "yyyyMMddHH0000"),
          true,
        )) ?? format(startOfHour(utc(new Date())), "yyyyMMddHH0000"),
        eventId: TimeUuid.fromDate(this.options.stopAt).toString(),
      }
    }

    if (this.options.processor) {
      this.processLoop().catch((error) => {
        this.logger?.error("Error in processor", { error })
        this.stop()
      })
    }

    if (!callback) {
      return this.loop()
    }

    void this.loop()
      .then(() => callback())
      .catch((error) => callback(error))
  }

  public restart(state: FlowcoreDataPumpState, stopAt?: Date | null): void {
    this.restartTo = state
    if (stopAt !== undefined) {
      this.options.stopAt = stopAt ?? undefined
    }
    this.stop(true)
  }

  public stop(isRestart = false): void {
    this.running = false
    this.buffer = []
    this.updateMetricsGauges()
    this.abortController?.abort()
    this.waiterBufferThreshold?.()
    if (!isRestart) {
      this.waiterEvents?.()
    }
  }

  private updateState(eventId?: string) {
    if (!this.stateManager.setState) {
      return
    }
    const stateEventId = eventId ?? this.buffer[0]?.event.eventId
    if (!stateEventId) {
      return
    }
    const date = TimeUuid.fromString(stateEventId).getDate()
    const timeBucket = format(startOfHour(utc(date)), "yyyyMMddHH0000")
    return this.stateManager.setState?.({ timeBucket, eventId: stateEventId })
  }

  private async loop(): Promise<void> {
    do {
      const amountToFetch = this.options.bufferSize - this.buffer.length

      if (amountToFetch <= 0) {
        this.logger?.info("Buffer is full, waiting for space")
        await this.waitForBufferThreshold()
        continue
      }

      this.logger?.debug(
        `fetching ${amountToFetch} events from ${this.bufferState.timeBucket}(${this.bufferState.eventId})`,
      )
      const events = await this.dataSource.getEvents(this.bufferState, amountToFetch, this.stopAtState?.eventId)
      if (!this.running) {
        break
      }
      this.logger?.debug(`fetched ${events.length} events`)

      this.buffer.push(...events.map((event) => ({ event, status: "open" as const, deliveryCount: 0 })))
      this.updateMetricsGauges()

      events.length && this.waiterEvents?.()

      this.bufferState.eventId = events[events.length - 1]?.eventId ?? this.bufferState.eventId

      if (
        this.stopAtState?.timeBucket && this.bufferState.timeBucket >= this.stopAtState.timeBucket && !events.length
      ) {
        this.logger?.info("Stopping at stopAt state")
        await this.waitForBufferEmpty()
        this.stop()
        break
      }

      if (events.length !== amountToFetch) {
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
            await this.notifier.wait(this.abortController.signal)
          } else if (this.isLive) {
            await new Promise((resolve) => setTimeout(resolve, 1000))
          }
        }
      }
    } while (this.running)

    if (this.restartTo) {
      await this.dataSource.getTimeBuckets(true)
      this.restartTo.timeBucket = (await this.dataSource.getClosestTimeBucket(this.restartTo.timeBucket)) ??
        format(startOfHour(utc(new Date())), "yyyyMMddHH0000")
      this.bufferState = this.restartTo
      this.restartTo = undefined
      this.running = true
      return this.loop()
    }

    this.logger?.info("Data pump stopped")
  }

  // #region Puller

  public async reserve(amount: number): Promise<FlowcoreEvent[]> {
    if (!this.running) {
      return []
    }
    const events: FlowcoreEvent[] = []
    const deliveryId = crypto.randomUUID()
    for (const event of this.buffer) {
      if (event.status === "open") {
        event.status = "reserved"
        event.deliveryId = deliveryId
        event.deliveryCount++
        events.push(event.event)
        this.incMetricsCounter("pulled", event.event.eventType, JSON.stringify(event.event).length)
        if (events.length === amount) {
          break
        }
      }
    }

    if (!events.length) {
      await this.waitForEvents()
      return this.reserve(amount)
    }

    this.updateMetricsGauges()

    setTimeout(() => {
      this.reOpen(
        events.map((event) => event.eventId),
        deliveryId,
      )
    }, this.options.achknowledgeTimeoutMs)

    return events
  }

  public async acknowledge(eventIds: string[]) {
    if (!this.running) {
      return
    }
    const lastEventInBuffer = this.buffer[this.buffer.length - 1]
    this.buffer = this.buffer.filter((event) => {
      if (eventIds.includes(event.event.eventId)) {
        this.incMetricsCounter("acknowledged", event.event.eventType, 1)
        return false
      }
      return true
    })

    if (this.buffer.length <= this.options.bufferSize - this.options.bufferThreshold) {
      this.waiterBufferThreshold?.()
    }

    await this.updateState(this.buffer.length ? undefined : lastEventInBuffer?.event.eventId)

    this.updateMetricsGauges()

    if (!this.buffer.length) {
      this.waiterBufferEmpty?.()
    }
  }

  public async fail(eventIds: string[]) {
    if (!this.running || !eventIds.length) {
      return
    }
    const lastEventInBuffer = this.buffer[this.buffer.length - 1]
    const failedEvents: FlowcoreEvent[] = []
    this.buffer = this.buffer.filter((event) => {
      if (eventIds.includes(event.event.eventId)) {
        this.incMetricsCounter("failed", event.event.eventType, 1)
        failedEvents.push(event.event)
        return false
      }
      return true
    })
    this.logger?.info(`Failed ${failedEvents.length} events`)
    void this.options.processor?.failedHandler?.(failedEvents)

    if (this.buffer.length <= this.options.bufferSize - this.options.bufferThreshold) {
      this.waiterBufferThreshold?.()
    }

    await this.updateState(this.buffer.length ? undefined : lastEventInBuffer?.event.eventId)

    if (!this.buffer.length) {
      this.waiterBufferEmpty?.()
    }
  }

  private async reOpen(eventIds: string[], deliveryId: string) {
    let lastEvent: FlowcoreEvent | undefined
    const failedEvents: FlowcoreEvent[] = []
    const reopenedEvents: FlowcoreEvent[] = []
    this.buffer = this.buffer.filter((event) => {
      if (event.deliveryId !== deliveryId || !eventIds.includes(event.event.eventId)) {
        return true
      }
      if (this.options.maxRedeliveryCount > -1 && event.deliveryCount > this.options.maxRedeliveryCount) {
        this.incMetricsCounter("failed", event.event.eventType, 1)
        failedEvents.push(event.event)
        lastEvent = event.event
        return false
      }
      event.status = "open"
      event.deliveryId = undefined
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
    void this.options.processor?.failedHandler?.(failedEvents)

    if (this.buffer.length <= this.options.bufferSize - this.options.bufferThreshold) {
      this.waiterBufferThreshold?.()
    }

    await this.updateState(this.buffer.length ? undefined : lastEvent?.eventId)

    if (!this.buffer.length) {
      this.waiterBufferEmpty?.()
    }
  }

  // #endregion

  // #region Pusher

  private async processLoop() {
    while (this.running) {
      try {
        const events = await this.reserve(this.options.processor?.concurrency ?? 1)
        await this.options.processor?.handler(events)
        await this.acknowledge(events.map((event) => event.eventId))
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : "Unknown error"
        this.logger?.error(`Failed to process events: ${errorMessage}`)
      }
    }
  }

  // #endregion

  // #region Metrics

  private updateMetricsGauges() {
    const stats = new Map<
      string,
      {
        eventCount: number
        eventReservedCount: number
        eventSizeBytes: number
      }
    >()
    for (const eventType of this.dataSource.eventTypes) {
      stats.set(eventType, {
        eventCount: 0,
        eventReservedCount: 0,
        eventSizeBytes: 0,
      })
    }

    for (const item of this.buffer) {
      const stat = stats.get(item.event.eventType)
      if (!stat) {
        continue
      }
      stat.eventCount++
      if (item.status === "reserved") {
        stat.eventReservedCount++
      }
      stat.eventSizeBytes += JSON.stringify(item.event.payload).length
    }

    for (const [eventType, stat] of stats) {
      metrics.bufferEventCountGauge.set(
        {
          tenant: this.dataSource.tenant,
          data_core: this.dataSource.dataCore,
          flow_type: this.dataSource.flowType,
          event_type: eventType,
        },
        stat.eventCount,
      )
      metrics.bufferReservedEventCountGauge.set(
        {
          tenant: this.dataSource.tenant,
          data_core: this.dataSource.dataCore,
          flow_type: this.dataSource.flowType,
          event_type: eventType,
        },
        stat.eventReservedCount,
      )
      metrics.bufferSizeBytesGauge.set(
        {
          tenant: this.dataSource.tenant,
          data_core: this.dataSource.dataCore,
          flow_type: this.dataSource.flowType,
          event_type: eventType,
        },
        stat.eventSizeBytes,
      )
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
    await promise
  }

  private waiterBufferThreshold?: () => void
  private async waitForBufferThreshold() {
    const promise = new Promise<void>((resolve) => {
      this.waiterBufferThreshold = resolve
    })
    await promise
  }

  private waiterBufferEmpty?: () => void
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
