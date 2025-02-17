import {
  DataCoreFetchCommand,
  EventsFetchCommand,
  EventsFetchTimeBucketsByNamesCommand,
  type FlowcoreClient,
  type FlowcoreEvent,
  TenantFetchCommand,
} from "@flowcore/sdk"
import { TimeUuid } from "./time-uuid.ts"
import { format, startOfHour } from "date-fns"
import { utc } from "@date-fns/utc"

enum FlowcoreBufferEventStatus {
  OPEN = "open",
  DELIVERED = "delivered",
}

interface FlowcoreBufferEvent {
  event: FlowcoreEvent
  status: "open" | "delivered"
  deliveryCount: number
  deliveryId: string
}

export interface Logger {
  debug(message: string, ...meta: unknown[]): void
  info(message: string, ...meta: unknown[]): void
  warn(message: string, ...meta: unknown[]): void
  error(message: string | Error, ...meta: unknown[]): void
}

interface DataPumpOptions {
  flowcoreClient: FlowcoreClient
  stateManager: DataPumpStateManager
  logger?: Logger
  notifier: DataPumpNotifier
  dataSource: {
    tenant: string
    dataCore: string
    flowType: string
    eventTypes: string[]
  }
  buffer: {
    size: number
    threshold: number
    maxRedeliveryCount: number
    achknowledgeTimeoutMs: number
  }
  processor?: {
    concurrency: number
    onEvents: (events: FlowcoreEvent[]) => Promise<boolean> | boolean
    onFailedEvents?: (events: FlowcoreEvent[]) => Promise<void> | void
  }
}

export interface DataPumpState {
  timeBucket: string
  eventId?: string
}

export interface DataPumpStateManager {
  getState(): Promise<DataPumpState | null> | DataPumpState | null
  setState(state: DataPumpState): Promise<void> | void
}

export interface DataPumpNotifier {
  wait(dataSource: DataPumpOptions["dataSource"] & { dataCoreId: string }, signal?: AbortSignal): Promise<void>
}

export class DataPump {
  private logger?: Logger
  private state: DataPumpState
  private _timeBuckets: string[] = []
  private buffer: FlowcoreBufferEvent[] = []
  private bufferWaiter: ((stopping?: boolean) => void) | null = null
  private waiter: ((stopping?: boolean) => void) | null = null
  private running = false
  private notifierAbortController?: AbortController
  private dataCoreId?: string

  constructor(private options: DataPumpOptions) {
    this.state = {
      timeBucket: this.getNowTimeBucket(),
    }
    this.logger = options.logger ?? undefined
  }

  public async start() {
    if (this.running) {
      return
    }

    await this.updateTimeBuckets()

    const newState = await this.options.stateManager.getState()
    if (!newState) {
      this.state.timeBucket = this.getClosestTimeBucket(this.getNowTimeBucket())
      this.state.eventId = TimeUuid.fromNow().toString()
    } else {
      this.state.timeBucket = this.getClosestTimeBucket(newState.timeBucket)
      this.state.eventId = newState.eventId
    }

    this.running = true

    this.logger?.debug("Starting event buffer loop")
    this.eventBufferLoop()
      .then(() => this.logger?.debug("Event buffer loop stopped"))
      .catch((error) => this.logger?.error("Error in event buffer loop", { error }))
      .finally(() => this.stop())

    if (this.options.processor) {
      this.logger?.debug("Starting processor")
      this.proceccor()
        .then(() => this.logger?.debug("Processor stopped"))
        .catch((error) => this.logger?.error("Error in processor", { error }))
        .finally(() => this.stop())
    }
  }

  public stop() {
    if (!this.running) {
      return
    }
    this.logger?.debug("Stopping data pump")
    this.running = false
    this.notifierAbortController?.abort("stopping")
    this.bufferWaiter?.(true)
    this.waiter?.(true)
  }

  private async proceccor() {
    if (!this.options.processor) {
      throw new Error("Processor not set")
    }
    while (this.running) {
      try {
        const events = await this.pullInner(this.options.processor.concurrency)
        if (!this.running) {
          break
        }
        const success = await this.options.processor.onEvents(events)
        if (success) {
          await this.acknowledgeInner(events.map((event) => event.eventId))
        }
      } catch (error) {
        this.logger?.error("Error in processor", { error })
        await this.stop()
      }
    }
  }

  private async eventBufferLoop(): Promise<void> {
    // Stop buffer loop if we are not running
    if (!this.running) {
      return
    }

    // Buffer is full, wait for some space
    if (this.buffer.length >= this.options.buffer.size - this.options.buffer.threshold && this.running) {
      this.logger?.debug("Buffer is full, waiting for some space")
      await this.waitForBufferThreshold()
    }

    // Stop buffer loop if we are not running
    if (!this.running) {
      return
    }

    // How many events should we fetch
    const eventsToFetch = this.options.buffer.size - this.buffer.length

    this.logger?.debug(
      `fetching ${eventsToFetch} events from ${this.state.timeBucket} @ ${this.state.eventId ?? "-"}`,
    )

    const timeStart = Date.now()

    const command = new EventsFetchCommand({
      tenant: this.options.dataSource.tenant,
      dataCoreId: await this.getDataCoreId(),
      flowType: this.options.dataSource.flowType,
      eventTypes: this.options.dataSource.eventTypes,
      timeBucket: this.state.timeBucket,
      afterEventId: this.state.eventId,
      pageSize: eventsToFetch,
    })

    const result = await this.options.flowcoreClient.execute(command)

    const time = Date.now() - timeStart
    // Stop buffer loop if we are not running
    if (!this.running) {
      return
    }

    const events = result.events.map((event) => ({
      event,
      status: FlowcoreBufferEventStatus.OPEN,
      deliveryCount: 0,
      deliveryTime: 0,
      deliveryId: "",
    }))

    this.buffer.push(...events)

    // Check if we have a waiter and if we have events, notify the waiter
    events.length && this.waiter?.()

    this.logger?.debug(
      `Got ${result.events.length} events from ${this.state.timeBucket} @ ${
        this.state.eventId ?? "-"
      } in ${time}ms. Buffer size ${this.buffer.length}`,
    )

    // Update buffer state
    this.state.eventId = result.events[result.events.length - 1]?.eventId ?? this.state.eventId

    // If we don't have a next cursor move to next time bucket
    if (!result.nextCursor) {
      // Get next time bucket
      const nextTimeBucketIndex = this.timeBuckets.indexOf(this.state.timeBucket) + 1
      // There is no next time bucket
      if (!this.timeBuckets[nextTimeBucketIndex]) {
        if (result.events.length) {
          // If we got events, we can try to get more one more time
          this.logger?.debug("Try to get more events")
        } else {
          // If we didn't get events, we wait for notifier
          await this.waitForNotifier()
        }
      } else {
        // Set next time bucket
        this.state.timeBucket = this.timeBuckets[nextTimeBucketIndex]
      }
    }

    return this.eventBufferLoop()
  }

  private async reOpen(eventIds: string[], deliveryId: string) {
    if (!eventIds.length) {
      return
    }
    const failedEvents: FlowcoreEvent[] = []
    const reopenedEvents: FlowcoreEvent[] = []
    let lastEvent: FlowcoreEvent | undefined
    this.buffer = this.buffer.filter((event) => {
      if (
        event.deliveryId === deliveryId && this.options.buffer.maxRedeliveryCount > -1 &&
        event.deliveryCount > this.options.buffer.maxRedeliveryCount
      ) {
        failedEvents.push(event.event)
        lastEvent = event.event
        return false
      }
      if (event.deliveryId === deliveryId && eventIds.includes(event.event.eventId)) {
        event.status = FlowcoreBufferEventStatus.OPEN
        event.deliveryId = ""
        reopenedEvents.push(event.event)
      }
      return true
    })

    if (reopenedEvents.length) {
      this.waiter?.()
    }

    if (this.bufferWaiter && this.buffer.length <= this.options.buffer.size - this.options.buffer.threshold) {
      this.bufferWaiter()
    }

    if (failedEvents.length) {
      await this.options.processor?.onFailedEvents?.(failedEvents)
      await this.updateState(lastEvent?.eventId)
    }
  }

  private updateState(eventId?: string) {
    const firstEvent = this.buffer[0]
    if (!firstEvent) {
      if (eventId) {
        const timeUuid = TimeUuid.fromString(eventId)
        return this.options.stateManager.setState({ timeBucket: timeUuid.getTimeBucket(), eventId })
      }
      return
    }
    const timeUuid = TimeUuid.fromString(firstEvent.event.eventId)
    const timeBucket = firstEvent.event.timeBucket
    return this.options.stateManager.setState({ timeBucket, eventId: timeUuid.getBefore().toString() })
  }

  private get timeBuckets() {
    const timeBucketNow = this.getNowTimeBucket()
    if (this._timeBuckets[this._timeBuckets.length - 1] !== timeBucketNow) {
      return [...this._timeBuckets, timeBucketNow]
    }
    return this._timeBuckets
  }

  private getClosestTimeBucket(timeBucket: string) {
    if (!timeBucket.match(/^\d{14}$/)) {
      throw new Error(`Invalid timebucket: ${timeBucket}`)
    }
    return (
      this.timeBuckets.find((t) => Number.parseFloat(t) >= Number.parseFloat(timeBucket)) ??
        this.timeBuckets[this.timeBuckets.length - 1]
    )
  }

  private async updateTimeBuckets() {
    this._timeBuckets = []
    let first = true
    let cursor: number | undefined
    while (cursor !== undefined || first) {
      first = false
      const command = new EventsFetchTimeBucketsByNamesCommand({
        tenant: this.options.dataSource.tenant,
        dataCoreId: await this.getDataCoreId(),
        flowType: this.options.dataSource.flowType,
        eventTypes: this.options.dataSource.eventTypes,
        pageSize: 10_000,
      })
      // sdkCommandCount.labels("EventsFetchTimeBucketsByNamesCommand").inc(1)
      const result = await this.options.flowcoreClient.execute(command)
      this._timeBuckets.push(...result.timeBuckets)
      cursor = result.nextCursor
    }
  }

  private async pullInner(amount: number): Promise<FlowcoreEvent[]> {
    if (!this.running) {
      throw new Error("Data pump not running")
    }

    const deliveryId = crypto.randomUUID()
    const events: FlowcoreEvent[] = []

    for (const event of this.buffer) {
      if (event.status === FlowcoreBufferEventStatus.OPEN) {
        event.status = FlowcoreBufferEventStatus.DELIVERED
        event.deliveryCount++
        event.deliveryId = deliveryId
        events.push(event.event)
        if (events.length >= amount) {
          break
        }
      }
    }

    if (!events.length) {
      await this.waitForEvents()
      if (!this.running) {
        return []
      }
      return this.pullInner(amount)
    }

    setTimeout(() => {
      this.reOpen(
        events.map((event) => event.eventId),
        deliveryId,
      )
    }, this.options.buffer.achknowledgeTimeoutMs)

    return events
  }

  private async acknowledgeInner(eventIds: string[]) {
    if (!eventIds.length) {
      return
    }
    // this.logger.info("Acknowledging events", { events: eventIds.length })
    const lastEventInBuffer = this.buffer[this.buffer.length - 1]
    this.buffer = this.buffer.filter((event) => {
      if (eventIds.includes(event.event.eventId)) {
        return false
      }
      return true
    })
    if (this.bufferWaiter && this.buffer.length <= this.options.buffer.size - this.options.buffer.threshold) {
      this.bufferWaiter()
    }
    if (!this.buffer.length) {
      await this.updateState(lastEventInBuffer?.event.eventId)
    } else {
      await this.updateState()
    }
  }

  private async waitForNotifier() {
    this.logger?.debug("No more events, waiting for notifier...")
    this.notifierAbortController = new AbortController()
    await this.options.notifier.wait(
      { ...this.options.dataSource, dataCoreId: await this.getDataCoreId() },
      this.notifierAbortController.signal,
    )
    this.notifierAbortController = undefined
  }

  private async waitForEvents() {
    this.logger?.debug("No events, waiting for events")
    await new Promise<void>((resolve) => {
      this.waiter = (stopping?: boolean) => {
        if (!stopping) {
          this.logger?.debug("Notifying waiter that we have events")
        }
        resolve()
      }
    })
    this.waiter = null
  }

  private async waitForBufferThreshold() {
    if (this.buffer.length < this.options.buffer.size - this.options.buffer.threshold) {
      return
    }
    await new Promise<void>((resolve) => {
      this.bufferWaiter = (stopping?: boolean) => {
        if (!stopping) {
          this.logger?.debug("Notifying buffer waiter that we are under threshold")
        }
        resolve()
      }
    })
    this.bufferWaiter = null
  }

  private getNowTimeBucket() {
    return format(startOfHour(utc(new Date())), "yyyyMMddHH0000")
  }

  private async getDataCoreId() {
    if (this.dataCoreId) {
      return this.dataCoreId
    }
    const tenant = await this.options.flowcoreClient.execute(
      new TenantFetchCommand({
        tenant: this.options.dataSource.tenant,
      }),
    )
    const dataCore = await this.options.flowcoreClient.execute(
      new DataCoreFetchCommand({
        dataCore: this.options.dataSource.dataCore,
        tenantId: tenant.id,
      }),
    )
    this.dataCoreId = dataCore.id
    return this.dataCoreId
  }

  public pull(amount: number): Promise<FlowcoreEvent[]> {
    if (this.options.processor) {
      throw new Error("Pull is not supported when processor is set")
    }
    return this.pullInner(amount)
  }

  public acknowledge(eventIds: string[]): Promise<void> {
    if (this.options.processor) {
      throw new Error("Acknowledge is not supported when processor is set")
    }
    return this.acknowledgeInner(eventIds)
  }
}
