import { NotificationClient, type NotificationEvent } from "@flowcore/sdk"
import * as Nats from "nats"
import { Subject } from "rxjs"
import { FlowcoreDataSource } from "./data-source.ts"
import type { FlowcoreDataPumpAuth, FlowcoreDataPumpDataSource, FlowcoreLogger } from "./types.ts"

const DEFAULT_TIMEOUT_MS = 20_000

export interface FlowcoreNotifierOptions {
  auth: FlowcoreDataPumpAuth
  dataSource: FlowcoreDataPumpDataSource
  natsServers?: string[]
  timeoutMs?: number
  logger?: FlowcoreLogger
}

export class FlowcoreNotifier {
  private dataSource: FlowcoreDataSource
  private nats?: Nats.NatsConnection
  private subject?: Subject<NotificationEvent>
  private notificationClient?: NotificationClient
  private eventResolver?: () => void
  // deno-lint-ignore no-explicit-any
  private timer?: any

  constructor(private readonly options: FlowcoreNotifierOptions) {
    this.dataSource = new FlowcoreDataSource({
      auth: this.options.auth,
      dataSource: this.options.dataSource,
    })
  }

  public wait(signal?: AbortSignal) {
    if (this.options.natsServers) {
      return this.waitNats()
    }
    return this.waitWebSocket(signal)
  }

  private async waitNats(signal?: AbortSignal) {
    this.options.logger?.info("Waiting for nats")
    if (!this.nats) {
      this.nats = await Nats.connect({ servers: this.options.natsServers })
    }
    const dataCoreId = await this.dataSource.getDataCoreId()
    const topics = this.dataSource.eventTypes.map(
      (eventType) => `stored.event.notify.0.${dataCoreId}.${this.dataSource.flowType}.${eventType}`,
    )

    const promise = new Promise<void>((resolve) => {
      this.eventResolver = resolve
    })

    const subscriptions: Nats.Subscription[] = []
    for (const topic of topics) {
      subscriptions.push(
        this.nats.subscribe(topic, {
          callback: () => {
            this.options.logger?.debug(`Received event from nats: ${topic}`)
            this.eventResolver?.()
          },
        }),
      )
    }

    clearTimeout(this.timer)
    this.timer = setTimeout(() => this.eventResolver?.(), this.options.timeoutMs ?? DEFAULT_TIMEOUT_MS)
    signal?.addEventListener("abort", () => this.eventResolver?.())

    await promise
    this.options.logger?.debug("Unsubscribing from nats")
    for (const subscription of subscriptions) {
      subscription.unsubscribe()
    }
    this.nats?.close()
    this.nats = undefined
  }

  private onWebSocketEvent(event: NotificationEvent) {
    if (this.dataSource.eventTypes.includes(event.data.eventType)) {
      this.eventResolver?.()
    }
  }

  private async waitWebSocket(signal?: AbortSignal) {
    this.options.logger?.debug("Waiting for web socket")
    this.subject = new Subject<NotificationEvent>()
    this.subject.subscribe({
      next: this.onWebSocketEvent.bind(this),
      error: (error: Error) => this.options.logger?.error("Notification stream error:", { error }),
    })

    this.notificationClient = new NotificationClient(
      this.subject,
      this.webSocketAuth(),
      {
        tenant: this.dataSource.tenant,
        dataCore: this.dataSource.dataCore,
        flowType: this.dataSource.flowType,
      },
      {
        logger: this.options.logger,
        reconnectInterval: 1000,
      },
    )

    await this.notificationClient.connect()

    const promise = new Promise<void>((resolve) => {
      this.eventResolver = resolve
    })
    clearTimeout(this.timer)
    this.timer = setTimeout(() => this.eventResolver?.(), this.options.timeoutMs ?? DEFAULT_TIMEOUT_MS)
    signal?.addEventListener("abort", () => this.eventResolver?.())
    await promise
    this.eventResolver = undefined
    this.notificationClient.disconnect()
  }

  private webSocketAuth() {
    if ("apiKey" in this.options.auth) {
      return {
        apiKey: this.options.auth.apiKey,
        apiKeyId: this.options.auth.apiKeyId,
      }
    }
    const getBearerToken = this.options.auth.getBearerToken
    return {
      oidcClient: {
        getToken: async () => ({
          accessToken: await getBearerToken(),
        }),
      },
    }
  }
}
