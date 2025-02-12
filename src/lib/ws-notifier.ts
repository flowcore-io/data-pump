import type { DataPumpNotifier, Logger } from "./data-pump.ts"
import { Subject } from "rxjs"
import { NotificationClient, type NotificationEvent } from "@flowcore/sdk"
import OidcClient from "@flowcore/sdk-oidc-client"

export class WebSocketNotifier {
  private readonly authClient: OidcClient.OidcClient
  private subject?: Subject<NotificationEvent>
  private notificationClient?: NotificationClient
  private dataSource: {
    tenant: string
    dataCore: string
    flowType: string
    eventTypes: string[]
  }
  private eventResolver?: () => void
  private logger?: Logger
  private timeoutMs: number

  constructor(options: {
    auth: {
      clientId: string
      clientSecret: string
      authUrl: string
    }
    dataSource: {
      tenant: string
      dataCore: string
      flowType: string
      eventTypes: string[]
    }
    logger?: Logger
    timeoutMs?: number
  }) {
    this.authClient = new OidcClient.OidcClient(
      options.auth.clientId,
      options.auth.clientSecret,
      options.auth.authUrl,
    )
    this.dataSource = options.dataSource
    this.logger = options.logger
    this.timeoutMs = options.timeoutMs ?? 20_000
  }

  private onEvent(event: NotificationEvent) {
    if (this.dataSource.eventTypes.includes(event.data.eventType)) {
      this.eventResolver?.()
    }
  }

  public getNotifier(): DataPumpNotifier {
    return {
      wait: async (_, signal) => {
        this.subject = new Subject<NotificationEvent>()
        this.subject.subscribe({
          next: this.onEvent.bind(this),
          error: (error) => this.logger?.error("Notification stream error:", error),
          complete: () => this.logger?.info("Notification stream completed"),
        })
        this.notificationClient = new NotificationClient(
          this.subject,
          this.authClient,
          {
            tenant: this.dataSource.tenant,
            dataCore: this.dataSource.dataCore,
            flowType: this.dataSource.flowType,
          },
          {
            logger: this.logger,
            reconnectInterval: 1000,
          },
        )
        await this.notificationClient.connect()
        const promise = new Promise<void>((resolve) => {
          this.eventResolver = resolve
        })
        setTimeout(() => this.eventResolver?.(), this.timeoutMs)
        signal?.addEventListener("abort", () => this.eventResolver?.())
        await promise
        this.eventResolver = undefined
        this.notificationClient.disconnect()
      },
    }
  }
}
