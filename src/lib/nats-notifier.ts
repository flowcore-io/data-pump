import * as Nats from "nats"
import type { DataPumpNotifier } from "./data-pump.ts"

interface NatsClientOptions {
  servers: string[]
  timeoutMs?: number
}

export class NatsNotifier {
  private nats: Nats.NatsConnection | undefined
  private timeoutMs: number
  constructor(private readonly options: NatsClientOptions) {
    this.timeoutMs = options.timeoutMs ?? 20_000
  }

  private async start() {
    if (this.nats) {
      return
    }
    this.nats = await Nats.connect({ servers: this.options.servers })
  }

  private async stop() {
    if (this.nats) {
      await this.nats.close()
      this.nats = undefined
    }
  }

  private awaitMessage(subjects: string[], timeoutMs: number): [Promise<void>, AbortController] {
    const abortController = new AbortController()

    const promise = new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        for (const subscription of subscriptions) {
          subscription.unsubscribe()
        }
        resolve()
      }, timeoutMs)

      const subscriptions: Nats.Subscription[] = []
      for (const subject of subjects) {
        subscriptions.push(
          this.getSubscription(subject, (_message) => {
            for (const subscription of subscriptions) {
              clearTimeout(timer)
              subscription.unsubscribe()
            }
            resolve()
          }),
        )
      }

      abortController.signal.addEventListener("abort", () => {
        for (const subscription of subscriptions) {
          subscription.unsubscribe()
          clearTimeout(timer)
        }
        resolve()
      })
    })

    return [promise, abortController]
  }

  private getSubscription(
    subject: string,
    onMessage: (message: Nats.Msg, subscription: Nats.Subscription) => Promise<void> | void,
  ) {
    if (!this.nats) {
      throw new Error("NatsClient is not started")
    }
    const subscription = this.nats.subscribe(subject)
    ;(async () => {
      for await (const message of subscription) {
        await onMessage(message, subscription)
      }
    })().catch(console.error)
    return subscription
  }

  public getNotifier(): DataPumpNotifier {
    return {
      wait: async (dataSource, signal) => {
        await this.start()
        const topics = dataSource.eventTypes.map(
          (eventType) => `stored.event.notify.0.${dataSource.dataCoreId}.${dataSource.flowType}.${eventType}`,
        )
        const [promise, abortController] = this.awaitMessage(topics, this.timeoutMs)
        signal?.addEventListener("abort", () => abortController.abort())
        await promise
      },
    }
  }
}
