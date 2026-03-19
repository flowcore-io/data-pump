import type { FlowcoreEvent } from "@flowcore/sdk"
import * as Nats from "nats"
import type { FlowcoreLogger } from "./types.ts"

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000

export interface NatsDistributionRequest {
  deliveryId: string
  events: FlowcoreEvent[]
}

export interface NatsDistributionReply {
  status: "ack" | "fail"
  error?: string
}

export class NatsDistributionLeader {
  private readonly codec = Nats.JSONCodec<NatsDistributionRequest | NatsDistributionReply>()

  constructor(
    private readonly connection: Nats.NatsConnection,
    private readonly subject: string,
    private readonly logger?: FlowcoreLogger,
    private readonly timeoutMs: number = DEFAULT_REQUEST_TIMEOUT_MS,
  ) {}

  async distribute(events: FlowcoreEvent[]): Promise<void> {
    const deliveryId = crypto.randomUUID()
    const request: NatsDistributionRequest = { deliveryId, events }

    this.logger?.debug("Distributing events via NATS", { deliveryId, count: events.length, subject: this.subject })

    const response = await this.connection.request(
      this.subject,
      this.codec.encode(request),
      { timeout: this.timeoutMs },
    )

    const reply = this.codec.decode(response.data) as NatsDistributionReply
    if (reply.status === "fail") {
      throw new Error(`Worker failed to process events: ${reply.error ?? "unknown error"}`)
    }
  }
}

export class NatsDistributionWorker {
  private subscription?: Nats.Subscription
  private readonly codec = Nats.JSONCodec<NatsDistributionRequest | NatsDistributionReply>()
  private readonly queueGroup = "data-pump-workers"

  constructor(
    private readonly connection: Nats.NatsConnection,
    private readonly subject: string,
    private readonly handler: (events: FlowcoreEvent[]) => Promise<void>,
    private readonly logger?: FlowcoreLogger,
  ) {}

  start(): void {
    this.logger?.info("Starting NATS distribution worker", { subject: this.subject, queue: this.queueGroup })

    this.subscription = this.connection.subscribe(this.subject, {
      queue: this.queueGroup,
      callback: (_err, msg) => {
        void this.handleMessage(msg)
      },
    })
  }

  stop(): void {
    if (this.subscription) {
      this.logger?.debug("Stopping NATS distribution worker")
      this.subscription.unsubscribe()
      this.subscription = undefined
    }
  }

  private async handleMessage(msg: Nats.Msg): Promise<void> {
    try {
      const request = this.codec.decode(msg.data) as NatsDistributionRequest
      this.logger?.debug("Received events from leader via NATS", {
        deliveryId: request.deliveryId,
        count: request.events.length,
      })

      await this.handler(request.events)

      const reply: NatsDistributionReply = { status: "ack" }
      msg.respond(this.codec.encode(reply))
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "unknown error"
      this.logger?.error("Worker failed to process events via NATS", { error: errorMessage })

      const reply: NatsDistributionReply = { status: "fail", error: errorMessage }
      msg.respond(this.codec.encode(reply))
    }
  }
}
