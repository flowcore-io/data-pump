import * as Nats from "nats"
import type { FlowcoreLogger } from "./types.ts"

export class NatsConnectionManager {
  private connection?: Nats.NatsConnection
  private connecting?: Promise<Nats.NatsConnection>

  constructor(
    private readonly servers: string[],
    private readonly logger?: FlowcoreLogger,
  ) {}

  connect(): Promise<Nats.NatsConnection> {
    if (this.connection && !this.connection.isClosed()) {
      return Promise.resolve(this.connection)
    }

    // deduplicate concurrent connect calls
    if (this.connecting) {
      return this.connecting
    }

    this.connecting = (async () => {
      this.logger?.debug("Connecting to NATS", { servers: this.servers })
      const conn = await Nats.connect({ servers: this.servers })
      this.connection = conn
      this.connecting = undefined
      this.logger?.info("Connected to NATS")
      return conn
    })()

    return this.connecting
  }

  async close(): Promise<void> {
    if (this.connection && !this.connection.isClosed()) {
      this.logger?.debug("Closing NATS connection")
      await this.connection.drain()
      this.connection = undefined
    }
    this.connecting = undefined
  }

  get isConnected(): boolean {
    return !!this.connection && !this.connection.isClosed()
  }
}
