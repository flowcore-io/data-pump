import type { FlowcoreEvent } from "@flowcore/sdk"
import type { FlowcoreLogger } from "./types.ts"

// #region Message Types

export interface WsEventsMessage {
  type: "events"
  deliveryId: string
  events: FlowcoreEvent[]
}

export interface WsAckMessage {
  type: "ack"
  deliveryId: string
  eventIds: string[]
}

export interface WsFailMessage {
  type: "fail"
  deliveryId: string
  eventIds: string[]
}

export interface WsPingMessage {
  type: "ping"
}

export interface WsPongMessage {
  type: "pong"
}

export type WsMessage = WsEventsMessage | WsAckMessage | WsFailMessage | WsPingMessage | WsPongMessage

// #endregion

// #region Serialization

export function serializeMessage(msg: WsMessage): string {
  return JSON.stringify(msg)
}

export function deserializeMessage(data: string): WsMessage | null {
  try {
    const parsed = JSON.parse(data)
    if (!parsed || typeof parsed !== "object" || !parsed.type) {
      return null
    }
    return parsed as WsMessage
  } catch {
    return null
  }
}

// #endregion

// #region Connection Management

const PING_INTERVAL_MS = 5_000
const PONG_TIMEOUT_MS = 10_000

export interface WsConnectionOptions {
  logger?: FlowcoreLogger
  onMessage: (msg: WsMessage) => void
  onClose: () => void
}

export class WsConnection {
  private pingInterval?: ReturnType<typeof setInterval>
  private pongTimeout?: ReturnType<typeof setTimeout>
  private closed = false

  constructor(
    private readonly ws: WebSocket,
    private readonly options: WsConnectionOptions,
  ) {
    this.ws.onmessage = (event) => {
      const msg = deserializeMessage(String(event.data))
      if (!msg) {
        this.options.logger?.warn("Received invalid WS message")
        return
      }
      if (msg.type === "ping") {
        this.send({ type: "pong" })
        return
      }
      if (msg.type === "pong") {
        this.clearPongTimeout()
        return
      }
      this.options.onMessage(msg)
    }

    this.ws.onclose = () => {
      this.cleanup()
      this.options.onClose()
    }

    this.ws.onerror = () => {
      this.options.logger?.warn("WebSocket error")
      this.close()
    }

    this.startPingLoop()
  }

  send(msg: WsMessage): void {
    if (this.closed || this.ws.readyState !== WebSocket.OPEN) {
      return
    }
    this.ws.send(serializeMessage(msg))
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    this.cleanup()
    try {
      this.ws.close()
    } catch {
      // ignore close errors
    }
  }

  get isClosed(): boolean {
    return this.closed
  }

  private startPingLoop(): void {
    this.pingInterval = setInterval(() => {
      if (this.closed) return
      this.send({ type: "ping" })
      this.pongTimeout = setTimeout(() => {
        this.options.logger?.warn("Pong timeout, closing connection")
        this.close()
      }, PONG_TIMEOUT_MS)
    }, PING_INTERVAL_MS)
  }

  private clearPongTimeout(): void {
    if (this.pongTimeout) {
      clearTimeout(this.pongTimeout)
      this.pongTimeout = undefined
    }
  }

  private cleanup(): void {
    this.closed = true
    if (this.pingInterval) {
      clearInterval(this.pingInterval)
      this.pingInterval = undefined
    }
    this.clearPongTimeout()
  }
}

// #endregion

// #region Pending Delivery Tracker

export interface PendingDelivery {
  deliveryId: string
  eventIds: string[]
  resolve: () => void
  reject: (error: Error) => void
  timeout: ReturnType<typeof setTimeout>
}

export class DeliveryTracker {
  private pending = new Map<string, PendingDelivery>()

  add(deliveryId: string, eventIds: string[], timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(deliveryId)
        reject(new Error(`Delivery ${deliveryId} timed out`))
      }, timeoutMs)

      this.pending.set(deliveryId, { deliveryId, eventIds, resolve, reject, timeout })
    })
  }

  ack(deliveryId: string): void {
    const delivery = this.pending.get(deliveryId)
    if (!delivery) return
    clearTimeout(delivery.timeout)
    this.pending.delete(deliveryId)
    delivery.resolve()
  }

  fail(deliveryId: string): void {
    const delivery = this.pending.get(deliveryId)
    if (!delivery) return
    clearTimeout(delivery.timeout)
    this.pending.delete(deliveryId)
    delivery.reject(new Error(`Delivery ${deliveryId} failed by worker`))
  }

  rejectAll(error: Error): void {
    for (const delivery of this.pending.values()) {
      clearTimeout(delivery.timeout)
      delivery.reject(error)
    }
    this.pending.clear()
  }

  get size(): number {
    return this.pending.size
  }

  getPendingEventIds(): string[] {
    const ids: string[] = []
    for (const delivery of this.pending.values()) {
      ids.push(...delivery.eventIds)
    }
    return ids
  }
}

// #endregion
