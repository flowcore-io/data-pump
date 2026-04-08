import { SendPumpPulseCommand } from "@flowcore/sdk"
import type { FlowcoreDataPumpAuth, FlowcoreLogger } from "./types.ts"
import { getFlowcoreClient } from "./flowcore-client.ts"

/**
 * Log level for pulse emitter events. Corresponds to FlowcoreLogger methods.
 */
export type PulseLogLevel = "debug" | "info" | "warn" | "error"

export interface PulseEmitterOptions {
  url: string
  intervalMs?: number
  auth: FlowcoreDataPumpAuth
  logger?: FlowcoreLogger
  /** Log level for successful pulses. Defaults to 'debug'. */
  successLogLevel?: PulseLogLevel
  /** Log level for pulse failures. Defaults to 'warn'. */
  failureLogLevel?: PulseLogLevel
}

export interface PulseSnapshot {
  pathwayId: string
  flowType: string
  timeBucket: string
  eventId: string | undefined
  isLive: boolean
  bufferDepth: number
  bufferReserved: number
  bufferSizeBytes: number
  acknowledgedTotal: number
  failedTotal: number
  pulledTotal: number
  uptimeMs: number
}

export class PulseEmitter {
  private interval: ReturnType<typeof setInterval> | null = null
  private startTimeout: ReturnType<typeof setTimeout> | null = null
  private readonly intervalMs: number
  private readonly logger?: FlowcoreLogger
  private readonly successLogLevel: PulseLogLevel
  private readonly failureLogLevel: PulseLogLevel

  constructor(
    private readonly options: PulseEmitterOptions,
    private readonly getSnapshot: () => PulseSnapshot | null,
  ) {
    this.intervalMs = options.intervalMs ?? 30_000
    this.logger = options.logger
    this.successLogLevel = options.successLogLevel ?? "debug"
    this.failureLogLevel = options.failureLogLevel ?? "warn"
  }

  start(): void {
    if (this.interval || this.startTimeout) return

    // Random initial delay to stagger pulses from multiple pumps
    const initialDelay = Math.floor(Math.random() * this.intervalMs)
    this.startTimeout = setTimeout(() => {
      this.startTimeout = null
      this.emitSafe()
      this.interval = setInterval(() => this.emitSafe(), this.intervalMs)
    }, initialDelay)
  }

  stop(): void {
    if (this.startTimeout) {
      clearTimeout(this.startTimeout)
      this.startTimeout = null
    }
    if (this.interval) {
      clearInterval(this.interval)
      this.interval = null
    }
  }

  private emitSafe(): void {
    this.emit().catch((err) => {
      const snapshot = this.getSnapshot()
      const msg = err instanceof Error ? err.message : String(err)
      this.logger?.[this.failureLogLevel]?.("Pulse emission failed", {
        error: msg,
        url: this.options.url,
        pathwayId: snapshot?.pathwayId,
        flowType: snapshot?.flowType,
      })
    })
  }

  private async emit(): Promise<void> {
    const snapshot = this.getSnapshot()
    if (!snapshot) return

    const client = getFlowcoreClient(this.options.auth, this.options.url)

    await client.execute(
      new SendPumpPulseCommand({
        pathwayId: snapshot.pathwayId,
        flowType: snapshot.flowType,
        timeBucket: snapshot.timeBucket,
        eventId: snapshot.eventId ?? null,
        isLive: snapshot.isLive,
        buffer: {
          depth: snapshot.bufferDepth,
          reserved: snapshot.bufferReserved,
          sizeBytes: snapshot.bufferSizeBytes,
        },
        counters: {
          acknowledged: snapshot.acknowledgedTotal,
          failed: snapshot.failedTotal,
          pulled: snapshot.pulledTotal,
        },
        uptimeMs: snapshot.uptimeMs,
      }),
    )

    this.logger?.[this.successLogLevel]?.("Pulse sent", {
      pathwayId: snapshot.pathwayId,
      flowType: snapshot.flowType,
      timeBucket: snapshot.timeBucket,
      isLive: snapshot.isLive,
      bufferDepth: snapshot.bufferDepth,
    })
  }
}
