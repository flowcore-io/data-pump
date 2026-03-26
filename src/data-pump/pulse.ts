import { SendPumpPulseCommand } from "@flowcore/sdk"
import type { FlowcoreDataPumpAuth, FlowcoreLogger } from "./types.ts"
import { getFlowcoreClient } from "./flowcore-client.ts"

export interface PulseEmitterOptions {
  url: string
  intervalMs?: number
  auth: FlowcoreDataPumpAuth
  logger?: FlowcoreLogger
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
  private readonly intervalMs: number
  private readonly logger?: FlowcoreLogger

  constructor(
    private readonly options: PulseEmitterOptions,
    private readonly getSnapshot: () => PulseSnapshot | null,
  ) {
    this.intervalMs = options.intervalMs ?? 30_000
    this.logger = options.logger
  }

  start(): void {
    if (this.interval) return

    this.interval = setInterval(() => {
      this.emit().catch((err) => {
        this.logger?.warn?.("Pulse emission failed", { error: err instanceof Error ? err.message : String(err) })
      })
    }, this.intervalMs)
  }

  stop(): void {
    if (this.interval) {
      clearInterval(this.interval)
      this.interval = null
    }
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
  }
}
