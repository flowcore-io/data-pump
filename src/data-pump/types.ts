import type { FlowcoreEvent } from "@flowcore/sdk"

export type FlowcoreDataPumpAuth =
  | {
    apiKey: string
    apiKeyId: string
  }
  | {
    getBearerToken: () => Promise<string>
  }

export interface FlowcoreDataPumpState {
  timeBucket: string
  eventId?: string
}

export interface FlowcoreDataPumpDataSource {
  tenant: string
  dataCore: string
  flowType: string
  eventTypes: string[]
}

export interface FlowcoreDataPumpStateManager {
  getState: () => Promise<FlowcoreDataPumpState | null> | FlowcoreDataPumpState | null
  setState?: (state: FlowcoreDataPumpState) => Promise<void> | void
}

export interface FlowcoreLogger {
  debug: (message: string, metadata?: Record<string, unknown>) => void
  info: (message: string, metadata?: Record<string, unknown>) => void
  warn: (message: string, metadata?: Record<string, unknown>) => void
  error: (message: string | Error, metadata?: Record<string, unknown>) => void
}

export interface FlowcoreDataPumpProcessor {
  concurrency?: number
  handler: (events: FlowcoreEvent[]) => Promise<void>
  failedHandler?: (events: FlowcoreEvent[]) => void | Promise<void>
}
