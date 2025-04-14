import type { FlowcoreLogger } from "./types.ts"

export const noOpLogger: FlowcoreLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: console.error,
}
