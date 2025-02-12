import type { Buffer } from "node:buffer"

declare module "cassandra-uuid" {
  export class TimeUuid {
    static fromDate: (date: Date, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer) => TimeUuid
    static fromString: (value: string) => TimeUuid
    static min: (date: Date, ticks: number) => TimeUuid
    static max: (date: Date, ticks: number) => TimeUuid
    static now: (nodeId?: string | Buffer, clockId?: string | Buffer) => TimeUuid
    static test: (s: string) => boolean
    getDatePrecision: () => { date: Date; ticks: number }
    getDate: () => Date

    constructor(value?: Date, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer)
  }
}
