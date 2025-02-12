import { TimeUuid as CassandraTimeUuid } from "cassandra-uuid"
import { subMilliseconds } from "date-fns"
import { utc } from "@date-fns/utc"
import type { Buffer } from "node:buffer"

export class TimeUuid {
  constructor(private readonly timeUuid: CassandraTimeUuid) {}

  public static fromDate(date: Date, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer) {
    return new TimeUuid(CassandraTimeUuid.fromDate(utc(date), ticks, nodeId, clockId))
  }

  public static fromString(value: string) {
    return new TimeUuid(CassandraTimeUuid.fromString(value))
  }

  private getDatePrecision(): { date: Date; ticks: number } {
    return this.timeUuid.getDatePrecision()
  }

  public getBefore() {
    const { date, ticks } = this.getDatePrecision()
    const newTicks = ticks - 1
    if (newTicks < 0) {
      return TimeUuid.fromDate(subMilliseconds(date, 1), 10_000)
    }
    return TimeUuid.fromDate(date, newTicks)
  }

  public static fromNow() {
    return TimeUuid.fromDate(new Date(), 10_000)
  }

  public toString(): string {
    return this.timeUuid.toString()
  }
}
