import { subMilliseconds } from "date-fns"
import { utc } from "@date-fns/utc"
import type { Buffer } from "node:buffer"
import { types } from "cassandra-driver"
import { format, startOfHour } from "date-fns"

export class TimeUuid {
  constructor(private readonly timeUuid: types.TimeUuid) {}

  public static fromDate(date: Date, ticks?: number, nodeId?: string | Buffer, clockId?: string | Buffer): TimeUuid {
    return new TimeUuid(types.TimeUuid.fromDate(utc(date), ticks, nodeId, clockId))
  }

  public static fromString(value: string): TimeUuid {
    return new TimeUuid(types.TimeUuid.fromString(value))
  }

  private getDatePrecision(): { date: Date; ticks: number } {
    return this.timeUuid.getDatePrecision()
  }

  public getBefore(): TimeUuid {
    const { date, ticks } = this.getDatePrecision()
    const newTicks = ticks - 1
    if (newTicks < 0) {
      return TimeUuid.fromDate(subMilliseconds(date, 1), 10_000)
    }
    return TimeUuid.fromDate(date, newTicks)
  }

  public static fromNow(): TimeUuid {
    return TimeUuid.fromDate(new Date(), 10_000)
  }

  public toString(): string {
    return this.timeUuid.toString()
  }

  public getTimeBucket(): string {
    return format(startOfHour(this.timeUuid.getDate()), "yyyyMMddHH0000")
  }
}
