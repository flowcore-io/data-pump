import type { EventListOutput, FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { utc } from "@date-fns/utc"
import { format, startOfHour } from "date-fns"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"
import type { FlowcoreDataPumpState } from "../../src/data-pump/types.ts"

export class FakeDataSource extends FlowcoreDataSource {
  private readonly events: FlowcoreEvent[]
  private readonly timeBucket: string
  private deliveredIndex = 0
  private startTime: number
  private readonly startDelayMs: number

  constructor(totalEvents: number, startDelayMs = 15000) {
    super({
      auth: { getBearerToken: () => Promise.resolve("fake") },
      dataSource: {
        tenant: "integration-test",
        dataCore: "test-data-core",
        flowType: "test-flow-type",
        eventTypes: ["test-event"],
      },
      noTranslation: true,
    })

    this.startDelayMs = startDelayMs
    this.startTime = Date.now()
    this.timeBucket = format(startOfHour(utc(new Date())), "yyyyMMddHH0000")
    this.events = []

    for (let i = 0; i < totalEvents; i++) {
      const timeUuid = TimeUuid.now()
      this.events.push({
        eventId: timeUuid.toString(),
        eventType: "test-event",
        aggregator: `agg-${i}`,
        payload: { index: i, data: `test-payload-${i}` },
        metadata: {},
        timeBucket: this.timeBucket,
        validTime: new Date().toISOString(),
      })
    }

    // pre-set cached IDs so the parent class never calls Flowcore API
    this.tenantId = "integration-test"
    this.dataCoreId = "test-data-core"
    this.flowTypeId = "test-flow-type"
    this.eventTypeIds = ["test-event"]
    this.timeBuckets = [this.timeBucket]
  }

  public override async getEvents(
    _from: FlowcoreDataPumpState,
    amount: number,
    _toEventId?: string,
    _cursor?: string,
    _includeSensitiveData?: boolean,
  ): Promise<EventListOutput> {
    // delay event delivery to give the cluster time to form (workers connect)
    const elapsed = Date.now() - this.startTime
    if (elapsed < this.startDelayMs) {
      await new Promise((resolve) => setTimeout(resolve, 2000))
      return { events: [], nextCursor: undefined }
    }

    if (this.deliveredIndex >= this.events.length) {
      return { events: [], nextCursor: undefined }
    }

    // deliver in batches of 10 to allow distribution across workers
    const batchSize = Math.min(10, amount, this.events.length - this.deliveredIndex)
    const batch = this.events.slice(this.deliveredIndex, this.deliveredIndex + batchSize)
    this.deliveredIndex += batchSize

    return {
      events: batch,
      nextCursor: this.deliveredIndex < this.events.length ? String(this.deliveredIndex) : undefined,
    }
  }

  public override async getTimeBuckets(_force?: boolean): Promise<string[]> {
    return [this.timeBucket]
  }

  public override async getNextTimeBucket(_timeBucket: string): Promise<string | null> {
    return null
  }

  public override async getClosestTimeBucket(_timeBucket: string, _getBefore?: boolean): Promise<string | null> {
    return this.timeBucket
  }

  public override async getTenantId(): Promise<string> {
    return "integration-test"
  }

  public override async getDataCoreId(): Promise<string> {
    return "test-data-core"
  }

  public override async getFlowTypeId(): Promise<string> {
    return "test-flow-type"
  }

  public override async getEventTypeIds(): Promise<string[]> {
    return ["test-event"]
  }
}
