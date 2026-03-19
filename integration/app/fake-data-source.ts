import type { EventListOutput, FlowcoreEvent } from "@flowcore/sdk"
import { TimeUuid } from "@flowcore/time-uuid"
import { utc } from "@date-fns/utc"
import { format, startOfHour } from "date-fns"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"
import type { FlowcoreDataPumpState } from "../../src/data-pump/types.ts"

export class FakeDataSource extends FlowcoreDataSource {
  private readonly events: FlowcoreEvent[]
  private readonly timeBucket: string
  private delivered = false

  constructor(totalEvents: number) {
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
    if (this.delivered) {
      return { events: [], nextCursor: undefined }
    }
    this.delivered = true
    return {
      events: this.events.slice(0, amount),
      nextCursor: undefined,
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
