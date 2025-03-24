import {
  DataCoreFetchCommand,
  EventListCommand,
  EventTypeListCommand,
  type FlowcoreClient,
  FlowTypeFetchCommand,
  TenantTranslateNameToIdCommand,
  TimeBucketListCommand,
} from "@flowcore/sdk"
import { getFlowcoreClient } from "./flowcore-client.ts"
import type { FlowcoreDataPumpAuth, FlowcoreDataPumpDataSource, FlowcoreDataPumpState } from "./types.ts"
export interface FlowcoreDataSourceOptions {
  auth: FlowcoreDataPumpAuth
  dataSource: FlowcoreDataPumpDataSource
}

export class FlowcoreDataSource {
  private flowcoreClient: FlowcoreClient
  private tenantId?: string
  private dataCoreId?: string
  private flowTypeId?: string
  private eventTypeIds?: string[]
  private timeBuckets?: string[]

  constructor(private readonly options: FlowcoreDataSourceOptions) {
    this.flowcoreClient = getFlowcoreClient(this.options.auth)
  }

  public get tenant() {
    return this.options.dataSource.tenant
  }

  public get dataCore() {
    return this.options.dataSource.dataCore
  }

  public get flowType() {
    return this.options.dataSource.flowType
  }

  public get eventTypes() {
    return this.options.dataSource.eventTypes
  }

  public async getTenantId() {
    if (this.tenantId) {
      return this.tenantId
    }
    const command = new TenantTranslateNameToIdCommand({
      tenant: this.options.dataSource.tenant,
    })
    const result = await this.flowcoreClient.execute(command)
    this.tenantId = result.id
    return this.tenantId
  }

  public async getDataCoreId() {
    if (this.dataCoreId) {
      return this.dataCoreId
    }
    const command = new DataCoreFetchCommand({
      tenant: this.options.dataSource.tenant,
      dataCore: this.options.dataSource.dataCore,
    })
    const result = await this.flowcoreClient.execute(command)
    this.dataCoreId = result.id
    return this.dataCoreId
  }

  public async getFlowTypeId() {
    if (this.flowTypeId) {
      return this.flowTypeId
    }

    const command = new FlowTypeFetchCommand({
      dataCoreId: await this.getDataCoreId(),
      flowType: this.options.dataSource.flowType,
    })
    const result = await this.flowcoreClient.execute(command)
    this.flowTypeId = result.id
    return this.flowTypeId
  }

  public async getEventTypeIds() {
    if (this.eventTypeIds) {
      return this.eventTypeIds
    }
    const command = new EventTypeListCommand({
      flowTypeId: await this.getFlowTypeId(),
    })
    const result = await this.flowcoreClient.execute(command)
    this.eventTypeIds = result.map((eventType) => eventType.id)
    return this.eventTypeIds
  }

  public async getTimeBuckets(force = false) {
    if (this.timeBuckets && !force) {
      return this.timeBuckets
    }
    let cursor: number | undefined
    const timeBuckets: string[] = []
    do {
      const result = await this.flowcoreClient.execute(
        new TimeBucketListCommand({
          tenant: this.options.dataSource.tenant,
          eventTypeId: (await this.getEventTypeIds()) as [string, ...string[]],
          cursor: cursor || undefined,
        }),
      )
      timeBuckets.push(...result.timeBuckets)
      cursor = result.nextCursor
    } while (cursor !== undefined)
    this.timeBuckets = timeBuckets
    return this.timeBuckets
  }

  public async getNextTimeBucket(timeBucket: string): Promise<string | null> {
    const closestTimeBucket = await this.getClosestTimeBucket(timeBucket)
    if (!closestTimeBucket) {
      return null
    }
    const timeBuckets = await this.getTimeBuckets()
    const index = timeBuckets.indexOf(closestTimeBucket)
    if (index === -1) {
      throw new Error(`Could not get next timeBucket, timeBucket ${timeBucket} not found`)
    }
    return timeBuckets[index + 1] ?? null
  }

  public async getClosestTimeBucket(timeBucket: string, getBefore = false): Promise<string | null> {
    const timeBuckets = await this.getTimeBuckets()
    if (!timeBucket.match(/^\d{14}$/)) {
      throw new Error(`Invalid timebucket: ${timeBucket}`)
    }
    if (getBefore) {
      return (
        timeBuckets.findLast((t) => Number.parseFloat(t) <= Number.parseFloat(timeBucket)) ??
          timeBuckets[timeBuckets.length - 1]
      )
    }
    return (
      timeBuckets.find((t) => Number.parseFloat(t) >= Number.parseFloat(timeBucket)) ??
        timeBuckets[timeBuckets.length - 1]
    )
  }

  public async getEvents(from: FlowcoreDataPumpState, amount: number, toEventId?: string) {
    const result = await this.flowcoreClient.execute(
      new EventListCommand({
        tenant: this.options.dataSource.tenant,
        eventTypeId: (await this.getEventTypeIds()) as [string, ...string[]],
        timeBucket: from.timeBucket,
        afterEventId: from.eventId,
        pageSize: amount,
        toEventId,
      }),
    )
    return result.events
  }
}
