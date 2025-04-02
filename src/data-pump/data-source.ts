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
  baseUrlOverride?: string
  noTranslation?: boolean
  directMode?: boolean
}

export class FlowcoreDataSource {
  private flowcoreClient: FlowcoreClient
  private tenantId?: string
  private dataCoreId?: string
  private flowTypeId?: string
  private eventTypeIds?: string[]
  private timeBuckets?: string[]

  constructor(private readonly options: FlowcoreDataSourceOptions) {
    this.flowcoreClient = getFlowcoreClient(this.options.auth, this.options.baseUrlOverride)
    console.log("this.options", this.options)
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

    if (this.options.noTranslation) {
      this.tenantId = this.options.dataSource.tenant
    } else {
      console.log("translating tenant", this.options.dataSource.tenant, this.options.directMode)
      const command = new TenantTranslateNameToIdCommand({
        tenant: this.options.dataSource.tenant,
      })
      const result = await this.flowcoreClient.execute(command, this.options.directMode)
      this.tenantId = result.id
    }
    return this.tenantId
  }

  public async getDataCoreId() {
    if (this.dataCoreId) {
      return this.dataCoreId
    }

    if (this.options.noTranslation) {
      this.dataCoreId = this.options.dataSource.dataCore
    } else {
      console.log("translating dataCore", this.options.dataSource.dataCore, this.options.directMode)
      const command = new DataCoreFetchCommand({
        tenant: this.options.dataSource.tenant,
        dataCore: this.options.dataSource.dataCore,
      })
      const result = await this.flowcoreClient.execute(command, this.options.directMode)
      this.dataCoreId = result.id
    }
    return this.dataCoreId
  }

  public async getFlowTypeId() {
    if (this.flowTypeId) {
      return this.flowTypeId
    }

    if (this.options.noTranslation) {
      this.flowTypeId = this.options.dataSource.flowType
    } else {
      console.log("translating flowType", this.options.dataSource.flowType, this.options.directMode)
      const command = new FlowTypeFetchCommand({
        dataCoreId: await this.getDataCoreId(),
        flowType: this.options.dataSource.flowType,
      })
      const result = await this.flowcoreClient.execute(command, this.options.directMode)
      this.flowTypeId = result.id
    }
    return this.flowTypeId
  }

  public async getEventTypeIds() {
    if (this.eventTypeIds) {
      return this.eventTypeIds
    }

    if (this.options.noTranslation) {
      this.eventTypeIds = this.options.dataSource.eventTypes
    } else {
      console.log("translating eventTypes", this.options.dataSource.eventTypes, this.options.directMode)
      const command = new EventTypeListCommand({
        flowTypeId: await this.getFlowTypeId(),
      })
      const results = await this.flowcoreClient.execute(command, this.options.directMode)
      const eventTypeIds: string[] = []
      for (const eventType of this.eventTypes) {
        const found = results.find((result) => result.name === eventType)
        if (!found) {
          throw new Error(`Event type ${eventType} not found`)
        }
        eventTypeIds.push(found.id)
      }
      this.eventTypeIds = eventTypeIds
    }
    return this.eventTypeIds
  }

  public async getTimeBuckets(force = false) {
    if (this.timeBuckets && !force) {
      return this.timeBuckets
    }
      
    let cursor: number | undefined
    const timeBuckets: string[] = []
    console.log("getting timeBuckets, directMode: ", this.options.directMode)
    do {
      const result = await this.flowcoreClient.execute(
        new TimeBucketListCommand({
          tenant: this.options.dataSource.tenant,
          eventTypeId: (await this.getEventTypeIds()) as [string, ...string[]],
          cursor: cursor || undefined,
          pageSize: 10_000,
        }),
        this.options.directMode,
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
    console.log("getEvents, directMode: ", this.options.directMode)
    const eventTypeIds = await this.getEventTypeIds()
    const result = await this.flowcoreClient.execute(
      new EventListCommand({
        tenant: this.options.dataSource.tenant,
        eventTypeId: eventTypeIds as [string, ...string[]],
        timeBucket: from.timeBucket,
        afterEventId: from.eventId,
        pageSize: amount,
        toEventId,
      }),
      this.options.directMode,
    )
    return result.events
  }
}
