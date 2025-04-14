/**
 * @module data-source
 * @description Provides functionality to interact with Flowcore data sources
 */
import {
  DataCoreFetchCommand,
  EventListCommand,
  EventTypeListCommand,
  type FlowcoreClient,
  type FlowcoreEvent,
  FlowTypeFetchCommand,
  TenantTranslateNameToIdCommand,
  TimeBucketListCommand,
} from "@flowcore/sdk"
import { getFlowcoreClient } from "./flowcore-client.ts"
import type { FlowcoreDataPumpAuth, FlowcoreDataPumpDataSource, FlowcoreDataPumpState } from "./types.ts"

/**
 * Options for configuring a Flowcore data source
 * @interface FlowcoreDataSourceOptions
 */
export interface FlowcoreDataSourceOptions {
  /** Authentication information for Flowcore */
  auth: FlowcoreDataPumpAuth
  /** Data source configuration */
  dataSource: FlowcoreDataPumpDataSource
  /** Optional base URL override for the Flowcore API */
  baseUrlOverride?: string
  /** When true, disables name-to-ID translation for tenant, dataCore, flowType, and eventTypes */
  noTranslation?: boolean
  /** When true, executes commands in direct mode */
  directMode?: boolean
}

/**
 * Provides methods to interact with a Flowcore data source
 * @class FlowcoreDataSource
 */
export class FlowcoreDataSource {
  /** Flowcore client instance */
  protected flowcoreClient: FlowcoreClient
  /** Cached tenant ID */
  protected tenantId?: string
  /** Cached data core ID */
  protected dataCoreId?: string
  /** Cached flow type ID */
  protected flowTypeId?: string
  /** Cached event type IDs */
  protected eventTypeIds?: string[]
  /** Cached time buckets */
  protected timeBuckets?: string[]

  /**
   * Creates a new FlowcoreDataSource instance
   * @param options - Configuration options for the data source
   */
  constructor(protected readonly options: FlowcoreDataSourceOptions) {
    this.flowcoreClient = getFlowcoreClient(this.options.auth, this.options.baseUrlOverride)
  }

  /**
   * Gets the tenant name from the data source configuration
   * @returns Tenant name
   */
  public get tenant(): string {
    return this.options.dataSource.tenant
  }

  /**
   * Gets the data core name from the data source configuration
   * @returns Data core name
   */
  public get dataCore(): string {
    return this.options.dataSource.dataCore
  }

  /**
   * Gets the flow type name from the data source configuration
   * @returns Flow type name
   */
  public get flowType(): string {
    return this.options.dataSource.flowType
  }

  /**
   * Gets the event type names from the data source configuration
   * @returns Array of event type names
   */
  public get eventTypes(): string[] {
    return this.options.dataSource.eventTypes
  }

  /**
   * Gets the tenant ID, translating from name if necessary
   * @returns Promise that resolves to the tenant ID
   */
  public async getTenantId(): Promise<string> {
    if (this.tenantId) {
      return this.tenantId
    }

    if (this.options.noTranslation) {
      this.tenantId = this.options.dataSource.tenant
    } else {
      const command = new TenantTranslateNameToIdCommand({
        tenant: this.options.dataSource.tenant,
      })
      const result = await this.flowcoreClient.execute(command, this.options.directMode)
      this.tenantId = result.id
    }
    return this.tenantId
  }

  /**
   * Gets the data core ID, translating from name if necessary
   * @returns Promise that resolves to the data core ID
   */
  public async getDataCoreId(): Promise<string> {
    if (this.dataCoreId) {
      return this.dataCoreId
    }

    if (this.options.noTranslation) {
      this.dataCoreId = this.options.dataSource.dataCore
    } else {
      const command = new DataCoreFetchCommand({
        tenant: this.options.dataSource.tenant,
        dataCore: this.options.dataSource.dataCore,
      })
      const result = await this.flowcoreClient.execute(command, this.options.directMode)
      this.dataCoreId = result.id
    }
    return this.dataCoreId
  }

  /**
   * Gets the flow type ID, translating from name if necessary
   * @returns Promise that resolves to the flow type ID
   */
  public async getFlowTypeId(): Promise<string> {
    if (this.flowTypeId) {
      return this.flowTypeId
    }

    if (this.options.noTranslation) {
      this.flowTypeId = this.options.dataSource.flowType
    } else {
      const command = new FlowTypeFetchCommand({
        dataCoreId: await this.getDataCoreId(),
        flowType: this.options.dataSource.flowType,
      })
      const result = await this.flowcoreClient.execute(command, this.options.directMode)
      this.flowTypeId = result.id
    }
    return this.flowTypeId
  }

  /**
   * Gets the event type IDs, translating from names if necessary
   * @returns Promise that resolves to an array of event type IDs
   * @throws Error if an event type is not found
   */
  public async getEventTypeIds(): Promise<string[]> {
    if (this.eventTypeIds) {
      return this.eventTypeIds
    }

    if (this.options.noTranslation) {
      this.eventTypeIds = this.options.dataSource.eventTypes
    } else {
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

  /**
   * Gets all time buckets for the configured event types
   * @param force - When true, forces a refresh of cached time buckets
   * @returns Promise that resolves to an array of time bucket strings
   */
  public async getTimeBuckets(force = false): Promise<string[]> {
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

  /**
   * Gets the next time bucket after the specified time bucket
   * @param timeBucket - The reference time bucket
   * @returns Promise that resolves to the next time bucket, or null if none exists
   */
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

  /**
   * Gets the closest time bucket to the specified time bucket
   * @param timeBucket - The reference time bucket
   * @param getBefore - When true, gets the closest time bucket before the reference
   * @returns Promise that resolves to the closest time bucket, or null if none exists
   * @throws Error if the time bucket format is invalid
   */
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

  /**
   * Gets events from the data source starting from a specific state
   * @param from - The state to start from (includes timeBucket and eventId)
   * @param amount - Maximum number of events to retrieve
   * @param toEventId - Optional ID to stop at when retrieving events
   * @returns Promise that resolves to an array of Flowcore events
   */
  public async getEvents(from: FlowcoreDataPumpState, amount: number, toEventId?: string): Promise<FlowcoreEvent[]> {
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
