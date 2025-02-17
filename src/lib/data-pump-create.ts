import { FlowcoreClient, type FlowcoreEvent } from "@flowcore/sdk"
import { DataPump, type DataPumpStateManager, type Logger } from "./data-pump.ts"
import { WebSocketNotifier } from "./ws-notifier.ts"
import { NatsNotifier } from "./nats-notifier.ts"

export const noOpLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
}

interface DataPumpClientOptionsAuthOidcClient {
  oidcClient: {
    getToken: () => Promise<{ accessToken: string }>
  }
}

interface DataPumpClientOptionsAuthApiKey {
  apiKey: string
  apiKeyId: string
}

type DataPumpClientOptionsAuth = DataPumpClientOptionsAuthOidcClient | DataPumpClientOptionsAuthApiKey

export interface DataPumpClientOptions {
  logger?: Logger
  auth: DataPumpClientOptionsAuth
  dataSource: {
    tenant: string
    dataCore: string
    flowType: string
    eventTypes: string[]
  }
  buffer?: {
    size?: number
    threshold?: number
    maxDeliveryCount?: number
    achknowledgeTimeoutMs?: number
  }
  stateManager?: DataPumpStateManager
  processor?: {
    concurrency?: number
    onEvents: (events: FlowcoreEvent[]) => Promise<boolean> | boolean
    onFailedEvents?: (events: FlowcoreEvent[]) => Promise<void> | void
  }
  natsServers?: string[]
}

export function createDataPump(options: DataPumpClientOptions): DataPump {
  let flowcoreClient: FlowcoreClient
  if ("oidcClient" in options.auth) {
    const authClient = options.auth.oidcClient
    flowcoreClient = new FlowcoreClient({
      getBearerToken: async () => (await authClient.getToken()).accessToken ?? null,
    })
  } else {
    flowcoreClient = new FlowcoreClient({
      apiKey: options.auth.apiKey,
      apiKeyId: options.auth.apiKeyId,
    })
  }

  const stateManager = options.stateManager ?? {
    getState: () => null,
    setState: () => {},
  }

  const notifier = options.natsServers
    ? new NatsNotifier({
      servers: options.natsServers,
    })
    : new WebSocketNotifier({
      auth: options.auth,
      dataSource: options.dataSource,
      logger: options.logger,
    })

  const bufferSize = options.buffer?.size ?? 1000
  const buffer = {
    size: bufferSize,
    threshold: options.buffer?.threshold ?? Math.round(bufferSize * 0.1),
    maxDeliveryCount: options.buffer?.maxDeliveryCount ?? 3,
    achknowledgeTimeoutMs: options.buffer?.achknowledgeTimeoutMs ?? 10_000,
  }

  const processor = options.processor
    ? {
      concurrency: options.processor.concurrency ?? 1,
      onEvents: options.processor.onEvents,
      onFailedEvents: options.processor.onFailedEvents,
    }
    : undefined

  const dataPump = new DataPump({
    flowcoreClient,
    stateManager,
    dataSource: options.dataSource,
    buffer,
    processor,
    notifier: notifier.getNotifier(),
    logger: options.logger,
  })

  return dataPump
}
