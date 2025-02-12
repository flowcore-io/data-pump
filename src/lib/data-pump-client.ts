import OidcClient from "@flowcore/sdk-oidc-client"
import { FlowcoreClient, type FlowcoreEvent } from "@flowcore/sdk"
import { DataPump, type DataPumpStateManager, type Logger } from "./data-pump.ts"
import { WebSocketNotifier } from "./ws-notifier.ts"
import { NatsNotifier } from "./nats-notifier.ts"

const defaultAuthUrl = "https://auth.flowcore.io/realms/flowcore/.well-known/openid-configuration"

export interface DataPumpClientOptions {
  logger?: Logger
  auth: {
    clientId: string
    clientSecret: string
    authUrl?: string
  }
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
    onEvents: (events: FlowcoreEvent[]) => Promise<void>
  }
  natsServers?: string[]
}

export function createDataPump(options: DataPumpClientOptions): DataPump {
  const authClient = new OidcClient.OidcClient(
    options.auth.clientId,
    options.auth.clientSecret,
    options.auth.authUrl ?? defaultAuthUrl,
  )
  const flowcoreClient = new FlowcoreClient({
    getBearerToken: async () => (await authClient.getToken()).accessToken ?? null,
  })

  const stateManager = options.stateManager ?? {
    getState: () => null,
    setState: () => {},
  }

  const notifier = options.natsServers
    ? new NatsNotifier({
      servers: options.natsServers,
    })
    : new WebSocketNotifier({
      auth: {
        clientId: options.auth.clientId,
        clientSecret: options.auth.clientSecret,
        authUrl: options.auth.authUrl ?? defaultAuthUrl,
      },
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
