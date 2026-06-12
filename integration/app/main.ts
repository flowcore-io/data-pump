import { Client } from "pg"
import { FlowcoreDataPumpCluster } from "../../src/data-pump/data-pump-cluster.ts"
import { PostgresCoordinator } from "./postgres-coordinator.ts"
import { PostgresStateManager } from "./postgres-state-manager.ts"
import { FakeDataSource } from "./fake-data-source.ts"

const DATABASE_URL = Bun.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/datapump_test"
const POD_NAME = Bun.env.POD_NAME ?? "local-pod"
const NATS_URL = Bun.env.NATS_URL
const TOTAL_EVENTS = parseInt(Bun.env.TOTAL_EVENTS ?? "100", 10)
const WS_PORT = parseInt(Bun.env.WS_PORT ?? "8080", 10)

const log = {
  debug: (msg: string, meta?: Record<string, unknown>) => console.log(`[DEBUG] [${POD_NAME}] ${msg}`, meta ?? ""),
  info: (msg: string, meta?: Record<string, unknown>) => console.log(`[INFO]  [${POD_NAME}] ${msg}`, meta ?? ""),
  warn: (msg: string, meta?: Record<string, unknown>) => console.warn(`[WARN]  [${POD_NAME}] ${msg}`, meta ?? ""),
  error: (msg: string | Error, meta?: Record<string, unknown>) =>
    console.error(`[ERROR] [${POD_NAME}] ${msg}`, meta ?? ""),
}

// connect to PG
const db = new Client({ connectionString: DATABASE_URL })
await db.connect()
log.info("Connected to PostgreSQL")

// create tables
await db.query(`
  CREATE TABLE IF NOT EXISTS flowcore_pump_leases (
    key TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
  );

  CREATE TABLE IF NOT EXISTS flowcore_pump_instances (
    instance_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  CREATE TABLE IF NOT EXISTS pump_state (
    id TEXT PRIMARY KEY,
    time_bucket TEXT NOT NULL,
    event_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    pod_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
`)
log.info("Database tables ready")

// create components
const coordinator = new PostgresCoordinator(db)
const stateManager = new PostgresStateManager(db)
const fakeDataSource = new FakeDataSource(TOTAL_EVENTS)

const useNats = !!NATS_URL
log.info(`Distribution mode: ${useNats ? "NATS" : "WS"}`, { natsUrl: NATS_URL })

// create cluster
const POD_IP = Bun.env.POD_IP ?? "127.0.0.1"
const cluster = new FlowcoreDataPumpCluster({
  auth: { getBearerToken: () => Promise.resolve("fake") },
  dataSource: {
    tenant: "integration-test",
    dataCore: "test-data-core",
    flowType: "test-flow-type",
    eventTypes: ["test-event"],
  },
  stateManager,
  coordinator,
  dataSourceOverride: fakeDataSource,
  ...(useNats
    ? {
        notifier: { type: "nats" as const, servers: [NATS_URL!] },
      }
    : {
        advertisedAddress: `ws://${POD_IP}:${WS_PORT}`,
        notifier: { type: "poller" as const, intervalMs: 2000 },
      }),
  noTranslation: true,
  processor: {
    concurrency: 5,
    handler: async (events) => {
      for (const event of events) {
        await db.query(`INSERT INTO processed_events (pod_name, event_id) VALUES ($1, $2)`, [POD_NAME, event.eventId])
      }
      log.info(`Processed ${events.length} events`)
    },
  },
  leaseTtlMs: 15000,
  leaseRenewIntervalMs: 5000,
  heartbeatIntervalMs: 3000,
  workerConcurrency: 5,
  logger: log,
})

// start HTTP server (health endpoint only in NATS mode, health + WS in WS mode)
interface BunWebSocketAdapter {
  onmessage: ((event: MessageEvent) => void) | null
  onclose: ((event: CloseEvent) => void) | null
  onerror: ((event: Event) => void) | null
  readonly readyState: number
  send(data: string | BufferSource): void
  close(code?: number, reason?: string): void
}

const server = Bun.serve<{ adapter?: BunWebSocketAdapter }>({
  port: WS_PORT,
  fetch(req, server) {
    const url = new URL(req.url)

    if (url.pathname === "/health") {
      return new Response(
        JSON.stringify({
          instanceId: cluster.id,
          isLeader: cluster.isLeaderInstance,
          workerCount: cluster.activeWorkerCount,
          isRunning: cluster.isRunning,
          podName: POD_NAME,
          distributionMode: useNats ? "nats" : "ws",
        }),
        { headers: { "content-type": "application/json" } },
      )
    }

    if (!useNats && server.upgrade(req, { data: {} })) {
      return undefined
    }

    return new Response("Not found", { status: 404 })
  },
  websocket: {
    open(socket) {
      const adapter: BunWebSocketAdapter = {
        onmessage: null,
        onclose: null,
        onerror: null,
        get readyState() {
          return socket.readyState
        },
        send(data) {
          socket.send(data as string)
        },
        close(code, reason) {
          socket.close(code, reason)
        },
      }
      socket.data.adapter = adapter
      cluster.handleConnection(adapter as WebSocket)
    },
    message(socket, message) {
      socket.data.adapter?.onmessage?.(new MessageEvent("message", { data: message }))
    },
    close(socket, code, reason) {
      socket.data.adapter?.onclose?.(new CloseEvent("close", { code, reason }))
    },
  },
})

log.info(`HTTP server listening on :${WS_PORT}`)

// start cluster
await cluster.start()
log.info("Cluster started")

// graceful shutdown
process.on("SIGTERM", async () => {
  log.info("SIGTERM received, shutting down...")
  await cluster.stop()
  await db.end()
  server.stop()
  process.exit(0)
})
