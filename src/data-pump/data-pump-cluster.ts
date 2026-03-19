import type { FlowcoreEvent } from "@flowcore/sdk"
import { FlowcoreDataPump, type FlowcoreDataPumpOptions } from "./data-pump.ts"
import type { FlowcoreDataSource } from "./data-source.ts"
import { clusterMetrics } from "./metrics.ts"
import { NatsConnectionManager } from "./nats-connection.ts"
import { NatsDistributionLeader, NatsDistributionWorker } from "./nats-distribution.ts"
import type { FlowcoreDataPumpCoordinator, FlowcoreLogger } from "./types.ts"
import { DeliveryTracker, WsConnection, type WsMessage } from "./ws-protocol.ts"

const LEASE_KEY = "flowcore-data-pump-leader"
const DISCOVERY_POLL_INTERVAL_MS = 10_000
const DEFAULT_LEASE_TTL_MS = 30_000
const DEFAULT_LEASE_RENEW_INTERVAL_MS = 10_000
const DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000
const DEFAULT_STALE_THRESHOLD_MS = 30_000
const DEFAULT_WORKER_CONCURRENCY = 1
const RECONNECT_BASE_MS = 1_000
const RECONNECT_MAX_MS = 30_000
const DELIVERY_TIMEOUT_MS = 30_000

export interface FlowcoreDataPumpClusterOptions extends FlowcoreDataPumpOptions {
  coordinator: FlowcoreDataPumpCoordinator
  advertisedAddress?: string
  dataSourceOverride?: FlowcoreDataSource
  leaseTtlMs?: number
  leaseRenewIntervalMs?: number
  heartbeatIntervalMs?: number
  workerConcurrency?: number
  clusterKey?: string
}

interface WorkerConnection {
  instanceId: string
  connection: WsConnection
  deliveryTracker: DeliveryTracker
  pendingCount: number
  reconnectAttempts: number
}

export class FlowcoreDataPumpCluster {
  private readonly instanceId: string
  private readonly coordinator: FlowcoreDataPumpCoordinator
  private readonly leaseTtlMs: number
  private readonly leaseRenewIntervalMs: number
  private readonly heartbeatIntervalMs: number
  private readonly workerConcurrency: number
  private readonly logger?: FlowcoreLogger

  private running = false
  private isLeader = false
  private pump?: FlowcoreDataPump
  private leaderConnection?: WsConnection

  // leader state
  private workers = new Map<string, WorkerConnection>()
  private workerRoundRobinIndex = 0
  private leaseRenewInterval?: ReturnType<typeof setInterval>
  private discoveryInterval?: ReturnType<typeof setInterval>

  // common state
  private heartbeatInterval?: ReturnType<typeof setInterval>
  private electionInterval?: ReturnType<typeof setInterval>

  // worker state - handler for processing events received from leader
  private workerHandler?: (events: FlowcoreEvent[]) => Promise<void>
  private workerFailedHandler?: (events: FlowcoreEvent[]) => void | Promise<void>

  // NATS distribution state
  private natsConnectionManager?: NatsConnectionManager
  private natsDistLeader?: NatsDistributionLeader
  private natsDistWorker?: NatsDistributionWorker

  constructor(private readonly options: FlowcoreDataPumpClusterOptions) {
    // validate: WS mode requires advertisedAddress
    if (!this.useNatsDistribution && !options.advertisedAddress) {
      throw new Error("advertisedAddress is required when not using NATS distribution (notifier.type !== 'nats')")
    }

    this.instanceId = crypto.randomUUID()
    this.coordinator = options.coordinator
    this.leaseTtlMs = options.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS
    this.leaseRenewIntervalMs = options.leaseRenewIntervalMs ?? DEFAULT_LEASE_RENEW_INTERVAL_MS
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS
    this.workerConcurrency = options.workerConcurrency ?? DEFAULT_WORKER_CONCURRENCY
    this.logger = options.logger

    // capture user's processor for worker mode
    this.workerHandler = options.processor?.handler
    this.workerFailedHandler = options.processor?.failedHandler
  }

  private get useNatsDistribution(): boolean {
    return this.options.notifier?.type === "nats"
  }

  private get natsDistributionSubject(): string {
    const key = this.options.clusterKey ??
      `${this.options.dataSource.tenant}.${this.options.dataSource.dataCore}.${this.options.dataSource.flowType}`
    return `data-pump.distribute.${key}`
  }

  get isRunning(): boolean {
    return this.running
  }

  get isLeaderInstance(): boolean {
    return this.isLeader
  }

  get activeWorkerCount(): number {
    return this.workers.size
  }

  get id(): string {
    return this.instanceId
  }

  /**
   * Handle an incoming WebSocket connection from a peer.
   * Call this from your HTTP server's WebSocket upgrade handler.
   *
   * Example (Deno):
   * ```typescript
   * Deno.serve({ port: 8080 }, (req) => {
   *   if (req.headers.get("upgrade") === "websocket") {
   *     const { socket, response } = Deno.upgradeWebSocket(req)
   *     cluster.handleConnection(socket)
   *     return response
   *   }
   *   return new Response("Not found", { status: 404 })
   * })
   * ```
   *
   * Example (Node.js with ws):
   * ```typescript
   * import { WebSocketServer } from "ws"
   * const wss = new WebSocketServer({ port: 8080 })
   * wss.on("connection", (ws) => cluster.handleConnection(ws))
   * ```
   */
  handleConnection(ws: WebSocket): void {
    this.logger?.debug("Incoming WS connection")

    const conn = new WsConnection(ws, {
      logger: this.logger,
      onMessage: (msg) => this.handleWorkerMessage(msg, conn),
      onClose: () => {
        this.logger?.info("Leader connection closed")
        this.leaderConnection = undefined
      },
    })

    this.leaderConnection = conn
  }

  async start(): Promise<void> {
    if (this.running) {
      throw new Error("Cluster already running")
    }
    this.running = true

    this.logger?.info("Starting cluster instance", { instanceId: this.instanceId })

    if (this.useNatsDistribution) {
      // NATS mode: connect shared NATS, start worker subscription
      const natsServers = this.options.notifier?.type === "nats" ? this.options.notifier.servers : []
      this.natsConnectionManager = new NatsConnectionManager(natsServers, this.logger)
      const conn = await this.natsConnectionManager.connect()

      // all instances subscribe as workers (leader also processes if no workers pick up)
      if (this.workerHandler) {
        this.natsDistWorker = new NatsDistributionWorker(
          conn,
          this.natsDistributionSubject,
          this.workerHandler,
          this.logger,
        )
        this.natsDistWorker.start()
      }

      // register with placeholder address (not used for routing in NATS mode)
      await this.coordinator.register(this.instanceId, "nats://distributed")
    } else {
      // WS mode: register with actual address
      await this.coordinator.register(this.instanceId, this.options.advertisedAddress!)
    }

    // Start heartbeat
    this.startHeartbeat()

    // Start leader election loop
    this.startElectionLoop()
  }

  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    this.logger?.info("Stopping cluster instance", { instanceId: this.instanceId })

    // stop election + heartbeat
    if (this.electionInterval) {
      clearInterval(this.electionInterval)
      this.electionInterval = undefined
    }
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval)
      this.heartbeatInterval = undefined
    }

    // stop leader duties
    this.stepDownAsLeader()

    // close worker connection to leader
    this.leaderConnection?.close()
    this.leaderConnection = undefined

    // stop NATS distribution
    this.natsDistWorker?.stop()
    this.natsDistWorker = undefined
    this.natsDistLeader = undefined

    // close shared NATS connection
    if (this.natsConnectionManager) {
      await this.natsConnectionManager.close()
      this.natsConnectionManager = undefined
    }

    // unregister
    try {
      await this.coordinator.releaseLease(this.instanceId, LEASE_KEY)
    } catch {
      // ignore
    }
    try {
      await this.coordinator.unregister(this.instanceId)
    } catch {
      // ignore
    }

    clusterMetrics.leaderStatusGauge.set(0)
    clusterMetrics.activeWorkersGauge.set(0)

    this.logger?.info("Cluster instance stopped")
  }

  // #region Heartbeat

  private startHeartbeat(): void {
    this.heartbeatInterval = setInterval(async () => {
      if (!this.running) return
      try {
        await this.coordinator.heartbeat(this.instanceId)
      } catch (error) {
        this.logger?.warn("Heartbeat failed", { error })
      }
    }, this.heartbeatIntervalMs)
  }

  // #endregion

  // #region Leader Election

  private startElectionLoop(): void {
    // attempt immediately
    void this.attemptElection()

    this.electionInterval = setInterval(async () => {
      if (!this.running) return

      if (this.isLeader) {
        // renew lease
        try {
          const renewed = await this.coordinator.renewLease(this.instanceId, LEASE_KEY, this.leaseTtlMs)
          if (!renewed) {
            this.logger?.warn("Lease renewal failed, stepping down")
            this.stepDownAsLeader()
          }
        } catch (error) {
          this.logger?.error("Lease renewal error, stepping down", { error })
          this.stepDownAsLeader()
        }
      } else {
        await this.attemptElection()
      }
    }, this.leaseRenewIntervalMs)
  }

  private async attemptElection(): Promise<void> {
    try {
      const acquired = await this.coordinator.acquireLease(this.instanceId, LEASE_KEY, this.leaseTtlMs)
      if (acquired && !this.isLeader) {
        this.logger?.info("Won leader election", { instanceId: this.instanceId })
        await this.becomeLeader()
      }
    } catch (error) {
      this.logger?.debug("Election attempt failed", { error })
    }
  }

  // #endregion

  // #region Leader Mode

  private async becomeLeader(): Promise<void> {
    this.isLeader = true
    clusterMetrics.leaderStatusGauge.set(1)

    // close any incoming leader connection (we are leader now)
    this.leaderConnection?.close()
    this.leaderConnection = undefined

    if (this.useNatsDistribution) {
      // NATS mode: create leader distributor, no WS discovery needed
      const conn = await this.natsConnectionManager!.connect()
      this.natsDistLeader = new NatsDistributionLeader(
        conn,
        this.natsDistributionSubject,
        this.logger,
      )
    } else {
      // WS mode: start worker discovery
      this.startWorkerDiscovery()
      await this.discoverWorkers()
    }

    // start the pump with distribution processor
    this.startPumpAsLeader()
  }

  private stepDownAsLeader(): void {
    if (!this.isLeader) return
    this.isLeader = false
    clusterMetrics.leaderStatusGauge.set(0)

    // stop discovery
    if (this.discoveryInterval) {
      clearInterval(this.discoveryInterval)
      this.discoveryInterval = undefined
    }

    // stop lease renewal (separate from election interval)
    if (this.leaseRenewInterval) {
      clearInterval(this.leaseRenewInterval)
      this.leaseRenewInterval = undefined
    }

    // NATS mode: close leader distributor (worker subscription stays)
    this.natsDistLeader = undefined

    // disconnect all WS workers
    for (const worker of this.workers.values()) {
      worker.deliveryTracker.rejectAll(new Error("Leader stepping down"))
      worker.connection.close()
    }
    this.workers.clear()
    clusterMetrics.activeWorkersGauge.set(0)

    // stop pump
    if (this.pump) {
      this.pump.stop()
      this.pump = undefined
    }
  }

  private startPumpAsLeader(): void {
    // create pump with distribution processor that sends events to workers
    const pumpOptions: FlowcoreDataPumpOptions = {
      ...this.options,
      processor: {
        concurrency: this.workerConcurrency,
        handler: async (events: FlowcoreEvent[]) => {
          await this.distributeEvents(events)
        },
        failedHandler: this.workerFailedHandler,
      },
    }

    this.pump = FlowcoreDataPump.create(pumpOptions, this.options.dataSourceOverride)
    this.pump.start().catch((error) => {
      this.logger?.error("Pump error in leader mode", { error })
    })
  }

  private distributeEvents(events: FlowcoreEvent[]): Promise<void> {
    if (this.useNatsDistribution) {
      return this.distributeEventsNats(events)
    }
    return this.distributeEventsWs(events)
  }

  private async distributeEventsNats(events: FlowcoreEvent[]): Promise<void> {
    if (!this.natsDistLeader) {
      // no NATS leader distributor - process locally as fallback
      this.logger?.debug("No NATS leader distributor, processing locally")
      if (this.workerHandler) {
        await this.workerHandler(events)
      }
      return
    }

    clusterMetrics.eventsDistributedCounter.inc(events.length)

    try {
      await this.natsDistLeader.distribute(events)
      clusterMetrics.workerAcksCounter.inc()
    } catch (error) {
      clusterMetrics.workerFailsCounter.inc()
      // fallback to local processing on NATS failure
      this.logger?.warn("NATS distribution failed, processing locally", {
        error: error instanceof Error ? error.message : "unknown",
      })
      if (this.workerHandler) {
        await this.workerHandler(events)
      }
    }
  }

  private async distributeEventsWs(events: FlowcoreEvent[]): Promise<void> {
    const worker = this.selectWorker()

    if (!worker) {
      // no workers available - process locally (single-instance fallback)
      this.logger?.debug("No workers available, processing locally")
      if (this.workerHandler) {
        await this.workerHandler(events)
      }
      return
    }

    const deliveryId = crypto.randomUUID()
    const eventIds = events.map((e) => e.eventId)

    worker.pendingCount++
    clusterMetrics.eventsDistributedCounter.inc(events.length)

    // send events to worker
    worker.connection.send({
      type: "events",
      deliveryId,
      events,
    })

    // wait for ack
    try {
      await worker.deliveryTracker.add(deliveryId, eventIds, DELIVERY_TIMEOUT_MS)
      worker.pendingCount--
      clusterMetrics.workerAcksCounter.inc()
    } catch (error) {
      worker.pendingCount--
      clusterMetrics.workerFailsCounter.inc()
      throw error
    }
  }

  private selectWorker(): WorkerConnection | null {
    const available: WorkerConnection[] = []
    for (const worker of this.workers.values()) {
      if (worker.pendingCount < this.workerConcurrency && !worker.connection.isClosed) {
        available.push(worker)
      }
    }

    if (available.length === 0) return null

    this.workerRoundRobinIndex = this.workerRoundRobinIndex % available.length
    const selected = available[this.workerRoundRobinIndex]
    this.workerRoundRobinIndex++
    return selected
  }

  // #endregion

  // #region Worker Discovery

  private startWorkerDiscovery(): void {
    this.discoveryInterval = setInterval(async () => {
      if (!this.running || !this.isLeader) return
      await this.discoverWorkers()
    }, DISCOVERY_POLL_INTERVAL_MS)
  }

  private async discoverWorkers(): Promise<void> {
    try {
      const instances = await this.coordinator.getInstances(DEFAULT_STALE_THRESHOLD_MS)

      for (const instance of instances) {
        // skip self
        if (instance.instanceId === this.instanceId) continue
        // skip already connected
        if (this.workers.has(instance.instanceId)) continue

        this.connectToWorker(instance.instanceId, instance.address)
      }

      // clean up workers that are no longer registered
      const registeredIds = new Set(instances.map((i) => i.instanceId))
      for (const [id, worker] of this.workers) {
        if (!registeredIds.has(id)) {
          this.logger?.info("Worker no longer registered, disconnecting", { instanceId: id })
          worker.deliveryTracker.rejectAll(new Error("Worker unregistered"))
          worker.connection.close()
          this.workers.delete(id)
        }
      }

      clusterMetrics.activeWorkersGauge.set(this.workers.size)
    } catch (error) {
      this.logger?.warn("Worker discovery failed", { error })
    }
  }

  private connectToWorker(instanceId: string, address: string): void {
    this.logger?.info("Connecting to worker", { instanceId, address })

    try {
      const ws = new WebSocket(address)
      const deliveryTracker = new DeliveryTracker()

      const workerConn: WorkerConnection = {
        instanceId,
        connection: undefined as unknown as WsConnection,
        deliveryTracker,
        pendingCount: 0,
        reconnectAttempts: 0,
      }

      ws.onopen = () => {
        const conn = new WsConnection(ws, {
          logger: this.logger,
          onMessage: (msg) => this.handleLeaderMessage(msg, instanceId),
          onClose: () => this.handleWorkerDisconnect(instanceId),
        })
        workerConn.connection = conn
        workerConn.reconnectAttempts = 0
        this.workers.set(instanceId, workerConn)
        clusterMetrics.activeWorkersGauge.set(this.workers.size)
        this.logger?.info("Connected to worker", { instanceId })
      }

      ws.onerror = () => {
        this.logger?.warn("Failed to connect to worker", { instanceId, address })
        this.scheduleReconnect(instanceId, address, (workerConn.reconnectAttempts || 0) + 1)
      }
    } catch (error) {
      this.logger?.warn("Error connecting to worker", { instanceId, error })
    }
  }

  private handleWorkerDisconnect(instanceId: string): void {
    const worker = this.workers.get(instanceId)
    if (!worker) return

    this.logger?.info("Worker disconnected", { instanceId })

    // reject all pending deliveries - pump's ack timeout will handle reOpen
    worker.deliveryTracker.rejectAll(new Error("Worker disconnected"))
    this.workers.delete(instanceId)
    clusterMetrics.activeWorkersGauge.set(this.workers.size)

    // schedule reconnect if still running as leader
    if (this.running && this.isLeader) {
      this.scheduleReconnect(instanceId, "", worker.reconnectAttempts + 1)
    }
  }

  private scheduleReconnect(instanceId: string, address: string, attempts: number): void {
    if (!this.running || !this.isLeader) return

    const delay = Math.min(RECONNECT_BASE_MS * Math.pow(2, attempts - 1), RECONNECT_MAX_MS)
    this.logger?.debug("Scheduling reconnect", { instanceId, delay, attempts })

    setTimeout(async () => {
      if (!this.running || !this.isLeader) return

      // re-discover address from DB if we don't have it
      let resolvedAddress = address
      if (!resolvedAddress) {
        try {
          const instances = await this.coordinator.getInstances(DEFAULT_STALE_THRESHOLD_MS)
          const instance = instances.find((i) => i.instanceId === instanceId)
          if (!instance) return // worker gone, don't reconnect
          resolvedAddress = instance.address
        } catch {
          return
        }
      }

      if (!this.workers.has(instanceId)) {
        this.connectToWorker(instanceId, resolvedAddress)
      }
    }, delay)
  }

  // #endregion

  // #region Message Handlers

  // leader receives ack/fail from workers
  private handleLeaderMessage(msg: WsMessage, workerInstanceId: string): void {
    const worker = this.workers.get(workerInstanceId)
    if (!worker) return

    if (msg.type === "ack") {
      worker.deliveryTracker.ack(msg.deliveryId)
    } else if (msg.type === "fail") {
      worker.deliveryTracker.fail(msg.deliveryId)
    }
  }

  // worker receives events from leader
  private handleWorkerMessage(msg: WsMessage, conn: WsConnection): void {
    if (msg.type !== "events") return

    const { deliveryId, events } = msg
    this.logger?.debug("Received events from leader", { deliveryId, count: events.length })

    // process events using user's handler
    void (async () => {
      try {
        if (this.workerHandler) {
          await this.workerHandler(events)
        }
        conn.send({
          type: "ack",
          deliveryId,
          eventIds: events.map((e) => e.eventId),
        })
      } catch (error) {
        this.logger?.error("Worker failed to process events", { deliveryId, error })
        conn.send({
          type: "fail",
          deliveryId,
          eventIds: events.map((e) => e.eventId),
        })
      }
    })()
  }

  // #endregion
}
