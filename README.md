# Flowcore Data Pump Client

A reliable, high-performance TypeScript client for streaming and processing events from the Flowcore platform. Built for
real-time event processing with automatic retry, buffering, and state management.

[![NPM Version](https://img.shields.io/npm/v/@flowcore/data-pump)](https://www.npmjs.com/package/@flowcore/data-pump)

## Simple Example Setup

```typescript
import { FlowcoreDataPump } from "@flowcore/data-pump"

const dataPump = FlowcoreDataPump.create({
  // The identity must have permission to list buckets and read events.
  auth: {
    apiKey: process.env.FLOWCORE_API_KEY!,
  },
  dataSource: {
    tenant: "your-tenant-name",
    dataCore: "your-data-core",
    flowType: "your-flow-type",
    eventTypes: ["event-type-1", "event-type-2"],
  },
  stateManager: {
    getState: () => loadState(),
    setState: (state) => saveState(state),
  },
  processor: {
    concurrency: 10, // Batch size passed to one handler invocation
    handler: async (events) => {
      for (const event of events) {
        await processIdempotently(event)
      }
    },
  },
  notifier: { type: "websocket" },
  bufferSize: 500,
  logger: {
    debug: (msg) => console.log(`[DEBUG] ${msg}`),
    info: (msg) => console.log(`[INFO] ${msg}`),
    warn: (msg) => console.warn(`[WARN] ${msg}`),
    error: (msg) => console.error(`[ERROR] ${msg}`),
  },
})

// The callback form runs in the background and enables fetch-loop self-healing.
await dataPump.start((error) => {
  if (error) console.error("Data Pump stopped with an error", error)
})
```

## Installation

### Node.js

```bash
npm install @flowcore/data-pump
```

```typescript
import { FlowcoreDataPump } from "@flowcore/data-pump"
```

## Key Concepts

### **Time Buckets**

Events are organized in **hourly time buckets** using the format `yyyyMMddHH0000`:

```
20240315140000 = March 15, 2024, 14:00 (2 PM)
20240315150000 = March 15, 2024, 15:00 (3 PM)
20240315160000 = March 15, 2024, 16:00 (4 PM)
```

**Why time buckets matter:**

- **Precise positioning**: Resume from any hour in your event history
- **Efficient queries**: Flowcore can quickly locate events within time ranges
- **Catch-up processing**: Process months of historical data in sequence
- **Debugging**: Jump to specific time periods when issues occurred

### **State Management**

The pump persists a conservative processing frontier using a time bucket and event ID:

```typescript
// Current position in event stream
{
  timeBucket: "20240315140000",
  eventId: "abc-123-def-456"
}
```

The fetch position can run ahead of this persisted frontier. When events remain buffered, the saved event ID is based on
the earliest remaining event rather than simply the last handler to finish. After a crash, the pump may therefore replay
events. Consumers must use `eventId` as a durable idempotency key.

### **Event Lifecycle & Processing Modes**

Understanding how events flow through the system:

#### **Push Mode Flow (Automatic)**

```
Fetch → Buffer → Reserve → Process → ✅ Acknowledge (or ❌ Retry)
         ↑                    ↑                 ↑
    You configure         Pump handles     You write business logic
```

#### **Pull Mode Flow (Manual)**

```
Fetch → Buffer → YOU Reserve → YOU Process → YOU Acknowledge/Fail
         ↑           ↑              ↑              ↑
    Pump handles   You control   You control   You control
```

### **Buffer Management**

Local **in-memory event queue** between fetching and processing:

```typescript
Buffer: [Event1, Event2, Event3, Event4, Event5]
         ↑                              ↑
    Processing these              Fetching more
```

**Handles key scenarios:**

- **Backpressure**: When processing is slower than event arrival rate
- **Batch processing**: Group multiple events for efficient processing
- **Flow control**: Automatic throttling based on buffer capacity
- **Memory protection**: Prevents unlimited memory growth during slow processing

### **Live vs Historical Processing**

Two fundamental processing patterns:

#### **Live Mode**

- **When**: `stateManager.getState()` returns `null`
- **Behavior**: Start in the current hour after a time-based event ID representing now, then process newly stored events
- **Use case**: Production event processing, real-time analytics

Returning `null` does not replay events from earlier in the current hour. Supply an explicit state for a backfill or when
creating a projection from retained history.

#### **Historical Mode**

- **When**: `stateManager.getState()` returns `{ timeBucket, eventId }`
- **Behavior**: Process events from specific point in time
- **Use case**: Backfill data, debugging, data migration, replaying scenarios

### **Batch Size and Application Concurrency**

In version 0.22.x, `processor.concurrency` controls how many events are reserved and passed to one handler invocation. It
does not start that many handler invocations in parallel:

```typescript
processor: {
  concurrency: 5,
  handler: async (events) => {
    // This batch contains up to five events.
    // Add parallelism here only when ordering and dependencies allow it.
    await Promise.all(events.map(processIdempotently))
  }
}
```

The batch is acknowledged only after the complete handler resolves. If some events commit before a later event throws,
the whole batch can be delivered again.

### **Failure Handling and Retries**

Handler retries are driven by the acknowledgment timeout:

```
Reserve → Handler throws → Reservation times out → Reopen → Reserve again
```

`achknowledgeTimeoutMs` is a fixed delay; per-event redelivery does not use exponential backoff. With the default
`maxRedeliveryCount: 3`, an event can be reserved four times: the initial delivery plus three redeliveries. Set the value
to `-1` only when unlimited redelivery is intentional.

Exponential backoff from one to thirty seconds is used for failed fetch loops, process loops, leader pumps, and cluster
reconnections.

## Usage Patterns

### Push Mode (Automatic Lifecycle Management)

**Best for most use cases** - You focus on business logic, the pump handles everything else automatically.

- **You handle**: Writing your event processing logic
- **Pump handles**: Reserve → Process → Acknowledge → Retry on failures
- **Use when**: Standard event processing with simple error handling

```typescript
const dataPump = FlowcoreDataPump.create({
  auth: {
    /* auth config */
  },
  dataSource: {
    /* data source config */
  },
  stateManager: {
    /* state management */
  },
  processor: {
    concurrency: 5,
    handler: async (events) => {
      for (const event of events) {
        await processIdempotently(event)
      }
      // The complete batch is acknowledged after this handler resolves.
      // If it throws, the reservation reopens after achknowledgeTimeoutMs.
    },
    failedHandler: async (failedEvents) => {
      // Handle events that permanently failed after all retries
      await logFailedEvents(failedEvents)
    },
  },
  bufferSize: 1000,
  maxRedeliveryCount: 3,
})

await dataPump.start((error) => {
  if (error) console.error("Data Pump stopped", error)
})
```

### Pull Mode (Manual Lifecycle Control)

**For advanced scenarios** - You control reservation and acknowledgment manually.

- **You handle**: Reserve → Process → Acknowledge, leave reserved for retry, or fail terminally
- **Pump provides**: Raw event access and buffer management
- **Use when**: Complex error handling, partial batch failures, or custom acknowledgment logic

```typescript
const dataPump = FlowcoreDataPump.create({
  auth: {
    /* auth config */
  },
  dataSource: {
    /* data source config */
  },
  stateManager: {
    /* state management */
  },
  // ❌ No processor = manual mode
})

await dataPump.start((error) => {
  if (error) console.error("Data Pump stopped", error)
})

// You manually control the entire event lifecycle
while (dataPump.isRunning) {
  const events = await dataPump.reserve(10)

  for (const event of events) {
    try {
      await processIdempotently(event)
      await dataPump.acknowledge([event.eventId])
    } catch (error) {
      console.warn("Leaving event reserved for timeout-based redelivery", event.eventId, error)
    }
  }
}
```

`fail(eventIds)` is terminal: it removes matching events from the buffer and invokes `failedHandler` when configured. It
does **not** schedule a retry. Leave a reservation unresolved to make it eligible for redelivery after the acknowledgment
timeout.

### Which Mode Should You Use?

| Scenario                        | Recommended Mode | Why                                                        |
| ------------------------------- | ---------------- | ---------------------------------------------------------- |
| **Simple event processing**     | **Push Mode**    | Just write business logic, pump handles everything else    |
| **Standard error handling**     | **Push Mode**    | Automatic retries and failure handling work for most cases |
| **Getting started**             | **Push Mode**    | Much simpler to set up and understand                      |
| **Complex error handling**      | **Pull Mode**    | Need to handle some events succeeding while others fail    |
| **Conditional acknowledgments** | **Pull Mode**    | Business logic determines which events to acknowledge      |
| **Custom retry strategies**     | **Pull Mode**    | Need more control than simple retry count                  |
| **Transaction integration**     | **Pull Mode**    | Need to coordinate with database transactions              |

## Authentication

### API Key Authentication

```typescript
auth: {
  apiKey: process.env.FLOWCORE_API_KEY!
}
```

Current `fc_{id}_{secret}` keys contain their key ID. `apiKeyId` remains available for old keys but is deprecated. The
identity needs IAM permission to list time buckets and read events for the selected resources.

### OIDC/Bearer Token Authentication

```typescript
import { oidcClient } from "@flowcore/sdk-oidc-client"

const oidc = oidcClient({
  clientId: "your-client-id",
  clientSecret: "your-client-secret",
})

auth: {
  getBearerToken: () => oidc.getToken().then((token) => token.accessToken)
}
```

## State Management

The state manager stores a conservative processing frontier. It supports recovery, but it does not provide exactly-once
processing. A crash between a business side effect and acknowledgment can cause replay, so handlers must be idempotent.

### Understanding State

**State Format:**

```typescript
interface FlowcoreDataPumpState {
  timeBucket: string // Format: "yyyyMMddHH0000" (e.g., "20240101120000")
  eventId?: string | undefined // Optional: specific event ID to resume from
  // eventId doesn't have to be the id of an actual event. Event Ids are timestamps that have been converted to UUIDs.
  // You can use the TimeUuid class to convert between timestamps and event IDs.
}
```

**Return Values:**

- `null` → Start in the current hour after an event ID representing the current time
- `{ timeBucket, eventId }` → Start from **specific position** (historical processing)

### Precise Positioning with TimeUuid

FlowcoreDataPump includes utilities for converting between timestamps and event IDs for precise positioning:

```typescript
import { TimeUuid } from "@flowcore/time-uuid"

// Generate event ID from specific timestamp
const eventId = TimeUuid.fromDate(new Date("2024-01-01T12:30:00Z")).toString()

// Start processing from timestamp (doesn't need to match existing event)
const stateManager = {
  getState: () => ({
    timeBucket: "20240101120000", // Hour bucket: 2024-01-01 12:00
    eventId, // Start from first event AFTER 12:30:00
  }),
  setState: (state) => {
    const timestamp = state.eventId ? TimeUuid.fromString(state.eventId).getDate() : undefined
    console.log("Saved processing frontier", state.timeBucket, timestamp?.toISOString())
  },
}
// Other useful TimeUuid methods:
const now = TimeUuid.now().toString() // Current timestamp as UUID
const date = TimeUuid.fromString(eventId).getDate() // Extract Date from UUID
const timestamp = date.getTime() // Unix timestamp
```

**Use cases:**

- **Precise replay**: Start from any timestamp within an hour (finds next available event)
- **Debugging**: Convert event IDs back to readable timestamps
- **Monitoring**: Track processing progress with human-readable times
- **Coordination**: Synchronize multiple instances to specific points
- **Gap handling**: Works even when no events exist at exact timestamp

### Memory State Manager (Development)

**Best for**: Local development, testing, non-critical applications

```typescript
let currentState = null; // Start in live mode

// Or start from specific time:
// let currentState = {
//   timeBucket: "20240101000000",  // January 1, 2024 00:00
//   eventId: undefined             // Start from first event in that hour
// };

stateManager: {
  getState: () => currentState,
  setState: (state) => {
    currentState = state;
    console.log(`Processed up to: ${state.timeBucket} - ${state.eventId}`);
  }
}
```

**⚠️ Limitations:**

- State lost on process restart
- No crash recovery
- Cannot share state between instances

### File-based State Manager (Single Instance)

**Best for**: Single instance deployments, simple persistence needs

```typescript
import { existsSync, readFileSync, writeFileSync } from "node:fs"

stateManager: {
  getState: () => {
    if (!existsSync("pump-state.json")) return null
    return JSON.parse(readFileSync("pump-state.json", "utf8"))
  },
  setState: (state) => {
    writeFileSync("pump-state.json", JSON.stringify(state, null, 2))
  }
}
```

**✅ Benefits:**

- Survives process restarts
- Simple file-based persistence
- No database dependency

**⚠️ Limitations:**

- Single instance only
- File system dependency
- No atomic updates

### Database State Manager (Production)

**Best for**: Production systems, multi-instance deployments, mission-critical applications

```sql
-- Example table schema
CREATE TABLE flowcore_pump_state (
  id VARCHAR(50) PRIMARY KEY,       -- Logical consumer identifier
  time_bucket VARCHAR(14) NOT NULL, -- "yyyyMMddHH0000"
  event_id VARCHAR(255),            -- Conservative processing frontier
  updated_at TIMESTAMP DEFAULT NOW()
);
```

```typescript
stateManager: {
  getState: async () => {
    try {
      const result = await db.query(
        'SELECT time_bucket, event_id FROM flowcore_pump_state WHERE id = ?',
        ['main']
      );

      if (result.length === 0) {
        console.log('No previous state found, starting in live mode');
        return null;
      }

      const state = {
        timeBucket: result[0].time_bucket,
        eventId: result[0].event_id
      };
      console.log('Resuming from database state:', state);
      return state;

    } catch (error) {
      console.error('Failed to load state from database:', error);
      throw error; // Do not silently jump to "now" when durable state is unavailable.
    }
  },

  setState: async (state) => {
    try {
      await db.query(`
        INSERT INTO flowcore_pump_state (id, time_bucket, event_id, updated_at)
        VALUES (?, ?, ?, NOW())
        ON DUPLICATE KEY UPDATE
          time_bucket = VALUES(time_bucket),
          event_id = VALUES(event_id),
          updated_at = NOW()
      `, ['main', state.timeBucket, state.eventId]);

    } catch (error) {
      console.error('CRITICAL: Failed to save state to database:', error);
      throw error; // Stop processing if we can't save progress
    }
  }
}
```

**✅ Benefits:**

- Survives crashes and restarts
- Supports shared state for cluster mode
- Atomic updates with transactions
- Can be backed up with your database
- Enables coordinated horizontal scaling

**⚠️ Considerations:**

- Database dependency
- Network latency on state updates
- Requires error handling strategy

### Independent Consumers and Shared Clusters

Use separate state keys when instances intentionally process different event selections as independent consumers:

```typescript
const consumerId = process.env.CONSUMER_ID!;

stateManager: {
  getState: async () => {
    const result = await db.query(
      'SELECT time_bucket, event_id FROM flowcore_pump_state WHERE id = ?',
      [consumerId]
    );
    return result[0] || null;
  },
  setState: async (state) => {
    await db.query(
      'INSERT OR REPLACE INTO flowcore_pump_state (id, time_bucket, event_id) VALUES (?, ?, ?)',
      [consumerId, state.timeBucket, state.eventId]
    );
  }
}
```

Do not use separate state rows for replicas that are meant to share one workload. Use `FlowcoreDataPumpCluster` with one
shared durable state manager so only the elected leader fetches and advances the logical checkpoint.

Persist every state update unless your storage adapter can prove that coalescing writes cannot advance beyond unfinished
work. Skipping arbitrary checkpoint writes can increase replay and make the stored frontier misleading.

### Choosing a State Manager

| Scenario                        | Recommended             | Reason                             |
| ------------------------------- | ----------------------- | ---------------------------------- |
| **Local development**           | Memory                  | Fast iteration, no setup           |
| **Testing/CI**                  | Memory                  | Clean state per test run           |
| **Single instance, simple**     | File-based              | Persistence without DB complexity  |
| **Production, single instance** | Database                | Reliability and backup integration |
| **Independent consumers**       | Database, separate keys | Separate selections and positions  |
| **Shared worker cluster**       | Database + cluster mode | One elected fetcher and checkpoint |
| **Mission-critical**            | Database + Monitoring   | Full observability stack           |

## Notification Methods

### WebSocket (Default)

Real-time notifications via Flowcore's notification service:

```typescript
notifier: {
  type: "websocket"
}
```

WebSocket waits have a 20-second safety timeout. An event, connection error, abort signal, or timeout releases the wait so
the fetch loop can query durable storage again. Each cycle creates a fresh client, allowing the pump to recover from a
hung or half-open connection.

### NATS

For distributed systems with message queues:

```typescript
notifier: {
  type: "nats",
  servers: ["nats://localhost:4222", "nats://backup:4222"]
}
```

### Polling

Simple polling mechanism:

```typescript
notifier: {
  type: "poller",
  intervalMs: 1000
}
```

> **Version 0.22.x caveat:** The implementation currently uses `Math.min(intervalMs, 1000)`. Values above one second
> therefore still wake after one second. Account for the API traffic until this behavior is corrected.

Notifications only wake the pump. Flowcore event history and the persisted state remain the durable recovery mechanism.

## Cluster Mode

`FlowcoreDataPumpCluster` scales handler execution while keeping one logical fetcher and one shared checkpoint. Every
replica registers with a coordinator and participates in lease election. The leader runs `FlowcoreDataPump`; other
instances process batches distributed by the leader.

Cluster mode requires a durable state manager and a user-provided `FlowcoreDataPumpCoordinator`. The package defines the
coordinator contract but does not ship a production implementation:

```typescript
interface FlowcoreDataPumpCoordinator {
  acquireLease(instanceId: string, key: string, ttlMs: number): Promise<boolean>
  renewLease(instanceId: string, key: string, ttlMs: number): Promise<boolean>
  releaseLease(instanceId: string, key: string): Promise<void>
  register(instanceId: string, address: string): Promise<void>
  heartbeat(instanceId: string): Promise<void>
  unregister(instanceId: string): Promise<void>
  getInstances(staleThresholdMs: number): Promise<
    Array<{
      instanceId: string
      address: string
    }>
  >
}
```

Lease acquisition must be atomic. Renewal must succeed only for the current holder, and `getInstances()` must omit stale
registrations. The `integration/app` directory contains a PostgreSQL reference used by the Kubernetes integration suite.

### NATS Distribution

Setting `notifier.type` to `nats` also selects NATS request/reply for cluster event distribution:

```typescript
import { FlowcoreDataPumpCluster } from "@flowcore/data-pump"

const cluster = new FlowcoreDataPumpCluster({
  auth: { apiKey: process.env.FLOWCORE_API_KEY! },
  dataSource: {
    tenant: "acme",
    dataCore: "commerce",
    flowType: "order.0",
    eventTypes: ["order.placed.0"],
  },
  stateManager: sharedStateManager,
  coordinator: postgresCoordinator,
  notifier: {
    type: "nats",
    servers: [process.env.NATS_URL!],
  },
  clusterKey: "orders-projection-v1",
  workerConcurrency: 10,
  processor: {
    handler: async (events) => {
      for (const event of events) await processIdempotently(event)
    },
  },
})

await cluster.start()
```

All replicas join the `data-pump-workers` queue group, including the leader. A distribution request waits up to 30
seconds for a reply. If NATS distribution fails, the leader runs the handler locally. A worker may have committed before
its reply was lost, so this fallback preserves at-least-once rather than exactly-once behavior.

`clusterKey` scopes the NATS subject. In 0.22.x the internal leader lease key is fixed to
`flowcore-data-pump-leader`; it is not scoped by `clusterKey`. Namespace coordinator storage when unrelated logical
clusters share a database.

### WebSocket Distribution

All non-NATS cluster configurations use a WebSocket mesh. Each replica must:

1. Host a WebSocket server and pass accepted connections to `cluster.handleConnection()`.
2. Configure an `advertisedAddress` reachable by every peer.
3. Register and heartbeat through the shared coordinator.

The leader discovers live workers every ten seconds and sends complete batches round-robin. Connections use ping/pong
health checks, and each delivery has a 30-second acknowledgment timeout. If no worker is available, the leader processes
locally.

The default lease TTL is 30 seconds, renewal interval is 10 seconds, and heartbeat interval is 5 seconds. Tune those
values together. Cluster mode exposes the automatic processor model only; it does not expose `reserve()`,
`acknowledge()`, `fail()`, or `restart()`.

Stop a cluster gracefully so it can release its lease and unregister:

```typescript
process.on("SIGTERM", async () => {
  await cluster.stop()
  await db.end()
  process.exit(0)
})
```

## ⚙️ Configuration Reference

| Option                  | Type                              | Default      | Description                                                                                                                                                               |
| ----------------------- | --------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `auth`                  | `FlowcoreDataPumpAuth`            | **Required** | Authentication configuration (API key or Bearer token)                                                                                                                    |
| `dataSource`            | `FlowcoreDataPumpDataSource`      | **Required** | Data source configuration (tenant, dataCore, flowType, eventTypes)                                                                                                        |
| `stateManager`          | `FlowcoreDataPumpStateManager`    | **Required** | State persistence configuration                                                                                                                                           |
| `bufferSize`            | `number`                          | `1000`       | Maximum events to buffer in memory                                                                                                                                        |
| `maxRedeliveryCount`    | `number`                          | `3`          | Redeliveries after the initial attempt; `-1` disables the cap                                                                                                             |
| `achknowledgeTimeoutMs` | `number`                          | `5000`       | Fixed timeout before an unresolved reservation reopens (spelling preserved by the public API)                                                                             |
| `includeSensitiveData`  | `boolean`                         | `false`      | Include sensitive data in events                                                                                                                                          |
| `processor`             | `FlowcoreDataPumpProcessor`       | `undefined`  | Automatic processing configuration; `concurrency` is the handler batch size in 0.22.x                                                                                     |
| `notifier`              | `FlowcoreDataPumpNotifierOptions` | `websocket`  | Notification method configuration                                                                                                                                         |
| `logger`                | `FlowcoreLogger`                  | `undefined`  | Custom logger implementation                                                                                                                                              |
| `stopAt`                | `Date`                            | `undefined`  | Stop processing at specific date (for historical processing)                                                                                                              |
| `baseUrlOverride`       | `string`                          | `undefined`  | Override Flowcore API base URL                                                                                                                                            |
| `noTranslation`         | `boolean`                         | `false`      | Treat tenant, data core, flow type, and event type values as IDs and skip translation                                                                                     |
| `directMode`            | `boolean`                         | `false`      | Enables direct API execution mode, bypassing intermediary gateways; recommended for dedicated Flowcore clusters to reduce latency (often used with `noTranslation: true`) |
| `pulse`                 | `object`                          | `undefined`  | Periodically send pump position, buffer, counters, and uptime to a Flowcore control-plane endpoint                                                                        |

## 🔧 API Reference

### FlowcoreDataSource Methods

The `FlowcoreDataSource` class provides several useful methods for historical processing and data exploration. This can
be used to replay events from the beginning or a specific time in the State Manager.

#### Time Bucket Management

```typescript
// Get all available time buckets for your event types
const timeBuckets = await dataSource.getTimeBuckets()
console.log(`Found ${timeBuckets.length} time buckets`)
console.log(`First: ${timeBuckets[0]}, Last: ${timeBuckets[timeBuckets.length - 1]}`)

// Get the next time bucket after a specific one
const nextBucket = await dataSource.getNextTimeBucket("20240101120000")

// Get the closest time bucket to a specific time (forward or backward)
const closestBucket = await dataSource.getClosestTimeBucket("20240101150000") // Forward
const previousBucket = await dataSource.getClosestTimeBucket("20240101150000", true) // Backward
```

#### Direct Event Access

```typescript
// Get events directly from a specific state
const events = await dataSource.getEvents(
  { timeBucket: "20240101120000", eventId: "some-event-id" },
  100, // amount
  undefined, // toEventId (optional)
  undefined, // cursor (optional)
  false, // includeSensitiveData
)

console.log(`Retrieved ${events.events.length} events`)
```

#### Resource Information

```typescript
// Access configured names
console.log(dataSource.tenant) // "your-tenant-name"
console.log(dataSource.dataCore) // "your-data-core"
console.log(dataSource.flowType) // "your-flow-type"
console.log(dataSource.eventTypes) // ["event-type-1", "event-type-2"]

// Get translated IDs (useful for debugging or direct API calls)
const tenantId = await dataSource.getTenantId()
const dataCoreId = await dataSource.getDataCoreId()
const flowTypeId = await dataSource.getFlowTypeId()
const eventTypeIds = await dataSource.getEventTypeIds()
```

### FlowcoreDataPump Methods

The `FlowcoreDataPump` provides control methods for both push and pull modes:

#### Pump Control

```typescript
// Check if pump is running
if (dataPump.isRunning) {
  console.log("Pump is running")
}

// Blocking: resolves when the fetch loop stops and rejects on a fetch error.
await dataPump.start()

// Background/self-healing: fetch errors retry with exponential backoff.
await dataPump.start((error) => {
  if (error) console.error("Data Pump stopped", error)
})

// Stop immediately. The in-memory buffer is cleared rather than drained.
dataPump.stop()

// Restart from a specific position. The bucket must contain exactly 14 digits.
dataPump.restart({
  timeBucket: "20240101120000",
  eventId: "specific-event-id",
})
```

Restart clears the current buffer and refreshes available time buckets. In 0.22.x, the optional
`restart(state, stopAt)` argument updates the option but does not rebuild the internal stop boundary. Create a new pump
when changing `stopAt`.

#### Pull Mode Methods (Manual Processing)

```typescript
const events = await dataPump.reserve(10) // Mark 10 events as reserved for processing

await dataPump.acknowledge(events.map((e) => e.eventId))

// Terminal: removes these events. This does not retry them.
await dataPump.fail(["event-id-1", "event-id-2"])

// Called only after timeout-based redelivery exceeds maxRedeliveryCount.
dataPump.onFinalyFailed(async (failedEvents) => {
  console.log(`${failedEvents.length} events permanently failed`)
})
```

`onFinalyFailed` is the current public spelling.

## Pulse Status Reporting

The optional pulse emitter reports pump status to a Flowcore control-plane endpoint:

```typescript
const dataPump = FlowcoreDataPump.create({
  // ...required options...
  pulse: {
    url: process.env.FLOWCORE_CONTROL_PLANE_URL!,
    pathwayId: process.env.FLOWCORE_PATHWAY_ID!,
    sourceId: process.env.PUMP_SOURCE_ID,
    intervalMs: 30_000,
    successLogLevel: "debug",
    failureLogLevel: "warn",
  },
})
```

Pulses include the current bucket and event ID, live status, buffer depth and reserved count, payload bytes, cumulative
pulled, acknowledged and failed counts, and uptime. The first pulse is randomly staggered within the interval so replicas
do not all report at once. Pulse failures are logged and do not stop processing. `pulse.url` is independent of
`baseUrlOverride`.

## Monitoring & Metrics

The data pump exposes Prometheus-compatible metrics:

```typescript
import { dataPumpPromRegistry } from "@flowcore/data-pump"

// Express.js example
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", dataPumpPromRegistry.contentType)
  res.end(await dataPumpPromRegistry.metrics())
})
```

### Available Metrics

- `flowcore_data_pump_buffer_events_gauge` - Events in buffer
- `flowcore_data_pump_buffer_reserved_events_gauge` - Reserved events
- `flowcore_data_pump_buffer_size_bytes_gauge` - Buffer size in bytes
- `flowcore_data_pump_events_acknowledged_counter` - Successfully processed events
- `flowcore_data_pump_events_failed_counter` - Failed events
- `flowcore_data_pump_events_pulled_size_bytes_counter` - Data throughput
- `flowcore_data_pump_sdk_commands_counter` - API calls to Flowcore
- `flowcore_data_pump_cluster_active_workers_gauge` - Connected cluster workers
- `flowcore_data_pump_cluster_leader_status_gauge` - `1` on the elected leader
- `flowcore_data_pump_cluster_events_distributed_counter` - Events sent to workers
- `flowcore_data_pump_cluster_worker_acks_counter` - Successful worker batch acknowledgments
- `flowcore_data_pump_cluster_worker_fails_counter` - Failed worker batch deliveries

Pump event metrics use `tenant`, `data_core`, `flow_type`, and `event_type` labels. The SDK command counter uses only
`command`. Cluster metrics currently have no tenant or event labels and are process-local.
