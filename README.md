# Flowcore Data Pump Client

A reliable, high-performance TypeScript client for streaming and processing events from the Flowcore platform. Built for
real-time event processing with automatic retry, buffering, and state management.

[![JSR](https://jsr.io/badges/@flowcore/data-pump)](https://jsr.io/@flowcore/data-pump)
[![NPM Version](https://img.shields.io/npm/v/@flowcore/data-pump)](https://www.npmjs.com/package/@flowcore/data-pump)

## Simple Example Setup

```typescript
import { FlowcoreDataPump } from "@flowcore/data-pump"

const dataPump = FlowcoreDataPump.create({
  // make sure that api key has sufficient IAM permissions to access streaming operations (COLLABORATOR is an example of a role that has sufficient permissions)
  // there are two ways to authenticate. API key and OIDC/Bearer token.
  auth: {
    apiKey: "your-api-key",
    apiKeyId: "your-api-key-id",
  },
  dataSource: {
    tenant: "your-tenant-name", // this should always be the tenant name, not the tenant id
    dataCore: "your-data-core", // if noTranslation is false, this should be the data core name, not the id
    flowType: "your-flow-type", // if noTranslation is false, this should be the flow type name, not the id
    eventTypes: ["event-type-1", "event-type-2", "event-type-3"], // if noTranslation is false, this should be the event type names, not the ids
  },
  noTranslation: false, // if true (the data core, flow types, and event types) names will not be translated to ids. Use this for performance reasons.
  stateManager: {
    getState: () => null, // Start in live mode
    setState: (state) => console.log("Position:", state),
  },
  processor: { // use this for automatic event lifecycle management
    handler: async (events) => {
      console.log(`Processing ${events.length} events`)
      // Your event processing logic here
    },
  },
  notifier: { type: "websocket" },

  directMode: false, // To interact with the Flowcore API more directly. This is a dedicated cluster feature.
  bufferSize: 100,
  logger: {
    debug: (msg) => console.log(`[DEBUG] ${msg}`),
    info: (msg) => console.log(`[INFO] ${msg}`),
    warn: (msg) => console.warn(`[WARN] ${msg}`),
    error: (msg) => console.error(`[ERROR] ${msg}`),
  },
})

await dataPump.start()
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

The pump **tracks its exact position** using time buckets + event IDs:

```typescript
// Current position in event stream
{
  timeBucket: "20240315140000",           // Currently processing 2 PM hour
  eventId: "abc-123-def-456"              // Last successfully processed event
}
```

**Critical capabilities:**

- **Crash recovery**: Restart exactly where you left off (no duplicate processing)
- **Horizontal scaling**: Multiple instances can coordinate using shared database state
- **Historical processing**: Start from any point in time (hours, days, months ago)
- **Deployment safety**: Updates don't lose processing progress

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
- **Behavior**: Process new events as they arrive (real-time)
- **Use case**: Production event processing, real-time analytics

#### **Historical Mode**

- **When**: `stateManager.getState()` returns `{ timeBucket, eventId }`
- **Behavior**: Process events from specific point in time
- **Use case**: Backfill data, debugging, data migration, replaying scenarios

### **⚡ Concurrency & Parallel Processing**

Control how many events process simultaneously:

```typescript
processor: {
  concurrency: 5,  // Process up to 5 events in parallel
  handler: async (events) => {
    // This batch could contain 1-5 events
    // All processed in parallel for efficiency
  }
}
```

**Performance considerations:**

- **Higher concurrency**: Faster processing, more resource usage
- **Lower concurrency**: More controlled, better for external API limits
- **Optimal range**: Usually 5-20 for most applications

### **🔧 Failure Handling & Retries**

Automatic resilience for production systems:

```
Event fails → Retry 1 → Retry 2 → Retry 3 → Permanent failure
     ↓           ↓         ↓         ↓            ↓
 Log error   Log retry  Log retry  Log retry   failedHandler()
```

**Configurable behavior:**

- `maxRedeliveryCount`: How many retries before giving up
- `failedHandler`: Your code to handle permanently failed events
- **Exponential backoff**: Automatic delays between retries

## Usage Patterns

### Push Mode (Automatic Lifecycle Management)

**Best for most use cases** - You focus on business logic, the pump handles everything else automatically.

- **You handle**: Writing your event processing logic
- **Pump handles**: Reserve → Process → Acknowledge → Retry on failures
- **Use when**: Standard event processing with simple error handling

```typescript
const dataPump = FlowcoreDataPump.create({
  auth: {/* auth config */},
  dataSource: {/* data source config */},
  stateManager: {/* state management */},
  processor: {
    concurrency: 5,
    handler: async (events) => {
      // You only write business logic here
      for (const event of events) {
        await processEvent(event)
      }
      // ✅ Pump automatically acknowledges if successful
      // ❌ Pump automatically retries if errors thrown
    },
    failedHandler: async (failedEvents) => {
      // Handle events that permanently failed after all retries
      await logFailedEvents(failedEvents)
    },
  },
  bufferSize: 1000,
  maxRedeliveryCount: 3,
})

await dataPump.start() // Just start and it runs automatically!
```

### Pull Mode (Manual Lifecycle Control)

**For advanced scenarios** - You control the entire event lifecycle manually.

- **You handle**: Reserve → Process → Acknowledge/Fail → Custom retry logic
- **Pump provides**: Raw event access and buffer management
- **Use when**: Complex error handling, partial batch failures, or custom acknowledgment logic

```typescript
const dataPump = FlowcoreDataPump.create({
  auth: {/* auth config */},
  dataSource: {/* data source config */},
  stateManager: {/* state management */},
  // ❌ No processor = manual mode
})

await dataPump.start()

// You manually control the entire event lifecycle
while (dataPump.isRunning) {
  try {
    // 1️⃣ YOU manually reserve events from buffer
    const events = await dataPump.reserve(10)

    if (events.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 1000))
      continue
    }

    // 2️⃣ YOU handle business logic with custom error handling
    const results = await Promise.allSettled(
      events.map((event) => processEvent(event)),
    )

    // 3️⃣ YOU decide what succeeded vs failed
    const successfulIds = []
    const failedIds = []

    results.forEach((result, index) => {
      const eventId = events[index].eventId
      if (result.status === "fulfilled") {
        successfulIds.push(eventId)
      } else {
        failedIds.push(eventId)
      }
    })

    // 4️⃣ YOU manually acknowledge successful events (removes from buffer)
    if (successfulIds.length > 0) {
      await dataPump.acknowledge(successfulIds)
    }

    // 5️⃣ YOU manually mark failed events for retry
    if (failedIds.length > 0) {
      await dataPump.fail(failedIds)
    }
  } catch (error) {
    console.error("Processing error:", error)
  }
}
```

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
  apiKey: "your-api-key",
  apiKeyId: "your-api-key-id"
}
```

> **💡 Important:** Make sure your API key has sufficient IAM permissions. The key should have **COLLABORATOR** role or
> other IAM permissions that have access to streaming operations.

### OIDC/Bearer Token Authentication

```typescript
import { oidcClient } from "@flowcore/oidc-client"

const oidc = oidcClient({
  clientId: "your-client-id",
  clientSecret: "your-client-secret",
})

auth: {
  getBearerToken: ;
  ;(() => oidc.getToken().then((token) => token.accessToken))
}
```

## State Management

The state manager tracks your processing position so you can resume exactly where you left off after restarts, crashes,
or deployments. It prevents duplicate processing and ensures no events are lost.

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

- `null` → Start in **live mode** (process new events only)
- `{ timeBucket, eventId }` → Start from **specific position** (historical processing)

### Precise Positioning with TimeUuid

FlowcoreDataPump includes utilities for converting between timestamps and event IDs for precise positioning:

```typescript
import { TimeUuid } from "@flowcore/time-uuid"

// Generate event ID from specific timestamp
const eventId = TimeUuid.fromDate(new Date("2024-01-01T12:30:00Z")).toString()

// Start processing from timestamp (doesn't need to match existing event)
const stateManager = {
  stateManager: {
    getState: () => ({
      timeBucket: "20240101120000", // Hour bucket: 2024-01-01 12:00
      eventId: eventId, // Start from first event AFTER 12:30:00
    }),
    setState: (state) => {
      // Extract timestamp from event ID
      const timestamp = TimeUuid.fromString(state.eventId).getDate()
      console.log(`Processed up to: ${timestamp.toISOString()}`)
    },
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
import { readFileSync, writeFileSync } from 'fs';

stateManager: {
  getState: () => {
    try {
      const data = readFileSync('pump-state.json', 'utf8');
      const state = JSON.parse(data);
      console.log('Resuming from saved state:', state);
      return state;
    } catch (error) {
      console.log('No previous state found, starting fresh');
      return null; // Start in live mode
    }
  },
  setState: (state) => {
    try {
      writeFileSync('pump-state.json', JSON.stringify(state, null, 2));
      console.log('State saved:', state);
    } catch (error) {
      console.error('Failed to save state:', error);
      // Consider throwing to stop pump if state saving is critical
    }
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
  id VARCHAR(50) PRIMARY KEY,      -- Instance identifier
  time_bucket VARCHAR(14) NOT NULL, -- "yyyyMMddHH0000"
  event_id VARCHAR(255),           -- Last processed event ID
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
      // Critical decision: start fresh or fail fast?
      return null; // Start fresh if DB is down
      // throw error; // Or fail fast if state is critical
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
- Supports multiple instances
- Atomic updates with transactions
- Can be backed up with your database
- Enables horizontal scaling

**⚠️ Considerations:**

- Database dependency
- Network latency on state updates
- Requires error handling strategy

### State Management Patterns

#### Multi-Instance Coordination

```typescript
// Each instance processes different event types
const instanceId = `processor-${process.env.INSTANCE_ID}`;

stateManager: {
  getState: async () => {
    const result = await db.query(
      'SELECT time_bucket, event_id FROM flowcore_pump_state WHERE id = ?',
      [instanceId] // Each instance has unique state
    );
    return result[0] || null;
  },
  setState: async (state) => {
    await db.query(
      'INSERT OR REPLACE INTO flowcore_pump_state (id, time_bucket, event_id) VALUES (?, ?, ?)',
      [instanceId, state.timeBucket, state.eventId]
    );
  }
}
```

#### Checkpoint Strategy

```typescript
// Save state every N events for performance
let eventCount = 0;
const CHECKPOINT_INTERVAL = 100;

stateManager: {
  getState: () => loadStateFromFile(),
  setState: (state) => {
    eventCount++;
    // Only save every 100 events to reduce I/O
    if (eventCount % CHECKPOINT_INTERVAL === 0) {
      saveStateToFile(state);
      console.log(`Checkpoint saved after ${eventCount} events`);
    }
  }
}
```

### Choosing a State Manager

| Scenario                        | Recommended              | Reason                             |
| ------------------------------- | ------------------------ | ---------------------------------- |
| **Local development**           | Memory                   | Fast iteration, no setup           |
| **Testing/CI**                  | Memory                   | Clean state per test run           |
| **Single instance, simple**     | File-based               | Persistence without DB complexity  |
| **Production, single instance** | Database                 | Reliability and backup integration |
| **Multi-instance**              | Database                 | Shared state coordination          |
| **High-throughput**             | Database + Checkpointing | Performance optimization           |
| **Mission-critical**            | Database + Monitoring    | Full observability stack           |

## Notification Methods

### WebSocket (Default)

Real-time notifications via Flowcore's notification service:

```typescript
notifier: {
  type: "websocket"
}
```

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
  intervalMs: 5000 // Poll every 5 seconds
}
```

## ⚙️ Configuration Reference

| Option                  | Type                              | Default      | Description                                                                                                                                                               |
| ----------------------- | --------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `auth`                  | `FlowcoreDataPumpAuth`            | **Required** | Authentication configuration (API key or Bearer token)                                                                                                                    |
| `dataSource`            | `FlowcoreDataPumpDataSource`      | **Required** | Data source configuration (tenant, dataCore, flowType, eventTypes)                                                                                                        |
| `stateManager`          | `FlowcoreDataPumpStateManager`    | **Required** | State persistence configuration                                                                                                                                           |
| `bufferSize`            | `number`                          | `1000`       | Maximum events to buffer in memory                                                                                                                                        |
| `maxRedeliveryCount`    | `number`                          | `3`          | Max retry attempts before marking event as failed                                                                                                                         |
| `achknowledgeTimeoutMs` | `number`                          | `5000`       | Timeout for event acknowledgment                                                                                                                                          |
| `includeSensitiveData`  | `boolean`                         | `false`      | Include sensitive data in events                                                                                                                                          |
| `processor`             | `FlowcoreDataPumpProcessor`       | `undefined`  | Automatic processing configuration                                                                                                                                        |
| `notifier`              | `FlowcoreDataPumpNotifierOptions` | `websocket`  | Notification method configuration                                                                                                                                         |
| `logger`                | `FlowcoreLogger`                  | `undefined`  | Custom logger implementation                                                                                                                                              |
| `stopAt`                | `Date`                            | `undefined`  | Stop processing at specific date (for historical processing)                                                                                                              |
| `baseUrlOverride`       | `string`                          | `undefined`  | Override Flowcore API base URL                                                                                                                                            |
| `noTranslation`         | `boolean`                         | `false`      | Skip name-to-ID translation. This is mostly for performance reasons.                                                                                                      |
| `directMode`            | `boolean`                         | `false`      | Enables direct API execution mode, bypassing intermediary gateways; recommended for dedicated Flowcore clusters to reduce latency (often used with `noTranslation: true`) |

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

// Start the pump
await dataPump.start()

// Stop the pump
dataPump.stop()

// Restart from a specific position - stops current processing and resumes from new location
// This is useful for backfill scenarios, error recovery, and dynamic repositioning
dataPump.restart({
  timeBucket: "20240101120000", // Required: target time bucket
  eventId: "specific-event-id", // Optional: specific event (omit to start from first event in bucket)
})

// Restart with a new stop date - change both position AND stop condition
dataPump.restart(
  { timeBucket: "20240101120000" },
  new Date("2024-01-02"), // New stopAt date (or null to remove limit)
)

// Common restart patterns:
// 1. Jump to historical data: dataPump.restart({ timeBucket: firstTimeBucket })
// 2. Reprocess from error point: dataPump.restart(lastKnownGoodState)
// 3. Start backfill operation: dataPump.restart({ timeBucket: "20240101000000" }, endDate)
```

#### Pull Mode Methods (Manual Processing)

```typescript
const events = await dataPump.reserve(10) // Mark 10 events as reserved for processing

await dataPump.acknowledge(events.map((e) => e.eventId))

await dataPump.fail(["event-id-1", "event-id-2"])

// Handle events that permanently failed (exceeded retry limit)
dataPump.onFinalyFailed(async (failedEvents) => {
  console.log(`${failedEvents.length} events permanently failed`)
})
```

## Monitoring & Metrics

The data pump exposes Prometheus-compatible metrics:

```typescript
import { dataPumpPromRegistry } from "@flowcore/data-pump"

// Express.js example
app.get("/metrics", (req, res) => {
  res.set("Content-Type", dataPumpPromRegistry.contentType)
  res.end(dataPumpPromRegistry.metrics())
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

All metrics include labels: `tenant`, `data_core`, `flow_type`, `event_type`
