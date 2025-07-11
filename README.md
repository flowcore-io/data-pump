# Flowcore Data Pump Client

A reliable, high-performance TypeScript client for streaming and processing events from the Flowcore platform. Built for
real-time event processing with automatic retry, buffering, and state management.

[![JSR](https://jsr.io/badges/@flowcore/data-pump)](https://jsr.io/@flowcore/data-pump)
[![NPM Version](https://img.shields.io/npm/v/@flowcore/data-pump)](https://www.npmjs.com/package/@flowcore/data-pump)

## Example Setup

```typescript
import { FlowcoreDataPump } from "@flowcore/data-pump"

const dataPump = FlowcoreDataPump.create({
  // make sure that api key has sufficient permissions to access streaming operations (COLLABORATOR is an example of a role that has sufficient permissions)
  auth: {
    apiKey: "your-api-key",
    apiKeyId: "your-api-key-id",
  },
  dataSource: {
    tenant: "your-tenant-name",
    dataCore: "your-data-core-name",
    flowType: "your-flow-type-name",
    eventTypes: ["event-type-name-1", "event-type-name-2", "event-type-name-3"],
  },
  stateManager: {
    getState: () => null, // Start in live mode
    setState: (state) => console.log("Position:", state),
  },
  processor: {
    handler: async (events) => {
      console.log(`Processing ${events.length} events`)
      // Your event processing logic here
    },
  },
  notifier: { type: "websocket" },

  directMode: false, // To interact with the Flowcore API more directly. This is a dedicated cluster feature.
  noTranslation: false, // Don't convert names to ids. This is a dedicated cluster feature.
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

## 📦 Installation

### Deno

```typescript
import { FlowcoreDataPump } from "jsr:@flowcore/data-pump"
```

### Node.js

```bash
npm install @flowcore/data-pump
```

```typescript
import { FlowcoreDataPump } from "@flowcore/data-pump"
```

## 🔑 Key Concepts

### **Time Buckets**

Events are organized in hourly time buckets (format: `yyyyMMddHH0000`). This enables:

- Precise resumption from any point in time
- Efficient historical data processing
- Handling of catch-up scenarios

### **State Management**

The data pump tracks its position using time buckets and event IDs, allowing:

- **Crash recovery**: Resume exactly where you left off
- **Horizontal scaling**: Multiple instances with shared state
- **Historical processing**: Process events from specific time ranges

### **Buffer Management**

Events are buffered locally with configurable sizes to handle:

- **Backpressure**: When processing is slower than event arrival
- **Batch processing**: Process multiple events together efficiently
- **Flow control**: Automatic throttling based on buffer capacity

## 🎯 Usage Patterns

### Push Mode (Automatic Processing)

The data pump automatically reserves, processes, and acknowledges events:

```typescript
const dataPump = FlowcoreDataPump.create({
  auth: {/* auth config */},
  dataSource: {/* data source config */},
  stateManager: {/* state management */},
  processor: {
    concurrency: 5,
    handler: async (events) => {
      for (const event of events) {
        await processEvent(event)
      }
    },
    failedHandler: async (failedEvents) => {
      // Handle permanently failed events
      await logFailedEvents(failedEvents)
    },
  },
  bufferSize: 1000,
  maxRedeliveryCount: 3,
})

await dataPump.start()
```

### Pull Mode (Manual Processing)

For full control over event processing lifecycle:

```typescript
const dataPump = FlowcoreDataPump.create({
  auth: {/* auth config */},
  dataSource: {/* data source config */},
  stateManager: {/* state management */},
  // No processor - manual mode
})

await dataPump.start()

// Manual processing loop
while (dataPump.isRunning) {
  try {
    // Reserve events for processing
    const events = await dataPump.reserve(10)

    if (events.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 1000))
      continue
    }

    // Process events
    const results = await Promise.allSettled(
      events.map((event) => processEvent(event)),
    )

    // Separate successful and failed events
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

    // Acknowledge successful events
    if (successfulIds.length > 0) {
      await dataPump.acknowledge(successfulIds)
    }

    // Mark failed events
    if (failedIds.length > 0) {
      await dataPump.fail(failedIds)
    }
  } catch (error) {
    console.error("Processing error:", error)
  }
}
```

## 🔐 Authentication

### API Key Authentication

```typescript
auth: {
  apiKey: "your-api-key",
  apiKeyId: "your-api-key-id"
}
```

> **💡 Important:** Make sure your API key has sufficient permissions. The key should have **COLLABORATOR** role or
> equivalent permissions to access streaming operations.

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

## 💾 State Management

### Memory State Manager (Development)

```typescript
let currentState = null;

stateManager: {
  getState: () => currentState,
  setState: (state) => { currentState = state; }
}
```

### File-based State Manager

```typescript
import { readFileSync, writeFileSync } from 'fs';

stateManager: {
  getState: () => {
    try {
      const data = readFileSync('pump-state.json', 'utf8');
      return JSON.parse(data);
    } catch {
      return null; // Start from latest
    }
  },
  setState: (state) => {
    writeFileSync('pump-state.json', JSON.stringify(state));
  }
}
```

### Database State Manager

```typescript
stateManager: {
  getState: async () => {
    const result = await db.query(
      'SELECT time_bucket, event_id FROM pump_state WHERE id = ?', 
      ['main']
    );
    return result[0] || null;
  },
  setState: async (state) => {
    await db.query(
      'INSERT OR REPLACE INTO pump_state (id, time_bucket, event_id) VALUES (?, ?, ?)',
      ['main', state.timeBucket, state.eventId]
    );
  }
}
```

## 🔔 Notification Methods

### WebSocket (Default)

Real-time notifications via Flowcore's notification service:

```typescript
notifier: {
  type: "websocket"
} // Default - can be omitted
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

| Option                  | Type                              | Default      | Description                                                                                                                                                                                                     |
| ----------------------- | --------------------------------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `auth`                  | `FlowcoreDataPumpAuth`            | **Required** | Authentication configuration (API key or Bearer token)                                                                                                                                                          |
| `dataSource`            | `FlowcoreDataPumpDataSource`      | **Required** | Data source configuration (tenant, dataCore, flowType, eventTypes)                                                                                                                                              |
| `stateManager`          | `FlowcoreDataPumpStateManager`    | **Required** | State persistence configuration                                                                                                                                                                                 |
| `bufferSize`            | `number`                          | `1000`       | Maximum events to buffer in memory                                                                                                                                                                              |
| `maxRedeliveryCount`    | `number`                          | `3`          | Max retry attempts before marking event as failed                                                                                                                                                               |
| `achknowledgeTimeoutMs` | `number`                          | `5000`       | Timeout for event acknowledgment                                                                                                                                                                                |
| `includeSensitiveData`  | `boolean`                         | `false`      | Include sensitive data in events                                                                                                                                                                                |
| `processor`             | `FlowcoreDataPumpProcessor`       | `undefined`  | Automatic processing configuration                                                                                                                                                                              |
| `notifier`              | `FlowcoreDataPumpNotifierOptions` | `websocket`  | Notification method configuration                                                                                                                                                                               |
| `logger`                | `FlowcoreLogger`                  | `undefined`  | Custom logger implementation                                                                                                                                                                                    |
| `stopAt`                | `Date`                            | `undefined`  | Stop processing at specific date (for historical processing)                                                                                                                                                    |
| `baseUrlOverride`       | `string`                          | `undefined`  | Override Flowcore API base URL                                                                                                                                                                                  |
| `noTranslation`         | `boolean`                         | `false`      | Skip name-to-ID translation. This is mostly used in dedicated clusters. And for performance reasons.                                                                                                            |
| `directMode`            | `boolean`                         | `false`      | Enables direct API execution mode, bypassing intermediary gateways and automatic name-to-ID translations; recommended for dedicated Flowcore clusters to reduce latency (often used with `noTranslation: true`) |

## 📊 Monitoring & Metrics

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

## 🛠️ Advanced Usage

### Historical Processing

Process events from a specific time range:

```typescript
const dataPump = FlowcoreDataPump.create({
  // ... other config
  stopAt: new Date("2024-01-01T12:00:00Z"), // Stop at specific time
  stateManager: {
    getState: () => ({
      timeBucket: "20240101000000", // Start from specific time
      eventId: undefined,
    }),
    setState: (state) => console.log("Progress:", state),
  },
})
```

### Custom Logging

```typescript
const customLogger = {
  debug: (msg, meta) => console.debug(`[DEBUG] ${msg}`, meta),
  info: (msg, meta) => console.info(`[INFO] ${msg}`, meta),
  warn: (msg, meta) => console.warn(`[WARN] ${msg}`, meta),
  error: (msg, meta) => console.error(`[ERROR] ${msg}`, meta),
}

const dataPump = FlowcoreDataPump.create({
  // ... other config
  logger: customLogger,
})
```

### Graceful Shutdown

```typescript
let dataPump

// Start the pump
dataPump = FlowcoreDataPump.create({/* config */})
await dataPump.start()

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("Shutting down gracefully...")
  dataPump.stop()
  process.exit(0)
})
```

#### Understanding Direct Mode

Direct mode optimizes API calls by skipping certain proxy layers and assuming inputs are already in raw ID format (e.g.,
UUIDs for tenants). This is particularly useful in private or dedicated Flowcore environments for better performance. If
enabled without a compatible setup, you may encounter routing or authentication errors. Always combine with
`noTranslation: true` when providing IDs directly in your `dataSource` configuration.

### Restart from Specific Position

```typescript
// Restart processing from a specific state
dataPump.restart({
  timeBucket: "20240101120000",
  eventId: "specific-event-id",
})
```

## 🐛 Troubleshooting

### Common Issues

**Connection Problems**

```typescript
// Check authentication
auth: {
  getBearerToken: ;
  ;(async () => {
    const token = await getToken()
    console.log("Token acquired:", !!token)
    return token
  })
}

// Override base URL for different environments
baseUrlOverride: "https://api.staging.flowcore.io"
```

**Authentication/Permission Issues**

If you're getting authentication errors or access denied:

- **API Key Permissions**: Ensure your API key has **COLLABORATOR** role or higher in your Flowcore tenant
- **Scope Access**: Verify the key has access to the specific tenant, data core, and flow types you're trying to access
- **Key Validity**: Check that your API key hasn't expired and is still active

```typescript
// Test your authentication
const testAuth = async () => {
  try {
    // This should work if your API key has correct permissions
    const dataPump = FlowcoreDataPump.create({
      auth: { apiKey: "your-key", apiKeyId: "your-key-id" },
      dataSource: { tenant: "test", dataCore: "test", flowType: "test", eventTypes: ["test"] },
      stateManager: { getState: () => null, setState: () => {} },
    })
    console.log("Authentication configured successfully")
  } catch (error) {
    console.error("Authentication issue:", error.message)
  }
}
```

**Performance Issues**

```typescript
// Increase buffer size for high-throughput scenarios
bufferSize: 10000,

// Increase concurrency for faster processing
processor: {
  concurrency: 10,
  handler: async (events) => { /* process */ }
}

// Use NATS for better notification performance
notifier: { type: "nats", servers: ["nats://your-nats-server"] }
```

**State Recovery Issues**

```typescript
// Implement robust state management
stateManager: {
  getState: async () => {
    try {
      return await loadStateFromDatabase();
    } catch (error) {
      console.warn('Could not load state, starting fresh:', error);
      return null; // Will start from latest
    }
  },
  setState: async (state) => {
    try {
      await saveStateToDatabase(state);
    } catch (error) {
      console.error('Failed to save state:', error);
      // Consider throwing to stop the pump if state persistence is critical
    }
  }
}
```

**Memory Issues**

```typescript
// Reduce buffer size
bufferSize: 100,

// Process events individually instead of batching
processor: {
  concurrency: 1,
  handler: async (events) => {
    for (const event of events) {
      await processEvent(event);
      // Process one at a time to reduce memory usage
    }
  }
}
```

### Debug Mode

```typescript
// Enable debug logging
logger: {
  debug: console.debug,
  info: console.info,
  warn: console.warn, 
  error: console.error
}
```
