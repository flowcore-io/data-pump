# Flowcore Data Pump Client

## Usage example

```ts
import { FlowcoreDataPump } from "@flowcore/data-pump"
import { oidcClient } from "@flowcore/oidc-client"

const oidcClient = oidcClient({
  clientId: "",
  clientSecret: "",
})

const dataPump = FlowcoreDataPump.create({
  auth: {
    getBearerToken: () => oidcClient.getToken().then((token) => token.accessToken),
  },
  dataSource: {
    tenant: "tenant",
    dataCore: "data-core",
    flowType: "data.0",
    eventTypes: ["data.created.0", "data.updated.0", "data.deleted.0"],
  },
  processor: {
    concurrency: 1,
    handler: async (events) => {
      console.log(`Got ${events.length} events`)
      await new Promise((resolve) => setTimeout(resolve, 100))
      return true
    },
  },
  bufferSize: 10_000,
  maxRedeliveryCount: 4,
  achknowledgeTimeoutMs: 10_000,
  logger: console,
})

await dataPump.start((error?: Error) => {
  console.log("Datapump ended with: ", error)
})
```

## Configuration

### Concurrency and Acknowledgment Timeout

When using higher concurrency values, the data pump automatically scales the acknowledgment timeout to prevent events
from being prematurely failed due to processing time. The default acknowledgment timeout is calculated as:

```ts
const defaultAckTimeout = Math.max(10_000, concurrency * 2_000) // At least 10s, scale with concurrency
```

For example:

- `concurrency: 1` → `achknowledgeTimeoutMs: 10_000` (10 seconds)
- `concurrency: 10` → `achknowledgeTimeoutMs: 20_000` (20 seconds)
- `concurrency: 50` → `achknowledgeTimeoutMs: 100_000` (100 seconds)

You can override this by explicitly setting `achknowledgeTimeoutMs` in the options.

### Important Notes

- When using high concurrency, ensure your `achknowledgeTimeoutMs` is sufficient for your processing time
- Events that exceed `maxRedeliveryCount` (default: 3) will be permanently removed from the buffer
- The buffer management has been improved to prevent timeout race conditions with high concurrency
