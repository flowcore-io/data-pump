# Flowcore Data Pump Client

## Usage example

```ts
import { createDataPump } from "@flowcore/data-pump"
import { oidcClient } from "@flowcore/oidc-client"

const oidcClient = oidcClient({
  clientId: "",
  clientSecret: "",
})

const dataPump = createDataPump({
  auth: {
    oidcClient,
  },
  dataSource: {
    tenant: "tenant",
    dataCore: "data-core",
    flowType: "data.0",
    eventTypes: ["data.created.0", "data.updated.0", "data.deleted.0"],
  },
  processor: {
    onEvents: async (events) => {
      await new Promise((resolve) => setTimeout(resolve, 100))
      console.log(`Got ${events.length} events`)
      return true
    },
    onFailedEvents: async (events) => {
      console.error(`Failed ${events.length} events`)
    },
  },
  buffer: {
    size: 10_000,
    threshold: 1_000,
    maxRedeliveryCount: 1,
    achknowledgeTimeoutMs: 1_000,
  },
  logger: console,
})

await dataPump.start()
```
