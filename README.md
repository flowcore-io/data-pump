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
