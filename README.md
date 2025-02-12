# Flowcore Data Pump Client

## Usage example

```ts
import { createDataPump } from  "@flowcore/data-pump"

const  dataPump = createDataPump({
	auth: {
		clientId: "",
		clientSecret: "",
	},
	dataSource: {
		tenant:  "tenant",
		dataCore:  "data-core",
		flowType:  "data.0",
		eventTypes: ["data.created.0", "data.updated.0", "data.deleted.0"],
	},
	processor: {
		onEvents:  async (events) => {
		await  new  Promise((resolve) =>  setTimeout(resolve, 100))
		console.log(`Got ${events.length} events`)
	},
	logger: console,
})

await dataPump.start()
```
