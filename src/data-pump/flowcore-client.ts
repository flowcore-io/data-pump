import { type Command, FlowcoreClient } from "@flowcore/sdk"
import type { FlowcoreDataPumpAuth } from "./types.ts"
import { metrics } from "./metrics.ts"

class FlowcoreClientWithMetrics extends FlowcoreClient {
  public override execute<Input, Output>(command: Command<Input, Output>): Promise<Output> {
    metrics.sdkCommandsCounter.inc({ command: command.constructor.name }, 1)
    return super.execute(command)
  }
}

export function getFlowcoreClient(auth: FlowcoreDataPumpAuth) {
  const client = new FlowcoreClientWithMetrics(auth)
  return client
}
