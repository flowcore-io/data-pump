import { type Command, FlowcoreClient } from "@flowcore/sdk"
import type { FlowcoreDataPumpAuth } from "./types.ts"
import { metrics } from "./metrics.ts"

class FlowcoreClientWithMetrics extends FlowcoreClient {
  public override execute<Input, Output>(command: Command<Input, Output>, directMode?: boolean): Promise<Output> {
    metrics.sdkCommandsCounter.inc({ command: command.constructor.name }, 1)
    return super.execute(command, directMode)
  }
}

export function getFlowcoreClient(auth: FlowcoreDataPumpAuth, baseUrlOverride?: string) {
  const client = "apiKey" in auth
    ? new FlowcoreClientWithMetrics({
      apiKey: auth.apiKey,
      apiKeyId: auth.apiKeyId as string,
    })
    : new FlowcoreClientWithMetrics(auth)

  if (baseUrlOverride) {
    client.setBaseUrl(baseUrlOverride)
  }

  return client
}
