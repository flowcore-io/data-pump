import { describe, expect, it, spyOn } from "bun:test"
import type { FlowcoreEvent } from "@flowcore/sdk"
import { FlowcoreDataPumpCluster } from "../../src/data-pump/data-pump-cluster.ts"
import type { FlowcoreDataPumpCoordinator } from "../../src/data-pump/types.ts"
import { DeliveryTracker } from "../../src/data-pump/ws-protocol.ts"

const options = {
  auth: { getBearerToken: async () => "synthetic" },
  dataSource: { tenant: "test", dataCore: "test", flowType: "test", eventTypes: ["test"] },
  coordinator: {} as FlowcoreDataPumpCoordinator,
  stateManager: { getState: async () => null, setState: async () => {} },
  advertisedAddress: "ws://localhost:9999",
}
function fixture(timeout?: number) {
  const cluster = new FlowcoreDataPumpCluster({ ...options, workerDeliveryTimeoutMs: timeout })
  const tracker = new DeliveryTracker()
  let deliveryId = ""
  const worker = {
    pendingCount: 0,
    deliveryTracker: tracker,
    connection: {
      isClosed: false,
      send: (message: { deliveryId: string }) => {
        deliveryId = message.deliveryId
      },
    },
  }
  const internals = cluster as unknown as {
    workers: Map<string, typeof worker>
    distributeEventsWs(events: FlowcoreEvent[]): Promise<void>
  }
  internals.workers.set("worker", worker)
  return {
    tracker,
    worker,
    id: () => deliveryId,
    send: () => internals.distributeEventsWs([{ eventId: "synthetic" } as FlowcoreEvent]),
  }
}

describe("WebSocket worker delivery deadline", () => {
  it("preserves the default 30 second deadline", async () => {
    const f = fixture()
    const add = spyOn(f.tracker, "add")
    const result = f.send()
    expect(add.mock.calls[0][2]).toBe(30_000)
    f.tracker.ack(f.id())
    await result
  })
  it("accepts a worker acknowledgement after a simulated throttle wait within the configured deadline", async () => {
    const f = fixture(100)
    const result = f.send()
    await Bun.sleep(40)
    expect(f.tracker.size).toBe(1)
    f.tracker.ack(f.id())
    await result
    expect(f.tracker.size).toBe(0)
    expect(f.worker.pendingCount).toBe(0)
  })
  it("expires at the configured deadline and ignores late acknowledgement of the previous delivery", async () => {
    const f = fixture(100)
    const expired = f.send().then(
      () => null,
      (error: Error) => error,
    )
    const oldId = f.id()
    await Bun.sleep(130)
    expect((await expired)?.message).toContain("timed out")
    const retry = f.send()
    f.tracker.ack(oldId)
    expect(f.tracker.size).toBe(1)
    f.tracker.ack(f.id())
    await retry
    expect(f.worker.pendingCount).toBe(0)
  })
  it("still propagates worker failure immediately", async () => {
    const f = fixture(100)
    const result = f.send().then(
      () => null,
      (error: Error) => error,
    )
    f.tracker.fail(f.id())
    expect((await result)?.message).toContain("failed by worker")
    expect(f.tracker.size).toBe(0)
    expect(f.worker.pendingCount).toBe(0)
  })
  for (const value of [0, -1, 1.5, NaN, Infinity, 2_147_483_648]) {
    it(`rejects invalid timer duration ${value}`, () => {
      expect(() => new FlowcoreDataPumpCluster({ ...options, workerDeliveryTimeoutMs: value })).toThrow(
        "workerDeliveryTimeoutMs",
      )
    })
  }
})
