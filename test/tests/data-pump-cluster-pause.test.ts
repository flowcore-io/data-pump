import { describe, expect, it, jest } from "bun:test"
import { FlowcoreDataPump } from "../../src/data-pump/data-pump.ts"
import { FlowcoreDataPumpCluster } from "../../src/data-pump/data-pump-cluster.ts"
import type { FlowcoreDataPumpCoordinator } from "../../src/data-pump/types.ts"

class NoopCoordinator implements FlowcoreDataPumpCoordinator {
  acquireLease(): Promise<boolean> {
    return Promise.resolve(true)
  }
  renewLease(): Promise<boolean> {
    return Promise.resolve(true)
  }
  releaseLease(): Promise<void> {
    return Promise.resolve()
  }
  register(): Promise<void> {
    return Promise.resolve()
  }
  heartbeat(): Promise<void> {
    return Promise.resolve()
  }
  unregister(): Promise<void> {
    return Promise.resolve()
  }
  getInstances(): Promise<Array<{ instanceId: string; address: string }>> {
    return Promise.resolve([])
  }
}

function createCluster() {
  return new FlowcoreDataPumpCluster({
    auth: { getBearerToken: () => Promise.resolve("fake") },
    dataSource: { tenant: "test", dataCore: "dc", flowType: "ft", eventTypes: ["ev"] },
    stateManager: { getState: () => null },
    coordinator: new NoopCoordinator(),
    advertisedAddress: "ws://localhost:8080",
    notifier: { type: "poller", intervalMs: 60_000 },
  })
}

/** Stand-in for a real pump, so the leader path can be driven without any network. */
function createFakePump() {
  const calls = { pause: 0, resume: 0, start: 0 }
  return {
    calls,
    pump: {
      pause: () => {
        calls.pause++
      },
      resume: () => {
        calls.resume++
      },
      start: () => {
        calls.start++
        return Promise.resolve()
      },
      stop: () => {},
    } as unknown as FlowcoreDataPump,
  }
}

describe("cluster pause survives a leader change", () => {
  it("re-applies the pause to the pump a new leader builds", () => {
    const cluster = createCluster()
    const first = createFakePump()
    const spy = jest.spyOn(FlowcoreDataPump, "create").mockReturnValue(first.pump)

    try {
      const internals = cluster as unknown as { startPumpAsLeader(): void }

      // This instance becomes leader and starts delivering.
      internals.startPumpAsLeader()
      expect(first.calls.start).toBe(1)
      expect(first.calls.pause).toBe(0)

      // An operator pauses the pathway.
      cluster.pause()
      expect(cluster.isPaused).toBe(true)
      expect(first.calls.pause).toBe(1)

      // The lease is lost and a new leader builds a BRAND NEW pump. Without the
      // cluster-level flag this pump would start delivering at full rate with no
      // operator action and no log line saying the pause was dropped.
      const second = createFakePump()
      spy.mockReturnValue(second.pump)
      internals.startPumpAsLeader()

      expect(second.calls.pause).toBe(1)
      expect(second.calls.start).toBe(1)
      expect(cluster.isPaused).toBe(true)
    } finally {
      spy.mockRestore()
    }
  })

  it("resume clears the flag so a later leader starts unpaused", () => {
    const cluster = createCluster()
    const first = createFakePump()
    const spy = jest.spyOn(FlowcoreDataPump, "create").mockReturnValue(first.pump)

    try {
      const internals = cluster as unknown as { startPumpAsLeader(): void }
      internals.startPumpAsLeader()
      cluster.pause()
      cluster.resume()
      expect(cluster.isPaused).toBe(false)
      expect(first.calls.resume).toBe(1)

      const second = createFakePump()
      spy.mockReturnValue(second.pump)
      internals.startPumpAsLeader()
      expect(second.calls.pause).toBe(0)
    } finally {
      spy.mockRestore()
    }
  })
})
