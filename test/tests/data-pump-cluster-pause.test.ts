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
  it("builds the new leader's pump already paused", () => {
    const cluster = createCluster()
    const first = createFakePump()
    const createdWith: Array<{ paused?: boolean }> = []
    const spy = jest.spyOn(FlowcoreDataPump, "create").mockImplementation((options) => {
      createdWith.push(options as { paused?: boolean })
      return first.pump
    })

    try {
      const internals = cluster as unknown as { startPumpAsLeader(): void }

      // This instance becomes leader and starts delivering.
      internals.startPumpAsLeader()
      expect(first.calls.start).toBe(1)
      expect(createdWith[0]?.paused).toBe(false)

      // An operator pauses the pathway.
      cluster.pause()
      expect(cluster.isPaused).toBe(true)
      expect(first.calls.pause).toBe(1)

      // The lease is lost and a new leader builds a BRAND NEW pump. Without the
      // cluster-level flag this pump would start delivering at full rate with no
      // operator action and no log line saying the pause was dropped.
      //
      // It must be born paused, not paused a tick after start() — otherwise the new
      // leader delivers events in the gap between the two calls.
      const second = createFakePump()
      spy.mockImplementation((options) => {
        createdWith.push(options as { paused?: boolean })
        return second.pump
      })
      internals.startPumpAsLeader()

      expect(createdWith[1]?.paused).toBe(true)
      expect(second.calls.start).toBe(1)
      expect(cluster.isPaused).toBe(true)
    } finally {
      spy.mockRestore()
    }
  })

  it("resume clears the flag so a later leader starts unpaused", () => {
    const cluster = createCluster()
    const first = createFakePump()
    const createdWith: Array<{ paused?: boolean }> = []
    const spy = jest.spyOn(FlowcoreDataPump, "create").mockImplementation((options) => {
      createdWith.push(options as { paused?: boolean })
      return first.pump
    })

    try {
      const internals = cluster as unknown as { startPumpAsLeader(): void }
      internals.startPumpAsLeader()
      cluster.pause()
      cluster.resume()
      expect(cluster.isPaused).toBe(false)
      expect(first.calls.resume).toBe(1)

      spy.mockImplementation((options) => {
        createdWith.push(options as { paused?: boolean })
        return createFakePump().pump
      })
      internals.startPumpAsLeader()
      expect(createdWith[1]?.paused).toBe(false)
    } finally {
      spy.mockRestore()
    }
  })

  it("honours a paused flag supplied in the cluster options", () => {
    const cluster = new FlowcoreDataPumpCluster({
      auth: { getBearerToken: () => Promise.resolve("fake") },
      dataSource: { tenant: "test", dataCore: "dc", flowType: "ft", eventTypes: ["ev"] },
      stateManager: { getState: () => null },
      coordinator: new NoopCoordinator(),
      advertisedAddress: "ws://localhost:8080",
      notifier: { type: "poller", intervalMs: 60_000 },
      paused: true,
    })
    expect(cluster.isPaused).toBe(true)
  })
})
