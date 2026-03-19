import { assertEquals, assertRejects, assertThrows } from "@std/assert"
import { beforeEach, describe, it } from "@std/testing/bdd"
import type { FlowcoreEvent } from "@flowcore/sdk"
import { FlowcoreDataPumpCluster } from "../../src/data-pump/data-pump-cluster.ts"
import type { FlowcoreDataPumpCoordinator } from "../../src/data-pump/types.ts"
import {
  DeliveryTracker,
  deserializeMessage,
  serializeMessage,
  type WsAckMessage,
  type WsEventsMessage,
  type WsPingMessage,
} from "../../src/data-pump/ws-protocol.ts"
import type { NatsDistributionReply, NatsDistributionRequest } from "../../src/data-pump/nats-distribution.ts"

// #region Mock Coordinator

class MockCoordinator implements FlowcoreDataPumpCoordinator {
  private leases = new Map<string, { instanceId: string; expiresAt: number }>()
  private instances = new Map<string, { address: string; lastHeartbeat: number }>()

  acquireLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    const existing = this.leases.get(key)
    if (existing && existing.expiresAt > Date.now() && existing.instanceId !== instanceId) {
      return Promise.resolve(false)
    }
    this.leases.set(key, { instanceId, expiresAt: Date.now() + ttlMs })
    return Promise.resolve(true)
  }

  renewLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    const existing = this.leases.get(key)
    if (!existing || existing.instanceId !== instanceId) {
      return Promise.resolve(false)
    }
    existing.expiresAt = Date.now() + ttlMs
    return Promise.resolve(true)
  }

  releaseLease(instanceId: string, key: string): Promise<void> {
    const existing = this.leases.get(key)
    if (existing?.instanceId === instanceId) {
      this.leases.delete(key)
    }
    return Promise.resolve()
  }

  register(instanceId: string, address: string): Promise<void> {
    this.instances.set(instanceId, { address, lastHeartbeat: Date.now() })
    return Promise.resolve()
  }

  heartbeat(instanceId: string): Promise<void> {
    const instance = this.instances.get(instanceId)
    if (instance) {
      instance.lastHeartbeat = Date.now()
    }
    return Promise.resolve()
  }

  unregister(instanceId: string): Promise<void> {
    this.instances.delete(instanceId)
    return Promise.resolve()
  }

  getInstances(staleThresholdMs: number): Promise<Array<{ instanceId: string; address: string }>> {
    const now = Date.now()
    const result: Array<{ instanceId: string; address: string }> = []
    for (const [id, info] of this.instances) {
      if (now - info.lastHeartbeat < staleThresholdMs) {
        result.push({ instanceId: id, address: info.address })
      }
    }
    return Promise.resolve(result)
  }

  // test helpers
  getLeaseHolder(key: string): string | null {
    const lease = this.leases.get(key)
    if (!lease || lease.expiresAt < Date.now()) return null
    return lease.instanceId
  }

  expireLease(key: string): void {
    this.leases.delete(key)
  }

  getRegisteredCount(): number {
    return this.instances.size
  }
}

// #endregion

// #region WS Protocol Tests

describe("WS Protocol", () => {
  describe("serializeMessage / deserializeMessage", () => {
    it("should round-trip an events message", () => {
      const msg: WsEventsMessage = {
        type: "events",
        deliveryId: "d1",
        events: [
          {
            eventId: "e1",
            eventType: "test",
            payload: { foo: "bar" },
            metadata: {},
            timeBucket: "20250101000000",
            tenant: "test-tenant",
            dataCoreId: "test-dc",
            flowType: "test-flow",
            validTime: "2025-01-01T00:00:00Z",
          },
        ],
      }
      const serialized = serializeMessage(msg)
      const deserialized = deserializeMessage(serialized)
      assertEquals(deserialized?.type, "events")
      assertEquals((deserialized as WsEventsMessage).deliveryId, "d1")
      assertEquals((deserialized as WsEventsMessage).events.length, 1)
    })

    it("should round-trip an ack message", () => {
      const msg: WsAckMessage = { type: "ack", deliveryId: "d1", eventIds: ["e1", "e2"] }
      const deserialized = deserializeMessage(serializeMessage(msg))
      assertEquals(deserialized?.type, "ack")
      assertEquals((deserialized as WsAckMessage).eventIds, ["e1", "e2"])
    })

    it("should round-trip a ping message", () => {
      const msg: WsPingMessage = { type: "ping" }
      const deserialized = deserializeMessage(serializeMessage(msg))
      assertEquals(deserialized?.type, "ping")
    })

    it("should return null for invalid JSON", () => {
      assertEquals(deserializeMessage("not json"), null)
    })

    it("should return null for missing type", () => {
      assertEquals(deserializeMessage('{"foo":"bar"}'), null)
    })
  })
})

// #endregion

// #region DeliveryTracker Tests

describe("DeliveryTracker", () => {
  it("should resolve on ack", async () => {
    const tracker = new DeliveryTracker()
    const promise = tracker.add("d1", ["e1"], 5000)
    tracker.ack("d1")
    await promise // should resolve without error
    assertEquals(tracker.size, 0)
  })

  it("should reject on fail", async () => {
    const tracker = new DeliveryTracker()
    const promise = tracker.add("d1", ["e1"], 5000)
    tracker.fail("d1")
    await assertRejects(() => promise, Error, "failed by worker")
    assertEquals(tracker.size, 0)
  })

  it("should reject on timeout", async () => {
    const tracker = new DeliveryTracker()
    const promise = tracker.add("d1", ["e1"], 50)
    await assertRejects(() => promise, Error, "timed out")
    assertEquals(tracker.size, 0)
  })

  it("should reject all pending on rejectAll", async () => {
    const tracker = new DeliveryTracker()
    const p1 = tracker.add("d1", ["e1"], 5000)
    const p2 = tracker.add("d2", ["e2"], 5000)
    assertEquals(tracker.size, 2)

    tracker.rejectAll(new Error("shutdown"))

    await assertRejects(() => p1, Error, "shutdown")
    await assertRejects(() => p2, Error, "shutdown")
    assertEquals(tracker.size, 0)
  })

  it("should return pending event IDs", async () => {
    const tracker = new DeliveryTracker()
    const p1 = tracker.add("d1", ["e1", "e2"], 5000)
    const p2 = tracker.add("d2", ["e3"], 5000)
    const ids = tracker.getPendingEventIds()
    assertEquals(ids.sort(), ["e1", "e2", "e3"])

    // cleanup - catch the rejections
    tracker.rejectAll(new Error("cleanup"))
    await p1.catch(() => {})
    await p2.catch(() => {})
  })

  it("should ignore ack for unknown deliveryId", () => {
    const tracker = new DeliveryTracker()
    tracker.ack("unknown") // should not throw
    assertEquals(tracker.size, 0)
  })
})

// #endregion

// #region Mock Coordinator Tests

describe("MockCoordinator", () => {
  let coordinator: MockCoordinator

  beforeEach(() => {
    coordinator = new MockCoordinator()
  })

  describe("leader election", () => {
    it("should grant lease to first requester", async () => {
      const result = await coordinator.acquireLease("instance-1", "leader", 30000)
      assertEquals(result, true)
      assertEquals(coordinator.getLeaseHolder("leader"), "instance-1")
    })

    it("should deny lease to second requester while first holds it", async () => {
      await coordinator.acquireLease("instance-1", "leader", 30000)
      const result = await coordinator.acquireLease("instance-2", "leader", 30000)
      assertEquals(result, false)
    })

    it("should allow same instance to re-acquire", async () => {
      await coordinator.acquireLease("instance-1", "leader", 30000)
      const result = await coordinator.acquireLease("instance-1", "leader", 30000)
      assertEquals(result, true)
    })

    it("should allow renewal by lease holder", async () => {
      await coordinator.acquireLease("instance-1", "leader", 30000)
      const result = await coordinator.renewLease("instance-1", "leader", 30000)
      assertEquals(result, true)
    })

    it("should deny renewal by non-holder", async () => {
      await coordinator.acquireLease("instance-1", "leader", 30000)
      const result = await coordinator.renewLease("instance-2", "leader", 30000)
      assertEquals(result, false)
    })

    it("should allow new acquisition after release", async () => {
      await coordinator.acquireLease("instance-1", "leader", 30000)
      await coordinator.releaseLease("instance-1", "leader")
      const result = await coordinator.acquireLease("instance-2", "leader", 30000)
      assertEquals(result, true)
    })

    it("should allow new acquisition after expiry", async () => {
      await coordinator.acquireLease("instance-1", "leader", 1) // 1ms TTL
      await new Promise((r) => setTimeout(r, 10))
      const result = await coordinator.acquireLease("instance-2", "leader", 30000)
      assertEquals(result, true)
    })
  })

  describe("instance registry", () => {
    it("should register and discover instances", async () => {
      await coordinator.register("i1", "ws://host1:8080")
      await coordinator.register("i2", "ws://host2:8080")
      const instances = await coordinator.getInstances(30000)
      assertEquals(instances.length, 2)
    })

    it("should unregister instances", async () => {
      await coordinator.register("i1", "ws://host1:8080")
      await coordinator.unregister("i1")
      const instances = await coordinator.getInstances(30000)
      assertEquals(instances.length, 0)
    })

    it("should update heartbeat", async () => {
      await coordinator.register("i1", "ws://host1:8080")
      await coordinator.heartbeat("i1")
      const instances = await coordinator.getInstances(30000)
      assertEquals(instances.length, 1)
    })

    it("should filter stale instances", async () => {
      await coordinator.register("i1", "ws://host1:8080")
      // with a 0ms stale threshold, everything is stale
      await new Promise((r) => setTimeout(r, 5))
      const instances = await coordinator.getInstances(1)
      assertEquals(instances.length, 0)
    })
  })
})

// #endregion

// #region NATS Distribution Tests

describe("NATS Distribution Protocol", () => {
  const sampleEvents: FlowcoreEvent[] = [
    {
      eventId: "e1",
      eventType: "test",
      payload: { foo: "bar" },
      metadata: {},
      timeBucket: "20250101000000",
      tenant: "test-tenant",
      dataCoreId: "test-dc",
      flowType: "test-flow",
      validTime: "2025-01-01T00:00:00Z",
    },
    {
      eventId: "e2",
      eventType: "test",
      payload: { baz: 42 },
      metadata: {},
      timeBucket: "20250101000000",
      tenant: "test-tenant",
      dataCoreId: "test-dc",
      flowType: "test-flow",
      validTime: "2025-01-01T00:00:01Z",
    },
  ]

  it("should serialize and deserialize request format", () => {
    const request: NatsDistributionRequest = {
      deliveryId: "d1",
      events: sampleEvents,
    }
    const json = JSON.stringify(request)
    const parsed = JSON.parse(json) as NatsDistributionRequest
    assertEquals(parsed.deliveryId, "d1")
    assertEquals(parsed.events.length, 2)
    assertEquals(parsed.events[0].eventId, "e1")
    assertEquals(parsed.events[1].payload, { baz: 42 })
  })

  it("should serialize and deserialize ack reply", () => {
    const reply: NatsDistributionReply = { status: "ack" }
    const json = JSON.stringify(reply)
    const parsed = JSON.parse(json) as NatsDistributionReply
    assertEquals(parsed.status, "ack")
    assertEquals(parsed.error, undefined)
  })

  it("should serialize and deserialize fail reply", () => {
    const reply: NatsDistributionReply = { status: "fail", error: "processing error" }
    const json = JSON.stringify(reply)
    const parsed = JSON.parse(json) as NatsDistributionReply
    assertEquals(parsed.status, "fail")
    assertEquals(parsed.error, "processing error")
  })
})

// #endregion

// #region Cluster Options Validation Tests

describe("Cluster Options Validation", () => {
  const baseOptions = {
    auth: { getBearerToken: () => Promise.resolve("fake") },
    dataSource: {
      tenant: "test",
      dataCore: "dc",
      flowType: "ft",
      eventTypes: ["ev"],
    },
    stateManager: {
      getState: () => null,
    },
  }

  it("should throw when WS mode and no advertisedAddress", () => {
    assertThrows(
      () =>
        new FlowcoreDataPumpCluster({
          ...baseOptions,
          coordinator: new MockCoordinator(),
          notifier: { type: "poller", intervalMs: 1000 },
        }),
      Error,
      "advertisedAddress is required",
    )
  })

  it("should not throw when WS mode and advertisedAddress provided", () => {
    const cluster = new FlowcoreDataPumpCluster({
      ...baseOptions,
      coordinator: new MockCoordinator(),
      advertisedAddress: "ws://localhost:8080",
      notifier: { type: "poller", intervalMs: 1000 },
    })
    assertEquals(typeof cluster.id, "string")
  })

  it("should not throw when NATS mode and no advertisedAddress", () => {
    const cluster = new FlowcoreDataPumpCluster({
      ...baseOptions,
      coordinator: new MockCoordinator(),
      notifier: { type: "nats", servers: ["nats://localhost:4222"] },
    })
    assertEquals(typeof cluster.id, "string")
  })

  it("should not throw when NATS mode with advertisedAddress (ignored)", () => {
    const cluster = new FlowcoreDataPumpCluster({
      ...baseOptions,
      coordinator: new MockCoordinator(),
      advertisedAddress: "ws://localhost:8080",
      notifier: { type: "nats", servers: ["nats://localhost:4222"] },
    })
    assertEquals(typeof cluster.id, "string")
  })
})

// #endregion
