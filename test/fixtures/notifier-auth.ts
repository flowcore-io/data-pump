import assert from "node:assert/strict"

const sockets: Socket[] = []
class Socket {
  protocol = ""
  onopen?: () => void
  onerror?: (event: Event) => void
  onclose?: (event: { code: number; reason: string }) => void
  sent: string[] = []
  constructor(
    readonly url: string,
    readonly protocols: string[],
  ) {
    sockets.push(this)
  }
  send(value: string) {
    this.sent.push(value)
  }
  close() {}
}
globalThis.WebSocket = Socket as unknown as typeof WebSocket
globalThis.fetch = (async () =>
  Response.json([
    {
      id: "core",
      dataCoreId: "core",
      tenantId: "tenant",
      tenant: "synthetic",
      name: "synthetic",
      description: "",
      accessControl: "private",
      deleteProtection: false,
      isDeleting: false,
      isFlowcoreManaged: false,
    },
  ])) as unknown as typeof fetch
const { FlowcoreDataPump } = await import("../../src/data-pump/data-pump.ts")
import type { FlowcoreNotifier } from "../../src/data-pump/notifier.ts"

for (const bearer of [false, true]) {
  for (const allowQueryFallback of [false, true, undefined]) {
    for (const outcome of ["error", "missing-protocol", "success"]) {
      if (allowQueryFallback !== false && outcome !== "error") continue
      sockets.length = 0
      const pump = FlowcoreDataPump.create({
        auth: bearer
          ? { getBearerToken: async () => "synthetic-token" }
          : { apiKey: "synthetic-key", apiKeyId: "synthetic-id" },
        dataSource: { tenant: "synthetic", dataCore: "synthetic", flowType: "test.0", eventTypes: ["test.created.0"] },
        stateManager: { getState: async () => null, setState: async () => {} },
        notifier: { type: "websocket", allowQueryFallback },
        noTranslation: true,
      })
      // Exercise the notifier created by the public factory without starting ingestion loops.
      const notifier = (pump as unknown as { notifier: FlowcoreNotifier }).notifier
      const abort = new AbortController()
      const waiting = notifier.wait(abort.signal)
      try {
        for (let i = 0; i < 100 && sockets.length === 0; i++) await Bun.sleep(1)
        assert.equal(sockets.length, 1)
        const socket = sockets[0]
        assert.equal(new URL(socket.url).search, "")
        assert.equal(socket.protocols[0], bearer ? "flowcore-bearer" : "flowcore-api-key")
        if (outcome === "error") {
          socket.onerror?.(new Event("error"))
          socket.onclose?.({ code: 1006, reason: "synthetic" })
        } else {
          socket.protocol = outcome === "success" ? socket.protocols[0] : ""
          socket.onopen?.()
        }
        await Bun.sleep(10)
        assert.equal(sockets.length, allowQueryFallback === false ? 1 : 2)
        assert.equal(socket.sent.length, outcome === "success" ? 1 : 0)
        if (allowQueryFallback !== false) assert.notEqual(new URL(sockets[1].url).search, "")
      } finally {
        abort.abort()
        await waiting
      }
    }
  }
}
console.log("Notifier authentication forwarding passed")
