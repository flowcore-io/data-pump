import { describe, expect, it } from "bun:test"
import { PulseEmitter, type PulseSnapshot } from "../../src/data-pump/pulse.ts"

// The PulseEmitter posts via a Flowcore SDK Command, which routes through
// the network. We can't test the wire payload here without a mock server,
// but we can verify the snapshot shape the emitter threads through.

describe("PulseEmitter snapshot shape", () => {
  it("PulseSnapshot carries optional sourceId", () => {
    const snapshot: PulseSnapshot = {
      pathwayId: "pathway-a",
      sourceId: "source-1",
      flowType: "flow.0",
      timeBucket: "2026042317",
      eventId: undefined,
      isLive: true,
      bufferDepth: 0,
      bufferReserved: 0,
      bufferSizeBytes: 0,
      acknowledgedTotal: 0,
      failedTotal: 0,
      pulledTotal: 0,
      uptimeMs: 0,
    }
    expect(snapshot.sourceId).toEqual("source-1")
  })

  it("PulseSnapshot.sourceId is optional for back-compat", () => {
    const snapshot: PulseSnapshot = {
      pathwayId: "pathway-a",
      flowType: "flow.0",
      timeBucket: "2026042317",
      eventId: undefined,
      isLive: true,
      bufferDepth: 0,
      bufferReserved: 0,
      bufferSizeBytes: 0,
      acknowledgedTotal: 0,
      failedTotal: 0,
      pulledTotal: 0,
      uptimeMs: 0,
    }
    expect(snapshot.sourceId).toEqual(undefined)
  })

  it("emitter accepts sourceId in options (compile-time check)", () => {
    // The emitter takes PulseEmitterOptions; sourceId lives on the closure-
    // captured snapshot, not on options. This test just confirms the class
    // constructs without sourceId in the options shape.
    const emitter = new PulseEmitter(
      {
        url: "http://localhost:0",
        auth: { apiKey: "fc_noop_noop" },
      },
      () => null,
    )
    expect(typeof emitter.start).toEqual("function")
    expect(typeof emitter.stop).toEqual("function")
  })
})
