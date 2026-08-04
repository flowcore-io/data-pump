import { describe, expect, it } from "bun:test"
import { FlowcoreDataSource } from "../../src/data-pump/data-source.ts"

function createSource(timeBuckets: string[]): FlowcoreDataSource {
  const source = new FlowcoreDataSource({
    auth: { apiKey: "fc_testid_testsecret" },
    dataSource: {
      tenant: "test-tenant",
      dataCore: "test-data-core",
      flowType: "test-flow-type",
      eventTypes: ["alpha.0"],
    },
    noTranslation: true,
  })
  ;(source as unknown as { flowcoreClient: { execute: () => Promise<unknown> } }).flowcoreClient = {
    execute: () => Promise.resolve({ timeBuckets, nextCursor: undefined }),
  }
  return source
}

function createPagedSource(pages: Array<{ timeBuckets: string[]; nextCursor?: number }>): {
  source: FlowcoreDataSource
  commandInputs: Array<{ order?: string; cursor?: number }>
} {
  const source = createSource([])
  const commandInputs: Array<{ order?: string; cursor?: number }> = []
  let pageIndex = 0
  ;(source as unknown as { flowcoreClient: { execute: (command: unknown) => Promise<unknown> } }).flowcoreClient = {
    execute: (command) => {
      commandInputs.push((command as { input: { order?: string; cursor?: number } }).input)
      return Promise.resolve(pages[pageIndex++])
    },
  }
  return { source, commandInputs }
}

function currentClosestOracle(timeBuckets: string[], target: string, getBefore: boolean): string | null {
  if (!timeBuckets.length) return null
  const targetNumber = Number.parseFloat(target)
  if (getBefore) {
    return timeBuckets.findLast((bucket) => Number.parseFloat(bucket) <= targetNumber) ?? timeBuckets.at(-1)!
  }
  return timeBuckets.find((bucket) => Number.parseFloat(bucket) >= targetNumber) ?? timeBuckets.at(-1)!
}

function currentNextOracle(timeBuckets: string[], target: string): string | null {
  const uniqueTimeBuckets = [...new Set(timeBuckets)]
  const closest = currentClosestOracle(uniqueTimeBuckets, target, false)
  if (!closest) return null
  return uniqueTimeBuckets[uniqueTimeBuckets.indexOf(closest) + 1] ?? null
}

describe("time bucket indexed traversal", () => {
  it("matches closest semantics and advances strictly past duplicate buckets", async () => {
    const sequences = [
      [],
      ["20260101000000"],
      ["20260101000000", "20260101010000", "20260101020000"],
      ["20260101000000", "20260101010000", "20260101010000", "20260101030000"],
    ]
    const targets = ["20251231230000", "20260101000000", "20260101003000", "20260101010000", "20260101040000"]

    for (const timeBuckets of sequences) {
      const source = createSource(timeBuckets)
      for (const target of targets) {
        expect(await source.getClosestTimeBucket(target)).toBe(currentClosestOracle(timeBuckets, target, false))
        expect(await source.getClosestTimeBucket(target, true)).toBe(currentClosestOracle(timeBuckets, target, true))
        expect(await source.getNextTimeBucket(target)).toBe(currentNextOracle(timeBuckets, target))
      }
    }
  })

  it("does not use repeated linear array helpers after building the bucket indexes", async () => {
    const source = createSource(["20260101000000", "20260101010000", "20260101010000", "20260101030000"])
    const buckets = await source.getTimeBuckets()
    Object.defineProperties(buckets, {
      find: {
        value: () => {
          throw new Error("linear find used")
        },
      },
      findLast: {
        value: () => {
          throw new Error("linear findLast used")
        },
      },
      indexOf: {
        value: () => {
          throw new Error("linear indexOf used")
        },
      },
    })

    expect(await source.getClosestTimeBucket("20260101003000")).toBe("20260101010000")
    expect(await source.getClosestTimeBucket("20260101020000", true)).toBe("20260101010000")
    expect(await source.getNextTimeBucket("20260101010000")).toBe("20260101030000")
  })

  it("rebuilds lookup state when buckets are force-refreshed", async () => {
    const source = createSource(["20260101000000", "20260101010000"])
    expect(await source.getNextTimeBucket("20260101000000")).toBe("20260101010000")
    ;(source as unknown as { flowcoreClient: { execute: () => Promise<unknown> } }).flowcoreClient = {
      execute: () => Promise.resolve({ timeBuckets: ["20260101000000", "20260101030000"], nextCursor: undefined }),
    }
    await source.getTimeBuckets(true)

    expect(await source.getNextTimeBucket("20260101000000")).toBe("20260101030000")
  })

  it("requests ascending pages and normalizes descending or out-of-order buckets", async () => {
    const { source, commandInputs } = createPagedSource([
      {
        timeBuckets: ["20260101030000", "20260101010000", "20260101020000"],
      },
    ])

    expect(await source.getTimeBuckets()).toEqual(["20260101010000", "20260101020000", "20260101030000"])
    expect(commandInputs.map(({ order }) => order)).toEqual(["asc"])
    expect(await source.getClosestTimeBucket("20260101013000")).toBe("20260101020000")
    expect(await source.getNextTimeBucket("20260101010000")).toBe("20260101020000")
  })

  it("deduplicates the complete cross-page catalog and makes forward progress", async () => {
    const { source, commandInputs } = createPagedSource([
      {
        timeBuckets: ["20260101030000", "20260101010000"],
        nextCursor: 2,
      },
      {
        timeBuckets: ["20260101020000", "20260101010000", "20260101040000"],
      },
    ])

    expect(await source.getTimeBuckets()).toEqual([
      "20260101010000",
      "20260101020000",
      "20260101030000",
      "20260101040000",
    ])
    expect(commandInputs.map(({ order, cursor }) => ({ order, cursor }))).toEqual([
      { order: "asc", cursor: undefined },
      { order: "asc", cursor: 2 },
    ])
    expect(await source.getNextTimeBucket("20260101010000")).toBe("20260101020000")
    expect(await source.getClosestTimeBucket("20260101025000", true)).toBe("20260101020000")
  })
})
