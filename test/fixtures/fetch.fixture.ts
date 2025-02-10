import { type Stub, stub } from "jsr:@std/testing/mock"
import { assertArrayIncludes, assertObjectMatch } from "jsr:@std/assert"

type Method = "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "OPTIONS" | "HEAD" | "TRACE" | "CONNECT"

interface MockedRequest {
  method: Method
  path: string
  headers: Record<string, string> | null
  searchParams: Record<string, string | string[]> | null
  body: string | Record<string, unknown> | Array<unknown> | null
  status: number
  response: string | Record<string, unknown> | Array<unknown> | null
  responseHeaders: Record<string, string>
  persist: boolean
}

interface FetchMockerOptions {
  passThroughUnknown?: boolean
}

export class FetchMocker {
  private originalFetch: typeof globalThis.fetch
  private fetchStub: Stub
  private mocks: Map<string, FetchMockBuilder> = new Map()

  constructor(private options: FetchMockerOptions = {}) {
    this.originalFetch = globalThis.fetch
    this.fetchStub = stub(globalThis, "fetch", this.mockedFetch.bind(this))
  }

  public clear() {
    this.mocks.clear()
  }

  public assert() {
    const notCalled: string[] = []
    for (const mock of this.mocks.values()) {
      notCalled.push(...mock.getNotCalled())
    }
    if (notCalled.length > 0) {
      throw new Error(`Mocked requests not called:\n${notCalled.join("\n")}`)
    }
  }

  public mock(baseUrl: string): FetchMockBuilder {
    if (!this.mocks.has(baseUrl)) {
      this.mocks.set(baseUrl, new FetchMockBuilder(baseUrl))
    }
    return this.mocks.get(baseUrl) as FetchMockBuilder
  }

  public restore() {
    this.fetchStub.restore()
  }

  private mockedFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const url = new URL(input.toString())
    let body = init?.body?.toString() ?? null
    const method = (init?.method ?? "GET") as Method
    const headers = new Headers(init?.headers)
    const searchParams = urlParamsToObject(url.searchParams)
    const headersRecord: Record<string, string> = {}
    headers.forEach((value, key) => {
      headersRecord[key] = value
    })

    if (body && headers.get("content-type") === "application/json") {
      body = JSON.parse(body)
    }
    const mock = this.mocks.get(url.origin)
    if (!mock) {
      return this.originalFetch(input, init)
    }

    const foundMock = mock.findMockedRequest(method, url.pathname, searchParams, body, headersRecord)
    if (!foundMock) {
      throw new Error(`No mock found for ${method} ${url.origin}${url.pathname}?${url.searchParams.toString()} ${body}`)
    }
    return Promise.resolve(
      new Response(
        typeof foundMock.response === "string" ? foundMock.response : JSON.stringify(foundMock.response), // response body
        {
          status: foundMock.status,
          headers: foundMock.responseHeaders,
        },
      ),
    )
  }
}

export class FetchMockBuilder {
  private baseUrl: string
  private mockedRequests: MockedRequest[] = []

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl
  }

  public getNotCalled() {
    const notCalled = []
    for (const mockedRequest of this.mockedRequests) {
      if (!mockedRequest.persist) {
        notCalled.push(`${mockedRequest.method} ${this.baseUrl}${mockedRequest.path}`)
      }
    }
    return notCalled
  }

  public get(path: string) {
    const methodPath = new FetchMockBuilderPath(this, "GET", path)
    return methodPath
  }

  public post(path: string) {
    const methodPath = new FetchMockBuilderPath(this, "POST", path)
    return methodPath
  }

  public put(path: string) {
    const methodPath = new FetchMockBuilderPath(this, "PUT", path)
    return methodPath
  }

  public delete(path: string) {
    const methodPath = new FetchMockBuilderPath(this, "DELETE", path)
    return methodPath
  }

  public patch(path: string) {
    const methodPath = new FetchMockBuilderPath(this, "PATCH", path)
    return methodPath
  }

  public addMock(mockedRequest: MockedRequest) {
    this.mockedRequests.push(mockedRequest)
  }

  public findMockedRequest(
    method: Method,
    path: string,
    searchParams: Record<string, string | string[]>,
    body: string | Record<string, unknown> | Array<unknown> | null,
    headers: Record<string, string>,
  ) {
    const foundIndex = this.mockedRequests.findIndex((mockedRequest) => {
      if (mockedRequest.method !== method) {
        return false
      }

      if (mockedRequest.path !== path) {
        return false
      }

      if (mockedRequest.searchParams) {
        try {
          assertObjectMatch(mockedRequest.searchParams, searchParams)
        } catch (_e) {
          return false
        }
      }

      if (mockedRequest.headers) {
        try {
          assertObjectMatch(mockedRequest.headers, headers)
        } catch (_e) {
          console.log("headers mismatch", { expected: mockedRequest.headers, actual: headers })
          return false
        }
      }

      if (mockedRequest.body) {
        if (body === null) {
          return false
        } else if (typeof mockedRequest.body === "string") {
          return mockedRequest.body === body
        } else if (Array.isArray(mockedRequest.body)) {
          if (!Array.isArray(body)) {
            return false
          }
          try {
            assertArrayIncludes(mockedRequest.body, body)
          } catch (_e) {
            return false
          }
        } else if (typeof mockedRequest.body === "object") {
          if (Array.isArray(body) || typeof body === "string") {
            return false
          }
          try {
            assertObjectMatch(mockedRequest.body, body)
          } catch (_e) {
            return false
          }
        }
      }
      return true
    })
    if (foundIndex === -1) {
      return null
    }
    const foundMock = this.mockedRequests[foundIndex]
    if (!foundMock.persist) {
      this.mockedRequests.splice(foundIndex, 1)
    }
    return foundMock
  }
}

class FetchMockBuilderPath {
  private parent: FetchMockBuilder
  private method: Method
  private path: string
  private headers: Record<string, string> | null = null
  private searchParams: Record<string, string | string[]> | null = null
  private body: string | Record<string, unknown> | Array<unknown> | null = null
  private status = 200
  private response: string | Record<string, unknown> | Array<unknown> | null = null
  private responseHeaders: Record<string, string> = {}
  private persist = false

  constructor(parent: FetchMockBuilder, method: Method, path: string) {
    this.parent = parent
    this.method = method
    this.path = path
  }

  public matchSearchParams(searchParams: Record<string, string | string[]>) {
    this.searchParams = searchParams
    return this
  }

  public matchBody(body: string | Record<string, unknown> | Array<unknown>) {
    this.body = body
    return this
  }

  public matchHeaders(headers: Record<string, string>) {
    this.headers = Object.fromEntries(
      Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value]),
    )
    return this
  }

  public persisted() {
    this.persist = true
    return this
  }

  public respondWith(status: number, body?: string | Record<string, unknown> | Array<unknown>) {
    this.status = status
    this.response = body ?? null
    if (typeof body === "object" && body !== null) {
      this.responseHeaders["Content-Type"] = "application/json"
    } else if (typeof body === "string") {
      this.responseHeaders["Content-Type"] = "text/plain"
    }

    const mockedRequest: MockedRequest = {
      method: this.method,
      path: this.path,
      headers: this.headers,
      searchParams: this.searchParams,
      body: this.body,
      status: this.status,
      response: this.response,
      responseHeaders: this.responseHeaders,
      persist: this.persist,
    }

    this.parent.addMock(mockedRequest)
  }
}

const urlParamsToObject = (urlParams: URLSearchParams) => {
  const obj: Record<string, string | string[]> = {}
  for (const [key, value] of urlParams) {
    if (typeof obj[key] === "string") {
      obj[key] = [obj[key]]
    }
    if (Array.isArray(obj[key])) {
      obj[key].push(value)
    } else {
      obj[key] = value
    }
  }
  return obj
}

function deepCompare(a: unknown, b: unknown): boolean {
  // Handle null/undefined
  if (a === b) {
    return true
  }
  if (a === null || b === null || a === undefined || b === undefined) {
    return false
  }

  // Handle arrays (order doesn't matter)
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) {
      return false
    }
    return a.every((item) => b.some((bItem) => deepCompare(item, bItem)))
  }

  // Handle objects
  if (typeof a === "object" && typeof b === "object") {
    const keysA = Object.keys(a as object)
    const keysB = Object.keys(b as object)

    if (keysA.length !== keysB.length) {
      return false
    }

    return keysA.every((key) => {
      return (
        keysB.includes(key) && deepCompare((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key])
      )
    })
  }

  return a === b
}
