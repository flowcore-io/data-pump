import { expect, it } from "bun:test"

it("enforces strict authentication through the public pump factory with the published SDK", async () => {
  const result = Bun.spawn([process.execPath, "test/fixtures/notifier-auth.ts"], {
    stdout: "pipe",
    stderr: "pipe",
  })
  const [stdout, stderr, code] = await Promise.all([
    new Response(result.stdout).text(),
    new Response(result.stderr).text(),
    result.exited,
  ])
  expect(code, stderr).toBe(0)
  expect(stdout).toContain("Notifier authentication forwarding passed")
})
