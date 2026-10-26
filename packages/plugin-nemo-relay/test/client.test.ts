import { expect, test } from "bun:test"
import { RelayClient } from "../src/client"

test("client diagnostics preserve only bounded app and runtime versions", () => {
  expect(
    RelayClient.diagnostics(
      { version: "2.0.25-preview.7+private-build", channel: "preview" },
      { platform: "darwin", arch: "arm64", versions: { bun: "1.4.2-canary.9", node: "24.15.0" } },
    ),
  ).toEqual({
    client_version: "2.0.25",
    release_channel: "preview",
    os: "darwin",
    arch: "arm64",
    runtime_family: "bun",
    runtime_version: "1.4",
  })
  expect(
    RelayClient.diagnostics(
      { version: "0.0.0", channel: "stable" },
      { platform: "linux", arch: "x64", versions: { node: "24.15.0" } },
    ),
  ).toEqual({
    client_version: "0.0.0",
    release_channel: "stable",
    os: "linux",
    arch: "x64",
    runtime_family: "node",
    runtime_version: "24.15",
  })
})

test("unreported client and runtime details stay absent instead of becoming install claims", () => {
  expect(RelayClient.diagnostics(undefined, { platform: "freebsd", arch: "ia32", versions: {} })).toEqual({
    release_channel: "unknown",
    os: "other",
    arch: "other",
  })
  const result = RelayClient.diagnostics(
    { version: "unknown", channel: "unknown" },
    { platform: "win32", arch: "x64", versions: { node: "unknown" } },
  )
  expect(result).toEqual({ release_channel: "unknown", os: "win32", arch: "x64", runtime_family: "node" })
  expect(result).not.toHaveProperty("install_method")
  expect(result).not.toHaveProperty("client_version")
  expect(result).not.toHaveProperty("runtime_version")
})

test("channel, OS and architecture values use closed vocabularies", () => {
  for (const channel of ["stable", "preview", "beta", "dev"] as const) {
    expect(
      RelayClient.diagnostics({ version: "2.0.25", channel }, { platform: "linux", arch: "arm64", versions: {} })
        .release_channel,
    ).toBe(channel)
  }
  expect(
    RelayClient.diagnostics(
      { version: "unknown", channel: "PRIVATE_BRANCH" },
      { platform: "PRIVATE_OS", arch: "PRIVATE_ARCH", versions: {} },
    ),
  ).toEqual({ release_channel: "unknown", os: "other", arch: "other" })
})

test("arbitrary version strings never become diagnostic values", () => {
  for (const value of [
    "",
    "unknown",
    "PRIVATE",
    "2.0.25PRIVATE",
    "2.0.25/PRIVATE",
    "v2.0.25",
    "2.0",
    "01.2.3",
    "2.00.3",
    "2.0.03",
    " 2.0.25",
    "2.0.25\n",
    "1234567.0.1",
    `2.0.25-${"PRIVATE".repeat(20)}`,
  ]) {
    const result = RelayClient.diagnostics(
      { version: value, channel: "dev" },
      { platform: "linux", arch: "x64", versions: { node: value } },
    )
    expect(result).not.toHaveProperty("client_version")
    expect(result).not.toHaveProperty("runtime_version")
  }
})

test("Bun reporting wins over its Node compatibility version without fabricating a Bun version", () => {
  expect(
    RelayClient.diagnostics(
      { version: "2.0.25", channel: "beta" },
      { platform: "darwin", arch: "arm64", versions: { bun: "PRIVATE", node: "24.15.0" } },
    ),
  ).toEqual({ client_version: "2.0.25", release_channel: "beta", os: "darwin", arch: "arm64", runtime_family: "bun" })
})

test("app names, paths, identity, environment and process arguments are not copied", () => {
  const app = { name: "PRIVATE_APP", version: "2.0.25-PRIVATE+PRIVATE", channel: "PRIVATE_CHANNEL" }
  const runtime = {
    platform: "darwin",
    arch: "arm64",
    versions: { bun: "1.4.2+PRIVATE", node: "24.15.0" },
    hostname: "PRIVATE_HOST",
    installationID: "PRIVATE_ID",
    install_method: "PRIVATE_METHOD",
    executable: "/PRIVATE_PATH/bun",
    env: { TOKEN: "PRIVATE_TOKEN" },
    argv: ["PRIVATE_ARGUMENT"],
  }
  const result = RelayClient.diagnostics(app, runtime)
  expect(result).toEqual({
    client_version: "2.0.25",
    release_channel: "unknown",
    os: "darwin",
    arch: "arm64",
    runtime_family: "bun",
    runtime_version: "1.4",
  })
  expect(JSON.stringify(result)).not.toContain("PRIVATE")
  expect(runtime.argv).toEqual(["PRIVATE_ARGUMENT"])
})
