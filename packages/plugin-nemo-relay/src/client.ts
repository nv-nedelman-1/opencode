export * as RelayClient from "./client.js"

import type { App } from "@opencode/plugin/app"

/** Process activation diagnostics only; neither installation identity nor global OTLP resources. */
export function diagnostics(
  app: Pick<App, "version" | "channel"> | undefined,
  runtime: {
    readonly platform: string
    readonly arch: string
    readonly versions: { readonly bun?: string; readonly node?: string }
  },
) {
  const clientVersion = version(app?.version, 3)
  const family = runtime.versions.bun !== undefined ? "bun" : runtime.versions.node !== undefined ? "node" : undefined
  const runtimeVersion = family === undefined ? undefined : version(runtime.versions[family], 2)
  const channel = app?.channel
  return {
    ...(clientVersion === undefined ? {} : { client_version: clientVersion }),
    release_channel:
      channel === "stable" || channel === "preview" || channel === "beta" || channel === "dev" ? channel : "unknown",
    os:
      runtime.platform === "linux" || runtime.platform === "darwin" || runtime.platform === "win32"
        ? runtime.platform
        : "other",
    arch: runtime.arch === "arm64" || runtime.arch === "x64" ? runtime.arch : "other",
    ...(family === undefined ? {} : { runtime_family: family }),
    ...(runtimeVersion === undefined ? {} : { runtime_version: runtimeVersion }),
  }
}

function version(value: string | undefined, components: 2 | 3) {
  if (value === undefined || value.length > 64 || value !== value.trim()) return undefined
  const match =
    /^(0|[1-9]\d{0,5})\.(0|[1-9]\d{0,5})\.(0|[1-9]\d{0,5})(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/.exec(
      value,
    )
  return match?.slice(1, components + 1).join(".")
}
