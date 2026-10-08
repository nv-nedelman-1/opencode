export * as RelayHost from "./host.js"

import { RelayBinding } from "#binding"
import type { Session } from "@opencode/schema/session"
import { Cause, Effect, Option, Schema, type Scope } from "effect"
import type { ScopeHandle, ScopeStack } from "nemo-relay-node"
import type { PluginConfig, PluginHostActivation } from "nemo-relay-node/plugin"
import { access } from "node:fs/promises"
import os from "node:os"
import path from "node:path"

export type Relay = Awaited<ReturnType<typeof RelayBinding.load>>[0]
export type Outcome = "success" | "failed" | "cancelled"

export interface Options {
  /** Programmatic plugin configuration layered over the discovered files. Activates Relay without a file. */
  readonly config?: PluginConfig
  /** Explicit user-layer `plugins.toml`. Relay's system layer still applies above it. */
  readonly pluginsToml?: string
  readonly shutdownBudgetMs?: number
}

export interface Runtime {
  readonly relay: Relay
  /** The session's execution scope, opened on first use beneath its parent session's scope when that is open. */
  readonly scope: (sessionID: Session.ID, parentID: Effect.Effect<Session.ID | undefined>) => Effect.Effect<ScopeHandle>
  readonly close: (sessionID: Session.ID, outcome: Outcome) => void
}

interface Active extends Runtime {
  readonly shutdown: Effect.Effect<void>
}

const OFF = new Set(["0", "false", "no", "off"])
const ON = new Set(["1", "true", "yes", "on"])
const GUARDRAIL = "guardrail rejected: "
// The standalone CLI kills a server that has not exited within three seconds of SIGTERM.
const SHUTDOWN_BUDGET_MS = 2_000
const STATUS = { success: "OK", failed: "ERROR", cancelled: "UNSET" } as const
// `ScopeType.Agent`. The binding declares an ambient const enum, which isolated modules cannot read.
const AGENT_SCOPE = 0

// Relay allows one plugin-host activation per process, while plugins start once per Location.
let owners = 0
let current: Promise<Active | undefined> | undefined
let closing = Promise.resolve()

/**
 * Shares one process-wide Relay runtime between plugin instances. The runtime starts only when Relay
 * is configured, and the last release drains open scopes, flushes subscribers, and closes the host.
 */
export const acquire = (options: Options = {}): Effect.Effect<Runtime | undefined, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const context = yield* Effect.context<never>()
      owners++
      const runtime = (current ??= closing.then(() => Effect.runPromiseWith(context)(start(options))))
      return yield* Effect.promise(() => runtime)
    }),
    () => Effect.promise(release),
  )

const release = () => {
  owners--
  if (owners > 0 || current === undefined) return closing
  const runtime = current
  current = undefined
  closing = runtime.then((active) => (active ? Effect.runPromise(active.shutdown) : undefined))
  return closing
}

const start = Effect.fn("RelayHost.start")(function* (options: Options) {
  const flag = process.env.OPENCODE_NEMO_RELAY?.trim().toLowerCase()
  if (flag !== undefined && OFF.has(flag)) return undefined
  const pluginsToml = options.pluginsToml ?? (process.env.OPENCODE_NEMO_RELAY_PLUGINS_TOML?.trim() || undefined)
  const forced = options.config !== undefined || (flag !== undefined && ON.has(flag))
  if (!forced && !(yield* configured(pluginsToml))) return undefined
  const modules = yield* Effect.tryPromise(RelayBinding.load).pipe(
    Effect.map(Option.some),
    Effect.catch((error) =>
      Effect.logWarning("NeMo Relay is configured but its native runtime could not be loaded", {
        error: String(error),
      }).pipe(Effect.as(Option.none())),
    ),
  )
  if (Option.isNone(modules)) return undefined
  const [relay, plugin] = modules.value
  const activation = yield* Effect.tryPromise(() =>
    plugin.initialize(options.config ?? plugin.defaultConfig(), pluginsToml),
  ).pipe(
    Effect.map(Option.some),
    Effect.catch((error) =>
      Effect.logWarning("NeMo Relay plugin host failed to start", { error: String(error) }).pipe(
        Effect.as(Option.none()),
      ),
    ),
  )
  if (Option.isNone(activation)) return undefined
  yield* Effect.logInfo("NeMo Relay plugin host is active")
  return make(relay, activation.value, options.shutdownBudgetMs ?? SHUTDOWN_BUDGET_MS)
})

// Core also runs on Node and workerd, so this avoids Bun-only file APIs.
const configured = (explicit: string | undefined) =>
  Effect.promise(async () =>
    (
      await Promise.all(
        locations(explicit).map((file) =>
          access(file).then(
            () => true,
            () => false,
          ),
        ),
      )
    ).some(Boolean),
  )

// Relay's own discovery order: the explicit or user file, then the system file.
const locations = (explicit: string | undefined) => [
  explicit ??
    path.join(process.env.XDG_CONFIG_HOME ?? path.join(os.homedir(), ".config"), "nemo-relay", "plugins.toml"),
  process.platform === "win32"
    ? path.join(process.env.ProgramData ?? "C:\\ProgramData", "nemo-relay", "plugins.toml")
    : "/etc/nemo-relay/plugins.toml",
]

const make = (relay: Relay, activation: PluginHostActivation, budget: number): Active => {
  const sessions = new Map<Session.ID, { readonly handle: ScopeHandle; readonly stack: ScopeStack }>()

  const open = (sessionID: Session.ID, parentID: Session.ID | undefined) => {
    // Each session owns a stack so sessions can close in any order without violating Relay's LIFO rule.
    const stack = relay.createScopeStack()
    const parent = parentID === undefined ? undefined : sessions.get(parentID)
    const handle = relay.withScopeStack(stack, () =>
      relay.pushScope("opencode.session", AGENT_SCOPE, parent?.handle ?? null, null, null, {
        "opencode.session_id": sessionID,
        ...(parentID === undefined ? {} : { "opencode.parent_session_id": parentID }),
      }),
    ) as ScopeHandle
    sessions.set(sessionID, { handle, stack })
    return handle
  }

  const close = (sessionID: Session.ID, outcome: Outcome) => {
    const entry = sessions.get(sessionID)
    if (!entry) return
    sessions.delete(sessionID)
    relay.withScopeStack(entry.stack, () =>
      relay.popScope(entry.handle, { outcome }, null, { "otel.status_code": STATUS[outcome] }),
    )
  }

  return {
    relay,
    scope: (sessionID, parentID) =>
      Effect.gen(function* () {
        const existing = sessions.get(sessionID)
        if (existing) return existing.handle
        const parent = yield* parentID
        return sessions.get(sessionID)?.handle ?? open(sessionID, parent)
      }),
    close,
    shutdown: Effect.suspend(() => {
      Array.from(sessions.keys()).forEach((sessionID) => close(sessionID, "cancelled"))
      return Effect.tryPromise(() =>
        relay
          .flushSubscribers()
          .catch(() => undefined)
          .then(() => activation.close()),
      ).pipe(Effect.timeoutOption(budget), Effect.ignore)
    }),
  }
}

const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown))

/** Parses untrusted JSON text, returning undefined when it is not JSON. */
export const parse = (text: string) => Option.getOrUndefined(decodeJson(text))

/** A JSON-safe copy. Relay's JSON boundary rejects `undefined` properties and other non-JSON values. */
export const json = (value: unknown): unknown => JSON.parse(JSON.stringify(value ?? null))

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

/** Compares JSON values regardless of key order; Relay's JSON boundary sorts object keys. */
export const sameJson = (left: unknown, right: unknown) => canonical(left) === canonical(right)

const canonical = (value: unknown) =>
  JSON.stringify(value, (_key, item: unknown) =>
    isRecord(item)
      ? Object.fromEntries(Object.entries(item).toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : item,
  )

/** The guardrail reason when Relay rejected a managed call before running it. */
export const rejection = (cause: Cause.Cause<unknown>) => {
  const error = Cause.squash(cause)
  const message = error instanceof Error ? error.message : String(error)
  return message.startsWith(GUARDRAIL) ? message.slice(GUARDRAIL.length) : undefined
}
