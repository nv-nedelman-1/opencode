export * as RelayHost from "./host.js"

import { RelayBinding } from "#binding"
import type { Session } from "@opencode/schema/session"
import { Cause, Effect, Option, Schema, type Scope } from "effect"
import type { PropagationContext, ScopeHandle, ScopeStack } from "nemo-relay-node"
import type { PluginConfig, PluginHostActivation } from "nemo-relay-node/plugin"
import { access } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { RelayMetrics } from "./metrics.js"

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
  readonly open: (sessionID: Session.ID, parentID?: Session.ID, timestamp?: number) => ScopeHandle
  readonly close: (sessionID: Session.ID, outcome: Outcome) => void
  readonly mark: (
    sessionID: Session.ID | undefined,
    name: string,
    data: unknown,
    metadata?: unknown,
    timestamp?: number,
  ) => void
  readonly admit: (eventID: string) => boolean
  readonly lease: (sessionID: Session.ID, parentID: Effect.Effect<Session.ID | undefined>) => Effect.Effect<Operation>
}

export interface Operation {
  readonly parent: ScopeHandle
  readonly run: <A>(callback: () => A) => A
  readonly onCancel: (cancel: () => void) => void
  readonly release: () => void
}

interface Active extends Runtime {
  readonly shutdown: Effect.Effect<void>
  readonly retired: Promise<void>
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
  const stopped = runtime.then((active) => (active ? Effect.runPromise(active.shutdown) : undefined))
  closing = stopped.then(() => runtime).then((active) => active?.retired)
  return stopped
}

const start = Effect.fn("RelayHost.start")(function* (options: Options) {
  const flag = process.env.OPENCODE_NEMO_RELAY?.trim().toLowerCase()
  if (flag !== undefined && OFF.has(flag)) return undefined
  const pluginsToml = options.pluginsToml ?? (process.env.OPENCODE_NEMO_RELAY_PLUGINS_TOML?.trim() || undefined)
  const forced =
    options.config !== undefined ||
    pluginsToml !== undefined ||
    (flag !== undefined && ON.has(flag)) ||
    [
      "OTEL_EXPORTER_OTLP_ENDPOINT",
      "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
      "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
      "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    ].some((name) => Boolean(process.env[name]?.trim()))
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

export const make = (relay: Relay, activation: PluginHostActivation, budget: number): Active => {
  interface Entry {
    readonly handle: ScopeHandle
    readonly stack: ScopeStack
    readonly parent?: Entry
    children: number
    operations: number
    outcome?: Outcome
  }
  const sessions = new Map<Session.ID, Entry>()
  const operations = new Set<{ readonly done: Promise<void>; cancel: () => void }>()
  const marks = new Set<string>()
  const events = new Set<string>()
  const state: { accepting: boolean; shutdown?: Promise<void> } = { accepting: true }
  const neutral = relay.createScopeStack()
  const retirement = Promise.withResolvers<void>()

  const open = (sessionID: Session.ID, parentID?: Session.ID, timestamp?: number) => {
    const existing = sessions.get(sessionID)
    if (existing) return existing
    // Each session owns a stack so sessions can close in any order without violating Relay's LIFO rule.
    const parent = parentID === undefined ? undefined : sessions.get(parentID)
    const stack = parent
      ? relay.createScopeStackFromPropagation(
          relay.withScopeStack(parent.stack, () => relay.capturePropagationContext()) as PropagationContext,
        )
      : relay.createScopeStack()
    const handle = relay.withScopeStack(stack, () =>
      relay.pushScope(
        "opencode.session",
        AGENT_SCOPE,
        parent?.handle ?? null,
        null,
        null,
        {
          "opencode.session_id": sessionID,
          ...(parentID === undefined ? {} : { "opencode.parent_session_id": parentID }),
        },
        null,
        timestamp,
      ),
    ) as ScopeHandle
    const entry = { handle, stack, parent, children: 0, operations: 0 }
    if (parent) parent.children++
    sessions.set(sessionID, entry)
    return entry
  }

  const finish = (entry: Entry) => {
    if (entry.outcome === undefined || entry.children > 0 || entry.operations > 0) return
    const outcome = entry.outcome
    relay.withScopeStack(entry.stack, () =>
      relay.popScope(entry.handle, { outcome }, null, { "otel.status_code": STATUS[outcome] }),
    )
    if (entry.parent) {
      entry.parent.children--
      finish(entry.parent)
    }
  }

  const close = (sessionID: Session.ID, outcome: Outcome) => {
    const entry = sessions.get(sessionID)
    if (!entry) return
    sessions.delete(sessionID)
    entry.outcome = outcome
    finish(entry)
  }

  const resolve = (sessionID: Session.ID, parentID: Effect.Effect<Session.ID | undefined>) =>
    Effect.gen(function* () {
      const existing = sessions.get(sessionID)
      if (existing) return existing
      const parent = yield* parentID
      if (!state.accepting) return yield* Effect.die(new Error("NeMo Relay runtime is stopping"))
      return open(sessionID, parent)
    })

  const lease = (sessionID: Session.ID, parentID: Effect.Effect<Session.ID | undefined>) =>
    Effect.gen(function* () {
      if (!state.accepting) return yield* Effect.die(new Error("NeMo Relay runtime is stopping"))
      const entry = yield* resolve(sessionID, parentID)
      if (!state.accepting) return yield* Effect.die(new Error("NeMo Relay runtime is stopping"))
      const stack = relay.createScopeStackFromPropagation(
        relay.withScopeStack(entry.stack, () => relay.capturePropagationContext()) as PropagationContext,
      )
      const completion = Promise.withResolvers<void>()
      const pending = { done: completion.promise, cancel: () => {} }
      operations.add(pending)
      entry.operations++
      return {
        parent: entry.handle,
        run: <A>(callback: () => A) => relay.withScopeStack(stack, callback) as A,
        onCancel: (cancel: () => void) => {
          pending.cancel = cancel
        },
        release: () => {
          if (!operations.delete(pending)) return
          entry.operations--
          finish(entry)
          completion.resolve()
        },
      }
    })

  const mark: Runtime["mark"] = (sessionID, name, data, metadata, timestamp) => {
    const id = isRecord(metadata) ? metadata["opencode.event_id"] : undefined
    if (typeof id === "string") {
      const key = `${name}:${id}`
      if (marks.has(key)) return
      marks.add(key)
      if (marks.size > 1024) marks.delete(marks.values().next().value!)
    }
    const entry = sessionID === undefined ? undefined : sessions.get(sessionID)
    const measurements = RelayMetrics.measurements(name, data)
    relay.withScopeStack(entry?.stack ?? neutral, () => {
      relay.event(name, entry?.handle ?? null, json(data), json(metadata), timestamp)
      if (measurements.length > 0)
        relay.metric(`${name}.metrics`, measurements, entry?.handle ?? null, json(metadata), timestamp)
    })
  }

  const finalize = Effect.gen(function* () {
    Array.from(sessions.keys()).forEach((sessionID) => close(sessionID, "cancelled"))
    yield* Effect.tryPromise(() => relay.flushSubscribers()).pipe(Effect.ignore)
    yield* Effect.tryPromise(() => activation.close()).pipe(Effect.ignore)
  }).pipe(Effect.ensuring(Effect.sync(retirement.resolve)))

  const shutdown = Effect.gen(function* () {
    state.accepting = false
    const deadline = Date.now() + budget
    operations.forEach((operation) => operation.cancel())
    const drained = Promise.all(Array.from(operations, (operation) => operation.done))
    const cleanup = drained.then(() => Effect.runPromise(finalize))
    const result = yield* Effect.promise(() => cleanup).pipe(Effect.timeoutOption(Math.max(1, deadline - Date.now())))
    if (Option.isNone(result)) {
      yield* Effect.logWarning("NeMo Relay shutdown timed out with live operations; retaining the native host", {
        pending: operations.size,
      })
    }
  })

  return {
    relay,
    open: (sessionID, parentID, timestamp) => open(sessionID, parentID, timestamp).handle,
    scope: (sessionID, parentID) => resolve(sessionID, parentID).pipe(Effect.map((entry) => entry.handle)),
    lease,
    mark,
    admit: (id) => {
      if (events.has(id)) return false
      events.add(id)
      if (events.size > 1024) events.delete(events.values().next().value!)
      return true
    },
    close,
    retired: retirement.promise,
    shutdown: Effect.promise(() => (state.shutdown ??= Effect.runPromise(shutdown))),
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
