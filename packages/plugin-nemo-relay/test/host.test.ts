import { afterAll, beforeAll, expect, test } from "bun:test"
import { Session } from "@opencode/schema/session"
import { Cause, Effect, Exit, Scope } from "effect"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { RelayBinding } from "../src/binding"
import { RelayHost } from "../src/host"

interface NativeEvent {
  uuid: string
  parent_uuid: string
  name: string
  scope_category?: string
  metadata?: Record<string, unknown>
}

const noParent = Effect.succeed(undefined)
const id = (name: string) => Session.ID.make(`ses_host_${name}`)
let directory: string
let pluginsToml: string

beforeAll(async () => {
  directory = await mkdtemp(path.join(os.tmpdir(), "opencode-relay-host-"))
  pluginsToml = path.join(directory, "plugins.toml")
  await writeFile(pluginsToml, "version = 1\n")
})

afterAll(async () => {
  if (directory) await rm(directory, { recursive: true, force: true })
})

const fixture = async (budget = 500) => {
  const [relay, plugin] = await RelayBinding.load()
  const activation = await plugin.initialize(plugin.defaultConfig(), pluginsToml)
  const events: NativeEvent[] = []
  const state = { flushes: 0, closes: 0 }
  relay.registerSubscriber("host-lifecycle-test", (event: unknown) => {
    events.push(event as NativeEvent)
  })
  const runtime = RelayHost.make(
    {
      ...relay,
      flushSubscribers: () => {
        state.flushes++
        return relay.flushSubscribers()
      },
    },
    {
      ...activation,
      close: () => {
        state.closes++
        return activation.close()
      },
    },
    budget,
  )
  return {
    relay,
    runtime,
    events,
    state,
    dispose: async () => {
      await Effect.runPromise(runtime.shutdown)
      await activation.close()
      relay.deregisterSubscriber("host-lifecycle-test")
    },
  }
}

test("overlapping sessions and operation stacks retain isolated propagation across awaits", async () => {
  const native = await fixture()
  try {
    const a = await Effect.runPromise(native.runtime.lease(id("a"), noParent))
    const b = await Effect.runPromise(native.runtime.lease(id("b"), noParent))
    const gates = [Promise.withResolvers<void>(), Promise.withResolvers<void>()]
    const run = (operation: RelayHost.Operation, name: string, gate: Promise<void>) =>
      operation.run(async () => {
        const scope = native.relay.pushScope(name, 9, operation.parent)
        const before = native.relay.capturePropagationContext()
        await gate
        const after = native.relay.capturePropagationContext()
        native.relay.popScope(scope)
        return { scope, before, after }
      })
    const pendingA = run(a, "a.operation", gates[0].promise)
    const pendingB = run(b, "b.operation", gates[1].promise)
    gates[1].resolve()
    const contextB = await pendingB
    gates[0].resolve()
    const contextA = await pendingA
    a.release()
    b.release()
    native.runtime.close(id("a"), "success")
    native.runtime.close(id("b"), "success")
    await native.relay.flushSubscribers()

    for (const [context, operation] of [
      [contextA, a],
      [contextB, b],
    ] as const) {
      expect(context.after).toEqual(context.before)
      expect(context.before.parentUuid).toBe(context.scope.uuid)
      expect(context.scope.parentUuid).toBe(operation.parent.uuid)
      expect(native.relay.propagationContextToTraceparent(context.after)).toBe(context.before.traceparent!)
    }
    expect(contextA.before.traceparent?.split("-")[1]).not.toBe(contextB.before.traceparent?.split("-")[1])
    expect(native.events.filter((event) => event.scope_category === "end").map((event) => event.uuid)).toEqual([
      contextB.scope.uuid,
      contextA.scope.uuid,
      a.parent.uuid,
      b.parent.uuid,
    ])
  } finally {
    await native.dispose()
  }
})

test("host marks emit parented metric instruments once per admitted event", async () => {
  const native = await fixture()
  try {
    const session = id("metrics")
    const parent = native.runtime.open(session)
    for (const _ of [0, 1])
      native.runtime.mark(
        session,
        "opencode.agent.run.completed",
        { count: 1, outcome: "success", duration_ms: 25 },
        { "opencode.event_id": "metric-event" },
        123_000,
      )
    await native.relay.flushSubscribers()
    expect(native.events.filter((event) => event.name === "opencode.agent.run.completed")).toHaveLength(1)
    expect(native.events.filter((event) => event.name === "opencode.agent.run.completed.metrics")).toEqual([
      expect.objectContaining({ parent_uuid: parent.uuid, timestamp: "1970-01-01T00:00:00.123+00:00" }),
    ])
    native.runtime.close(session, "success")
  } finally {
    await native.dispose()
  }
})

test("parent close waits for child close and its final lease; release is idempotent", async () => {
  const native = await fixture()
  try {
    const parent = native.runtime.open(id("parent"))
    const child = native.runtime.open(id("child"), id("parent"))
    const operation = await Effect.runPromise(native.runtime.lease(id("child"), Effect.succeed(id("parent"))))
    const parentOperation = await Effect.runPromise(native.runtime.lease(id("parent"), noParent))
    const parentTrace = parentOperation.run(() => native.relay.captureTraceparent())
    const childTrace = operation.run(() => native.relay.captureTraceparent())
    parentOperation.release()
    native.runtime.close(id("parent"), "success")
    native.runtime.close(id("child"), "failed")
    await native.relay.flushSubscribers()
    expect(native.events.filter((event) => event.scope_category === "end")).toEqual([])
    operation.release()
    operation.release()
    await native.relay.flushSubscribers()

    expect(child.parentUuid).toBe(parent.uuid)
    expect(childTrace.split("-")[1]).toBe(parentTrace.split("-")[1])
    expect(childTrace.split("-")[2]).not.toBe(parentTrace.split("-")[2])
    expect(
      native.events
        .filter((event) => event.scope_category === "end")
        .map((event) => [event.uuid, event.metadata?.["otel.status_code"]]),
    ).toEqual([
      [child.uuid, "ERROR"],
      [parent.uuid, "OK"],
    ])
  } finally {
    await native.dispose()
  }
})

test("shutdown cancels and drains live work once, closes scopes, then rejects new leases", async () => {
  const native = await fixture()
  try {
    const operation = await Effect.runPromise(native.runtime.lease(id("shutdown"), noParent))
    const state = { cancelled: 0 }
    operation.onCancel(() => {
      state.cancelled++
      queueMicrotask(operation.release)
    })
    await Effect.runPromise(native.runtime.shutdown)
    await Effect.runPromise(native.runtime.shutdown)
    expect(state.cancelled).toBe(1)
    expect(native.state).toEqual({ flushes: 1, closes: 1 })
    expect(
      native.events
        .filter((event) => event.scope_category === "end")
        .map((event) => event.metadata?.["otel.status_code"]),
    ).toEqual(["UNSET"])
    const exit = await Effect.runPromiseExit(native.runtime.lease(id("late"), noParent))
    expect(Exit.isFailure(exit) && String(Cause.squash(exit.cause))).toContain("runtime is stopping")
  } finally {
    await native.dispose()
  }
})

test("concurrent shutdown callers both await the accepted work", async () => {
  const native = await fixture()
  try {
    const operation = await Effect.runPromise(native.runtime.lease(id("drain"), noParent))
    const state = { cancellations: 0, completions: 0 }
    operation.onCancel(() => {
      state.cancellations++
    })
    const shutdowns = [
      Effect.runPromise(native.runtime.shutdown),
      Effect.runPromise(native.runtime.shutdown).then(() => {
        state.completions++
      }),
    ]
    await new Promise<void>((resolve) => setImmediate(resolve))
    const completedEarly = state.completions
    operation.release()
    await Promise.all(shutdowns)
    expect(completedEarly).toBe(0)
    expect(state.cancellations).toBe(1)
    expect(native.state).toEqual({ flushes: 1, closes: 1 })
  } finally {
    await native.dispose()
  }
})

test("retired hosts reject stale observations without reopening scopes or publishing marks", async () => {
  const native = await fixture()
  try {
    native.runtime.open(id("observation_shutdown"))
    await Effect.runPromise(native.runtime.shutdown)
    await native.runtime.retired
    const before = native.events.length
    expect(native.runtime.admit("stale-event")).toBe(false)
    expect(() => native.runtime.open(id("stale_observation"))).toThrow("runtime is stopping")
    native.runtime.mark(id("stale_observation"), "review.stale.mark", {})
    native.runtime.mark(undefined, "review.stale.global", {})
    await native.relay.flushSubscribers()
    expect(native.events).toHaveLength(before)
  } finally {
    await native.dispose()
  }
})

test("timed-out shutdown retains the native host until late work drains, then retires once", async () => {
  const native = await fixture(10)
  try {
    const operation = await Effect.runPromise(native.runtime.lease(id("late_drain"), noParent))
    const state = { cancellations: 0 }
    operation.onCancel(() => {
      state.cancellations++
    })
    await Promise.all([Effect.runPromise(native.runtime.shutdown), Effect.runPromise(native.runtime.shutdown)])
    expect(native.state).toEqual({ flushes: 0, closes: 0 })
    expect(native.events.filter((event) => event.scope_category === "end")).toEqual([])
    operation.release()
    operation.release()
    await native.runtime.retired
    await Effect.runPromise(native.runtime.shutdown)
    expect(state.cancellations).toBe(1)
    expect(native.state).toEqual({ flushes: 1, closes: 1 })
    expect(
      native.events
        .filter((event) => event.scope_category === "end")
        .map((event) => [event.uuid, event.metadata?.["otel.status_code"]]),
    ).toEqual([[operation.parent.uuid, "UNSET"]])
  } finally {
    await native.dispose()
  }
})

test("parent lookup completing after shutdown cannot admit a new operation", async () => {
  const native = await fixture()
  try {
    const parent = Promise.withResolvers<Session.ID | undefined>()
    const pending = Effect.runPromiseExit(
      native.runtime.lease(
        id("late_parent"),
        Effect.promise(() => parent.promise),
      ),
    )
    await Effect.runPromise(native.runtime.shutdown)
    parent.resolve(undefined)
    const exit = await pending
    expect(Exit.isFailure(exit) && String(Cause.squash(exit.cause))).toContain("runtime is stopping")
    expect(native.events).toEqual([])
  } finally {
    await native.dispose()
  }
})

test("Location owners share process startup and only the last release closes sessions", async () => {
  const [relay, plugin] = await RelayBinding.load()
  const scopes = [Effect.runSync(Scope.make()), Effect.runSync(Scope.make())]
  const events: NativeEvent[] = []
  relay.registerSubscriber("host-owners-test", (event: unknown) => {
    events.push(event as NativeEvent)
  })
  try {
    const runtimes = await Promise.all(
      scopes.map((scope) =>
        Effect.runPromise(
          RelayHost.acquire({ config: plugin.defaultConfig(), pluginsToml }).pipe(Scope.provide(scope)),
        ),
      ),
    )
    if (
      !runtimes[0] ||
      !runtimes[1] ||
      runtimes[0] instanceof RelayHost.StartupFailure ||
      runtimes[1] instanceof RelayHost.StartupFailure
    )
      throw new Error("Expected native Relay runtimes")
    expect(runtimes[0]).toBe(runtimes[1])
    const session = runtimes[0].open(id("owners"))
    await Effect.runPromise(Scope.close(scopes[0], Exit.void))
    await relay.flushSubscribers()
    expect(events.filter((event) => event.scope_category === "end")).toEqual([])
    const operation = await Effect.runPromise(runtimes[1].lease(id("owners"), noParent))
    expect(operation.parent.uuid).toBe(session.uuid)
    operation.release()
    await Effect.runPromise(Scope.close(scopes[1], Exit.void))
    expect(events.filter((event) => event.scope_category === "end").map((event) => event.uuid)).toEqual([session.uuid])
  } finally {
    await Promise.all(scopes.map((scope) => Effect.runPromise(Scope.close(scope, Exit.void))))
    relay.deregisterSubscriber("host-owners-test")
  }
})

test("a new Location waits for a timed-out previous native host to retire", async () => {
  const [, plugin] = await RelayBinding.load()
  const scopes = [Effect.runSync(Scope.make()), Effect.runSync(Scope.make())]
  try {
    const first = await Effect.runPromise(
      RelayHost.acquire({ config: plugin.defaultConfig(), pluginsToml, shutdownBudgetMs: 10 }).pipe(
        Scope.provide(scopes[0]),
      ),
    )
    if (!first || first instanceof RelayHost.StartupFailure) throw new Error("Expected an active native host")
    const operation = await Effect.runPromise(first.lease(id("restart"), noParent))
    await Effect.runPromise(Scope.close(scopes[0], Exit.void))
    const state = { acquired: false }
    const pending = Effect.runPromise(
      RelayHost.acquire({ config: plugin.defaultConfig(), pluginsToml }).pipe(Scope.provide(scopes[1])),
    ).then((runtime) => {
      state.acquired = true
      return runtime
    })
    await new Promise<void>((resolve) => setImmediate(resolve))
    const acquiredEarly = state.acquired
    operation.release()
    const second = await pending
    expect(acquiredEarly).toBe(false)
    expect(second).toBeDefined()
    expect(second).not.toBe(first)
  } finally {
    await Promise.all(scopes.map((scope) => Effect.runPromise(Scope.close(scope, Exit.void))))
  }
})
