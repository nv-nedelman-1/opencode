import { afterAll, beforeAll, expect, test } from "bun:test"
import type { SessionHttpCall } from "@opencode/plugin/effect/session"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Session } from "@opencode/schema/session"
import { Cause, Effect, Exit, Scope } from "effect"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { RelayBinding } from "../src/binding"
import { RelayHost } from "../src/host"
import { RelayModel } from "../src/model"
import { RelayClient } from "../src/client"

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

const fixture = async (budget = 500, client?: ReturnType<typeof RelayClient.diagnostics>) => {
  const [relay, plugin] = await RelayBinding.load()
  const activation = await plugin.initialize(plugin.defaultConfig(), pluginsToml)
  const events: NativeEvent[] = []
  const state = { flushes: 0, closes: 0 }
  const measurements: unknown[] = []
  relay.registerSubscriber("host-lifecycle-test", (event: unknown) => {
    events.push(event as NativeEvent)
  })
  const runtime = RelayHost.make(
    {
      ...relay,
      metric: (...args: Parameters<typeof relay.metric>) => {
        measurements.push(args[1])
        return relay.metric(...args)
      },
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
    client,
  )
  return {
    relay,
    runtime,
    events,
    state,
    measurements,
    dispose: async () => {
      await Effect.runPromise(runtime.shutdown)
      await activation.close()
      relay.deregisterSubscriber("host-lifecycle-test")
    },
  }
}

const modelCall = (sessionID: Session.ID, kind: SessionHttpCall["kind"], stream = false): SessionHttpCall => ({
  sessionID,
  kind,
  agent: Agent.ID.make("build"),
  model: { providerID: Provider.ID.make("test"), id: Model.ID.make("model") },
  protocol: "openai-chat",
  request: new Request("http://127.0.0.1:1/v1/chat/completions", {
    method: "POST",
    body: JSON.stringify({ model: "model", messages: [{ role: "user", content: "hi" }], stream }),
  }),
})

test.each(["sequential", "overlapping"])(
  "%s idle auxiliary model calls are isolated and do not own the next execution",
  async (order) => {
    const native = await fixture()
    const middleware = RelayModel.middleware(native.runtime, () => Effect.die("idle auxiliary parent lookup"))
    const session = id(`idle_${order}`)
    const gates = [Promise.withResolvers<void>(), Promise.withResolvers<void>()]
    const entered = [Promise.withResolvers<void>(), Promise.withResolvers<void>()]
    const contexts: string[] = []
    const run = (index: number) =>
      Effect.runPromise(
        middleware(modelCall(session, order === "sequential" ? "generate" : "title"), () =>
          Effect.promise(async () => {
            contexts[index] = native.relay.captureTraceparent()
            entered[index].resolve()
            await gates[index].promise
            expect(native.relay.captureTraceparent()).toBe(contexts[index])
            return Response.json({ choices: [{ message: { content: "Hi" } }] })
          }),
        ),
      )
    try {
      const first = run(0)
      await entered[0].promise
      const second = order === "overlapping" ? run(1) : undefined
      if (second) await entered[1].promise
      gates[0].resolve()
      await first
      const last = second ?? run(1)
      await entered[1].promise
      gates[1].resolve()
      await last
      await native.relay.flushSubscribers()
      expect(contexts[0].split("-")[1]).not.toBe(contexts[1].split("-")[1])
      expect(native.events.filter((event) => event.name === "opencode.session")).toEqual([])
      expect(
        native.events.filter((event) => event.scope_category === "end" && event.name === "openai.chat_completions"),
      ).toHaveLength(2)

      await Effect.runPromise(
        RelayModel.middleware(native.runtime, () => noParent)(modelCall(session, "primary"), () =>
          Effect.succeed(Response.json({ choices: [{ message: { content: "Primary" } }] })),
        ),
      )
      native.runtime.close(session, "success")
      await native.relay.flushSubscribers()
      const agents = native.events.filter((event) => event.name === "opencode.session")
      expect(agents.map((event) => event.scope_category)).toEqual(["start", "end"])
      expect(agents[1].metadata?.["otel.status_code"]).toBe("OK")
      expect(
        native.events
          .filter((event) => event.scope_category === "start" && event.name === "openai.chat_completions")
          .at(-1)?.parent_uuid,
      ).toBe(agents[0].uuid)
    } finally {
      gates.forEach((gate) => gate.resolve())
      await native.dispose()
    }
  },
)

test("an auxiliary stream cannot borrow a same-session execution without an owner", async () => {
  const native = await fixture()
  try {
    const session = id("aux_execution")
    const parent = native.runtime.open(session)
    const response = await Effect.runPromise(
      RelayModel.middleware(native.runtime, () => noParent)(modelCall(session, "compaction", true), () =>
        Effect.succeed(
          new Response('data: {"choices":[{"index":0,"delta":{"content":"Hi"}}]}\n\ndata: [DONE]\n\n', {
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      ),
    )
    native.runtime.close(session, "success")
    await native.relay.flushSubscribers()
    expect(native.events.filter((event) => event.uuid === parent.uuid && event.scope_category === "end")).toHaveLength(
      1,
    )
    await response.text()
    await native.relay.flushSubscribers()
    expect(
      native.events.filter((event) => event.name === "openai.chat_completions" && event.scope_category === "start"),
    ).toEqual([expect.objectContaining({ parent_uuid: expect.not.stringContaining(parent.uuid) })])
    expect(native.events.filter((event) => event.scope_category === "end").map((event) => event.name)).toEqual([
      "opencode.session",
      "openai.chat_completions",
    ])
  } finally {
    await native.dispose()
  }
})

test("shutdown cancels and drains an entryless auxiliary native call", async () => {
  const native = await fixture()
  const entered = Promise.withResolvers<void>()
  const state = { interrupted: false }
  const pending = Effect.runPromiseExit(
    RelayModel.middleware(native.runtime, () => noParent)(modelCall(id("aux_shutdown"), "generate"), () =>
      Effect.sync(() => entered.resolve()).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            state.interrupted = true
          }),
        ),
      ),
    ),
  )
  try {
    await entered.promise
    await Effect.runPromise(native.runtime.shutdown)
    const exit = await pending
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
    expect(state.interrupted).toBe(true)
    expect(native.events.filter((event) => event.name === "opencode.session")).toEqual([])
    expect(native.events.filter((event) => event.scope_category === "end").map((event) => event.name)).toEqual([
      "openai.chat_completions",
    ])
    expect(native.state).toEqual({ flushes: 1, closes: 1 })
    const late = await Effect.runPromiseExit(native.runtime.lease(id("late_aux"), noParent, false))
    expect(Exit.isFailure(late) && String(Cause.squash(late.cause))).toContain("runtime is stopping")
  } finally {
    await native.dispose()
    await pending
  }
})

test("signed auxiliary coverage does not borrow an active execution", async () => {
  const native = await fixture()
  try {
    const session = id("signed_auxiliary")
    const primary = native.runtime.open(session)
    await Effect.runPromise(
      RelayModel.middleware(native.runtime, () => noParent)(
        { ...modelCall(session, "generate"), protocol: "bedrock-converse" },
        () => Effect.succeed(Response.json({})),
      ),
    )
    await native.relay.flushSubscribers()
    const coverage = native.events.filter((event) => event.name === "opencode.llm.coverage")
    expect(coverage).toHaveLength(1)
    expect(coverage[0]?.parent_uuid).not.toBe(primary.uuid)
    native.runtime.close(session, "success")
  } finally {
    await native.dispose()
  }
})

test.each(["idle", "active"])("an %s auxiliary keeps physical observations off the next execution", async (state) => {
  const native = await fixture()
  const session = id("aux_marks")
  const original = state === "active" ? native.runtime.open(session) : undefined
  const entered = Promise.withResolvers<void>()
  const response = Promise.withResolvers<Response>()
  const pending = Effect.runPromise(
    RelayModel.middleware(native.runtime, () => noParent)(modelCall(session, "generate"), () =>
      Effect.sync(() => entered.resolve()).pipe(Effect.andThen(Effect.promise(() => response.promise))),
    ),
  )
  try {
    await entered.promise
    if (original) native.runtime.close(session, "success")
    const primary = native.runtime.open(session)
    response.resolve(Response.json({ choices: [{ message: { content: "Auxiliary" } }] }))
    await pending
    await native.relay.flushSubscribers()
    const observations = native.events.filter((event) =>
      [
        "opencode.llm.provider_attempt.headers",
        "opencode.llm.first_output",
        "opencode.llm.provider_attempt.completed",
      ].includes(event.name),
    )
    expect(observations.map((event) => event.name)).toEqual([
      "opencode.llm.provider_attempt.headers",
      "opencode.llm.first_output",
      "opencode.llm.provider_attempt.completed",
    ])
    expect(observations.every((event) => event.parent_uuid !== primary.uuid)).toBe(true)
    if (original) expect(observations.every((event) => event.parent_uuid !== original.uuid)).toBe(true)
    native.runtime.close(session, "success")
  } finally {
    response.resolve(Response.json({}))
    await pending
    await native.dispose()
  }
})

test("shutdown drains an unread auxiliary stream without creating an execution", async () => {
  const native = await fixture()
  try {
    const response = await Effect.runPromise(
      RelayModel.middleware(native.runtime, () => noParent)(modelCall(id("aux_unread"), "generate", true), () =>
        Effect.succeed(
          new Response('data: {"choices":[{"index":0,"delta":{"content":"Hi"}}]}\n\ndata: [DONE]\n\n', {
            headers: { "content-type": "text/event-stream" },
          }),
        ),
      ),
    )
    await Effect.runPromise(native.runtime.shutdown)
    await response.body?.cancel()
    expect(native.events.filter((event) => event.name === "opencode.session")).toEqual([])
    expect(native.events.filter((event) => event.scope_category === "end").map((event) => event.name)).toEqual([
      "openai.chat_completions",
    ])
    expect(native.state).toEqual({ flushes: 1, closes: 1 })
  } finally {
    await native.dispose()
  }
})

test("released operations cannot publish against their closed execution or its successor", async () => {
  const native = await fixture()
  try {
    const session = id("released_marks")
    const operation = await Effect.runPromise(native.runtime.lease(session, noParent))
    native.runtime.close(session, "success")
    operation.release()
    const successor = native.runtime.open(session)
    operation.mark("review.late.operation", { count: 1 })
    await native.relay.flushSubscribers()
    expect(native.events.filter((event) => event.name === "review.late.operation")).toEqual([])
    expect(native.events.filter((event) => event.uuid === successor.uuid && event.scope_category === "end")).toEqual([])
    native.runtime.close(session, "success")
  } finally {
    await native.dispose()
  }
})

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
      expect(context.scope.parentUuid).toBe(operation.parent!.uuid)
      expect(native.relay.propagationContextToTraceparent(context.after)).toBe(context.before.traceparent!)
    }
    expect(contextA.before.traceparent?.split("-")[1]).not.toBe(contextB.before.traceparent?.split("-")[1])
    expect(native.events.filter((event) => event.scope_category === "end").map((event) => event.uuid)).toEqual([
      contextB.scope.uuid,
      contextA.scope.uuid,
      a.parent!.uuid,
      b.parent!.uuid,
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
    // A terminal event may finish several waits; suffixes preserve each sample without replaying it.
    for (const requestID of ["a", "b", "a", "b"])
      native.runtime.mark(
        session,
        "opencode.permission.wait.completed",
        { count: 1, family: "terminal", resolution: "cancelled", duration_ms: 25 },
        { "opencode.event_id": `terminal-event:permission:${requestID}` },
      )
    await native.relay.flushSubscribers()
    expect(native.events.filter((event) => event.name === "opencode.permission.wait.completed.metrics")).toEqual([
      expect.objectContaining({ parent_uuid: parent.uuid }),
      expect.objectContaining({ parent_uuid: parent.uuid }),
    ])
    native.runtime.close(session, "success")
  } finally {
    await native.dispose()
  }
})

test("actual native instruments receive bounded process dimensions without changing host event payloads", async () => {
  const native = await fixture(
    500,
    RelayClient.diagnostics(
      { version: "2.0.25-SECRET", channel: "beta" },
      { platform: "linux", arch: "x64", versions: { node: "24.15.0" } },
    ),
  )
  try {
    native.runtime.mark(undefined, "opencode.agent.run.started", { count: 1 })
    await native.relay.flushSubscribers()
    expect(native.measurements).toEqual([
      [
        expect.objectContaining({
          attributes: expect.objectContaining({
            "opencode.client.version": "2.0.25",
            "opencode.client.channel": "beta",
            "opencode.host.os": "linux",
            "opencode.runtime.family": "node",
          }),
        }),
      ],
    ])
    expect(JSON.stringify(native.measurements)).not.toContain("SECRET")
    expect(native.events.filter((event) => event.name === "opencode.agent.run.started.metrics")).toHaveLength(1)
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
    ).toEqual([[operation.parent!.uuid, "UNSET"]])
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
          RelayHost.acquire({
            config: plugin.defaultConfig(),
            pluginsToml,
            client: { version: "2.0.25-private+private", channel: "preview" },
          }).pipe(Scope.provide(scope)),
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
    expect(events.filter((event) => event.name === "opencode.runtime.activation.metrics")).toHaveLength(1)
    expect(events.filter((event) => event.name === "opencode.runtime.activation")).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({ count: 1, client_version: "2.0.25", release_channel: "preview" }),
      }),
    ])
    const operation = await Effect.runPromise(runtimes[1].lease(id("owners"), noParent))
    expect(operation.parent!.uuid).toBe(session.uuid)
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
