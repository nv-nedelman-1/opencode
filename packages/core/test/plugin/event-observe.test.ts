import { expect } from "bun:test"
import { Bus } from "@opencode/core/bus"
import { Location } from "@opencode/core/location"
import { Plugin } from "@opencode/core/plugin"
import { Session } from "@opencode/core/session"
import { AbsolutePath } from "@opencode/schema/schema"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
import { Effect, Logger, Schema } from "effect"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)
const Rpc = Bus.ephemeral({ type: "rpc.observer.notice", schema: { value: Schema.String } })
const Internal = Bus.ephemeral({ type: "test.observer.internal", schema: {} })

it.effect("observes committed lifecycle events before publication returns and a subsequent owner starts", () =>
  Effect.gen(function* () {
    const plugins = yield* Plugin.Service
    const bus = yield* Bus.Service
    const sessions = yield* Session.Service
    const location = yield* Location.Service
    const session = yield* sessions.create({
      title: "source ordering",
      location: { directory: location.directory, workspaceID: location.workspaceID },
    })
    const order: string[] = []
    let committed = false
    let owner = 0
    yield* plugins.activate([
      {
        id: "source-observer",
        revision: "1",
        effect: (ctx) =>
          ctx.event
            .observe((event) => {
              if (event.type === "session.execution.started") {
                expect(committed).toBe(true)
                owner++
                order.push(`start:${owner}`)
              }
              if (event.type === "session.execution.succeeded") order.push(`end:${owner}`)
              return undefined
            })
            .pipe(Effect.asVoid),
      },
    ])
    for (const _ of [0, 1]) {
      committed = false
      yield* bus.publish(
        SessionEvent.Execution.Started,
        { sessionID: session.id },
        {
          commit: () =>
            Effect.sync(() => {
              committed = true
            }),
        },
      )
      order.push(`source-call:${owner}`)
      yield* bus.publish(SessionEvent.Execution.Succeeded, { sessionID: session.id })
      order.push(`idle:${owner}`)
    }
    expect(order).toEqual([
      "start:1",
      "source-call:1",
      "end:1",
      "idle:1",
      "start:2",
      "source-call:2",
      "end:2",
      "idle:2",
    ])
  }),
)

it.effect("disposes source observers explicitly and on plugin unload without changing ordinary delivery", () =>
  Effect.gen(function* () {
    const plugins = yield* Plugin.Service
    const bus = yield* Bus.Service
    const location = yield* Location.Service
    const seen: string[] = []
    let dispose = Effect.void
    yield* plugins.activate([
      {
        id: "source-observer",
        revision: "1",
        effect: (ctx) =>
          Effect.gen(function* () {
            const registration = yield* ctx.event.observe((event) => {
              if (event.type === "rpc.observer.notice") seen.push(String(event.data.value))
              return undefined
            })
            dispose = registration.dispose
          }),
      },
    ])
    yield* bus.publish(Rpc, { value: "own" })
    yield* bus.publish(
      Rpc,
      { value: "other" },
      { location: { directory: AbsolutePath.make(`${location.directory}/other`) } },
    )
    yield* bus.publish(Rpc, { value: "global" }, { global: true })
    expect(seen).toEqual(["own", "global"])
    yield* dispose
    yield* dispose
    yield* bus.publish(Rpc, { value: "disposed" })
    expect(seen).toEqual(["own", "global"])

    yield* plugins.activate([
      {
        id: "source-observer",
        revision: "2",
        effect: (ctx) =>
          ctx.event
            .observe((event) => {
              if (event.type === "rpc.observer.notice") seen.push(String(event.data.value))
              return undefined
            })
            .pipe(Effect.asVoid),
      },
    ])
    yield* bus.publish(Rpc, { value: "reloaded" })
    yield* plugins.activate([])
    yield* bus.publish(Rpc, { value: "unloaded" })
    expect(seen).toEqual(["own", "global", "reloaded"])
  }),
)

it.effect("isolates observer defects and unsupported async returns for durable and live-only events", () =>
  Effect.gen(function* () {
    const plugins = yield* Plugin.Service
    const bus = yield* Bus.Service
    const sessions = yield* Session.Service
    const location = yield* Location.Service
    const session = yield* sessions.create({
      title: "observer isolation",
      location: { directory: location.directory, workspaceID: location.workspaceID },
    })
    const seen: string[] = []
    const warnings: unknown[] = []
    yield* plugins.activate([
      {
        id: "source-observers",
        revision: "1",
        effect: (ctx) =>
          Effect.gen(function* () {
            yield* ctx.event.observe(() => {
              throw new Error("PRIVATE observer defect")
            })
            // @ts-expect-error Source observers cannot return promises; untyped misuse is still isolated.
            yield* ctx.event.observe(async () => {
              throw new Error("PRIVATE rejected async work")
            })
            yield* ctx.event.observe((event) => {
              seen.push(event.type)
              return undefined
            })
          }),
      },
    ])
    seen.length = 0
    yield* Effect.gen(function* () {
      yield* bus.publish(SessionEvent.Execution.Started, { sessionID: session.id })
      yield* bus.publish(SessionEvent.Text.Delta, {
        sessionID: session.id,
        assistantMessageID: SessionMessage.ID.make("msg_observer"),
        ordinal: 0,
        delta: "synthetic",
      })
      yield* bus.publish(Rpc, { value: "synthetic" })
      yield* bus.publish(Internal, {})
      yield* Effect.promise(() => Promise.resolve())
    }).pipe(
      Effect.provideService(
        Logger.CurrentLoggers,
        new Set([
          Logger.make((entry) => {
            warnings.push(entry.message)
          }),
        ]),
      ),
    )
    expect(seen).toEqual(["session.execution.started", "session.text.delta", "rpc.observer.notice"])
    expect(warnings).toHaveLength(6)
    expect(JSON.stringify(warnings)).toContain("Plugin event observer failed")
    expect(JSON.stringify(warnings)).toContain("Plugin event observer must be synchronous and return undefined")
    expect(JSON.stringify(warnings)).not.toContain("PRIVATE")
  }),
)
