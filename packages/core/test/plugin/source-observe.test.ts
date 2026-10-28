import { expect } from "bun:test"
import { Plugin } from "@opencode/core/plugin"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { SessionModelOutcome } from "@opencode/core/session/model-outcome"
import { Skill } from "@opencode/core/skill"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { AbsolutePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import type { SkillLoad } from "@opencode/plugin/effect/skill"
import type { MCPRecovery } from "@opencode/plugin/effect/mcp"
import { fromPromise } from "@opencode/plugin/promise/adapter"
import { Effect, Fiber } from "effect"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)
const record =
  <Event>(events: [string, Event][], source: string) =>
  (event: Event) =>
    void events.push([source, event])
const input = {
  sessionID: Session.ID.make("ses_source_observe"),
  agent: Agent.ID.make("build"),
  model: { providerID: Provider.ID.make("fixture"), id: Model.ID.make("fixture") },
  kind: "primary" as const,
}
const skillInput = {
  sessionID: input.sessionID,
  channel: "tool" as const,
  sourceKind: "directory" as const,
  skill: Skill.Info.make({
    id: Skill.ID.make("review"),
    name: Skill.Name.make("Review"),
    path: AbsolutePath.make("/private/review/SKILL.md"),
    content: "Private guidance",
  }),
}

it.effect(
  "Effect and Promise source observers share exact snapshots, filters, explicit disposal and plugin scope",
  () =>
    Effect.gen(function* () {
      const plugins = yield* Plugin.Service
      const hooks = yield* PluginHooks.Service
      const model: [string, PluginHooks.Observations["session"]["model.outcome"]][] = []
      const skills: [string, SkillLoad][] = []
      const mcp: [string, MCPRecovery][] = []
      let dispose = Effect.void
      let filtered = 0
      const promise = fromPromise({
        id: "promise-source-observer",
        setup: async (ctx) => {
          await ctx.session.observe("model.outcome", record(model, "promise"), { providerID: "fixture" })
          await ctx.skill.observe("load", record(skills, "promise"))
          await ctx.mcp.observe("recovery", record(mcp, "promise"))
        },
      })
      yield* plugins.activate([
        {
          id: "effect-source-observer",
          revision: "1",
          effect: (ctx) =>
            Effect.gen(function* () {
              const registration = yield* ctx.session.observe("model.outcome", record(model, "effect"), {
                providerID: "fixture",
              })
              dispose = registration.dispose
              yield* ctx.session.observe("model.outcome", () => void filtered++, { providerID: "other" })
              yield* ctx.skill.observe("load", record(skills, "effect"))
              yield* ctx.mcp.observe("recovery", record(mcp, "effect"))
            }),
        },
        { ...promise, revision: "1" },
      ])
      const call = SessionModelOutcome.observe(hooks, input, (observer) =>
        Effect.sync(() => observer.finish({ normalized: "stop" }, "accepted")),
      )
      yield* call
      yield* Skill.observeLoad(hooks, skillInput, Effect.void)
      const recovery = Object.freeze({
        operation: "tool" as const,
        reason: "session_expired" as const,
        phase: "retry" as const,
        outcome: "succeeded" as const,
      })
      yield* hooks.notify("mcp", "recovery", recovery)
      expect(model.map(([name]) => name)).toEqual(["effect", "promise"])
      expect(filtered).toBe(0)
      expect(model[0][1]).toBe(model[1][1])
      expect(model[0][1]).toMatchObject({ ...input, status: "completed", responseModel: "accepted" })
      expect(Object.isFrozen(model[1][1])).toBe(true)
      expect(skills.map(([name]) => name)).toEqual(["effect", "promise"])
      expect(skills[0][1]).toBe(skills[1][1])
      expect(skills[0][1]).toMatchObject({ ...skillInput, status: "completed" })
      expect(Object.isFrozen(skills[1][1].skill)).toBe(true)
      expect(mcp).toEqual([
        ["effect", recovery],
        ["promise", recovery],
      ])
      expect(mcp[0][1]).toBe(mcp[1][1])
      yield* dispose
      yield* dispose
      yield* call
      expect(model.map(([name]) => name)).toEqual(["effect", "promise", "promise"])
      yield* plugins.activate([])
      yield* call
      yield* Skill.observeLoad(hooks, skillInput, Effect.void)
      yield* hooks.notify("mcp", "recovery", recovery)
      expect(model).toHaveLength(3)
      expect(skills).toHaveLength(2)
      expect(mcp).toHaveLength(2)
    }),
)

it.effect("Promise adapter observations stay synchronous and do not await invalid async returns", () =>
  Effect.gen(function* () {
    const plugins = yield* Plugin.Service
    const hooks = yield* PluginHooks.Service
    const pending = Promise.withResolvers<undefined>()
    const seen: string[] = []
    let settled = false
    const promise = fromPromise({
      id: "invalid-promise-source-observer",
      setup: async (ctx) => {
        // @ts-expect-error Promise registration is async, but observation callbacks must be synchronous.
        await ctx.session.observe("model.outcome", () => pending.promise)
        // @ts-expect-error Rejected async observation work must be consumed without awaiting it.
        await ctx.skill.observe("load", async () => {
          throw new Error("private rejected work")
        })
        await ctx.session.observe("model.outcome", (event) => void seen.push(event.kind))
        await ctx.skill.observe("load", (event) => void seen.push(event.channel))
      },
    })
    yield* plugins.activate([{ ...promise, revision: "1" }])
    const call = yield* SessionModelOutcome.observe(hooks, input, (observer) =>
      Effect.sync(() => observer.finish()),
    ).pipe(
      Effect.andThen(Skill.observeLoad(hooks, skillInput, Effect.void)),
      Effect.onExit(() =>
        Effect.sync(() => {
          settled = true
        }),
      ),
      Effect.forkScoped({ startImmediately: true }),
    )
    yield* Effect.yieldNow
    const settledBeforeRelease = settled
    pending.resolve(undefined)
    yield* Fiber.join(call)
    yield* Effect.promise(() => Promise.resolve())
    expect(settledBeforeRelease).toBe(true)
    expect(seen).toEqual(["primary", "tool"])
  }),
)
