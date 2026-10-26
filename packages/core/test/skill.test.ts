import { describe, expect } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schedule, Stream } from "effect"
import type { SkillLoad } from "@opencode/plugin/effect/skill"
import { Agent } from "@opencode/core/agent"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { ManagedPolicy } from "@opencode/core/managed-policy"
import { Document, Event } from "@opencode/schema/config"
import { AbsolutePath } from "@opencode/core/schema"
import { Skill } from "@opencode/core/skill"
import { SkillInstructions } from "@opencode/core/skill/instructions"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Session } from "@opencode/schema/session"
import { testEffect } from "./lib/effect"
import { readInitial } from "./lib/instructions"
import { registerIntegrationPolicy } from "./fixture/policy"

const configLayer = Config.testLayer()
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Skill.node, SkillInstructions.node, Agent.node, Bus.node, ManagedPolicy.node, PluginHooks.node]),
    [Config.node.replace(configLayer)],
  ).pipe(Layer.provideMerge(configLayer)),
)

const info = (id: string, description: string) =>
  Skill.Info.make({
    id: Skill.ID.make(id),
    name: Skill.Name.make(id),
    description,
    path: AbsolutePath.make(`/skills/${id}/SKILL.md`),
    content: `# ${id}`,
  })

describe("Skill", () => {
  it.effect("keeps source provenance with effective registrations and restores it on disposal", () =>
    Effect.gen(function* () {
      const skills = yield* Skill.Service
      const original = info("review", "Original")
      yield* skills.transform((editor) => editor.add(original))
      expect((yield* skills.resolve(original.id))?.sourceKind).toBe("unknown")
      const overlay = yield* skills.transform((editor) => editor.add(info("review", "Remote"), "url"))
      const remote = yield* skills.resolve(original.id)
      expect(remote?.sourceKind).toBe("url")
      yield* skills.transform((editor) => editor.update("review", (skill) => (skill.content = "Updated")))
      expect((yield* skills.resolve(original.id))?.sourceKind).toBe("url")
      yield* overlay.dispose
      expect((yield* skills.resolve(original.id))?.sourceKind).toBe("unknown")
      expect(remote).toMatchObject({ skill: { description: "Remote" }, sourceKind: "url" })
      yield* skills.transform((editor) => editor.remove("review"))
      expect(yield* skills.resolve(original.id)).toBeUndefined()
    }),
  )

  it.effect("reports load outcomes without changing results, failures, or interruption when an observer defects", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      const observations: SkillLoad[] = []
      yield* hooks.observe("skill", "load", (event) => {
        observations.push(event)
        return undefined
      })
      const observer = yield* hooks.observe("skill", "load", () => {
        throw new Error("observer failure")
      })
      const input = {
        sessionID: Session.ID.make("ses_skill_load"),
        channel: "tool" as const,
        skill: info("review", "Review"),
        sourceKind: "directory" as const,
      }
      expect(yield* Skill.observeLoad(hooks, input, Effect.succeed("unchanged"))).toBe("unchanged")
      const error = new Error("preparation failure")
      expect(yield* Skill.observeLoad(hooks, input, Effect.fail(error)).pipe(Effect.flip)).toBe(error)
      expect(
        yield* Skill.observeLoad(
          hooks,
          { sessionID: input.sessionID, channel: input.channel },
          Effect.fail(error),
        ).pipe(Effect.flip),
      ).toBe(error)
      const interrupted = yield* Skill.observeLoad(hooks, input, Effect.interrupt).pipe(Effect.exit)
      expect(Exit.isFailure(interrupted) && Cause.hasInterruptsOnly(interrupted.cause)).toBe(true)
      expect(observations.map((event) => [event.status, event.sourceKind])).toEqual([
        ["completed", "directory"],
        ["failed", "directory"],
        ["unavailable", "unknown"],
        ["interrupted", "directory"],
      ])
      yield* observer.dispose
      // @ts-expect-error Returned Effects are not observers and must never be executed.
      yield* hooks.observe("skill", "load", () => Effect.interrupt)
      expect(yield* Skill.observeLoad(hooks, input, Effect.succeed("still unchanged"))).toBe("still unchanged")
    }),
  )

  it.effect("observes frozen snapshots without exposing the mutable catalog or changing returned skills", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      const skills = yield* Skill.Service
      const original = info("review", "Original")
      yield* skills.transform((editor) => editor.add(original, "directory"))
      const resolved = (yield* skills.resolve(original.id))!
      const seen: SkillLoad[] = []
      yield* hooks.observe("skill", "load", (event) => {
        expect(() => Object.assign(event, { status: "failed" })).toThrow()
        expect(() => Object.assign(event.skill!, { content: "Changed" })).toThrow()
        expect(() => Object.assign(event.skill!, { path: "/changed" })).toThrow()
        return undefined
      })
      yield* hooks.observe("skill", "load", (event) => {
        seen.push(event)
        return undefined
      })
      expect(
        yield* Skill.observeLoad(
          hooks,
          { sessionID: Session.ID.make("ses_skill_snapshot"), channel: "user_activation", ...resolved },
          Effect.succeed(resolved.skill),
        ),
      ).toEqual(original)
      expect(yield* skills.get(original.id)).toEqual(original)
      expect(seen[0]).toMatchObject({ status: "completed", skill: original })
      expect(seen[0].skill).not.toBe(resolved.skill)
      expect(Object.isFrozen(resolved.skill)).toBe(false)
      yield* skills.transform((editor) => editor.update(original.id, (skill) => (skill.content = "Updated")))
      expect((yield* skills.get(original.id))?.content).toBe("Updated")
      expect(seen[0].skill).toEqual(original)
    }),
  )

  it.effect("invalid async or Effect load observers cannot delay success, failure, or interruption", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      for (const status of ["completed", "failed", "interrupted"] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const pending = Promise.withResolvers<undefined>()
            const release = yield* Deferred.make<void>()
            let foreignRan = false
            let settled = false
            let first: SkillLoad | undefined
            const received: SkillLoad[] = []
            yield* hooks.observe("skill", "load", (event) => {
              first = event
              throw new Error("private observer defect")
            })
            // @ts-expect-error An untyped pending promise cannot hold load settlement.
            yield* hooks.observe("skill", "load", () => pending.promise)
            // @ts-expect-error Async observers are rejected by the source observation contract.
            yield* hooks.observe("skill", "load", async () => {
              throw new Error("private async rejection")
            })
            const foreign = Effect.sync(() => {
              foreignRan = true
            }).pipe(Effect.andThen(Deferred.await(release)))
            // @ts-expect-error Do not run or await returned Effects in load finalizers.
            yield* hooks.observe("skill", "load", () => foreign)
            yield* hooks.observe("skill", "load", (event) => {
              received.push(event)
              return undefined
            })
            const failure = new Error("original failure")
            const input = {
              sessionID: Session.ID.make("ses_load_stall"),
              channel: "tool" as const,
              sourceKind: "directory" as const,
              skill: info("review", "Review"),
            }
            const work =
              status === "completed"
                ? Effect.succeed("unchanged")
                : status === "failed"
                  ? Effect.fail(failure)
                  : Effect.interrupt
            const call = yield* Skill.observeLoad(hooks, input, work).pipe(
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
            yield* Deferred.succeed(release, undefined)
            const exit = yield* Fiber.await(call)
            yield* Effect.promise(() => Promise.resolve())
            expect(settledBeforeRelease).toBe(true)
            expect(foreignRan).toBe(false)
            expect(received).toHaveLength(1)
            expect(received[0]).toBe(first!)
            expect(received[0]).toMatchObject({ ...input, status })
            expect(Object.isFrozen(received[0])).toBe(true)
            expect(Object.isFrozen(received[0].skill)).toBe(true)
            if (status === "completed") expect(exit).toEqual(Exit.succeed("unchanged"))
            if (status === "failed") expect(exit).toEqual(Exit.fail(failure))
            if (status === "interrupted") expect(Exit.hasInterrupts(exit)).toBe(true)
          }),
        )
      }
    }),
  )

  it.live("hides and refuses denied skills while organization policy overrides local allows", () =>
    Effect.gen(function* () {
      const skills = yield* Skill.Service
      const managed = yield* ManagedPolicy.Service
      const config = yield* Config.Test
      yield* skills.transform((editor) => {
        editor.add(info("team:review", "Review"))
        editor.add(info("deploy", "Deploy"))
      })
      yield* config.setEntries([
        new Document({
          type: "document",
          info: {
            experimental: { policies: [{ action: "integration.use", resource: "skill:*", effect: "allow" }] },
          },
        }),
      ])
      yield* managed.set({
        statements: [
          { action: "integration.use", resource: "*", effect: "deny" },
          { action: "integration.use", resource: "skill:team:review", effect: "allow" },
        ],
      })
      yield* registerIntegrationPolicy({ skill: skills })
      expect(yield* skills.list()).toEqual([info("team:review", "Review")])
      expect(yield* skills.get(Skill.ID.make("deploy"))).toBeUndefined()
      expect(yield* skills.get(Skill.ID.make("team:review"))).toEqual(info("team:review", "Review"))
      const instructions = yield* SkillInstructions.Service
      const guidance = yield* instructions.load([]).pipe(Effect.flatMap(readInitial))
      expect(guidance.text).toContain("<id>team:review</id>")
      expect(guidance.text).not.toContain("deploy")

      yield* managed.set({ statements: [{ action: "integration.use", resource: "skill:team:review", effect: "deny" }] })
      yield* waitUntil(skills.get(Skill.ID.make("team:review")).pipe(Effect.map((skill) => skill === undefined)))
      expect(yield* skills.get(Skill.ID.make("team:review"))).toBeUndefined()
      expect(yield* skills.list()).toEqual([info("deploy", "Deploy")])
      yield* managed.set({ statements: [] })
      yield* waitUntil(skills.list().pipe(Effect.map((skills) => skills.length === 2)))
      expect(yield* skills.list()).toHaveLength(2)
    }),
  )

  it.live("applies configuration precedence to skills without changing their registered values", () =>
    Effect.gen(function* () {
      const skills = yield* Skill.Service
      const config = yield* Config.Test
      yield* skills.transform((editor) => editor.add(info("review", "Review")))
      yield* config.setEntries([
        new Document({
          type: "document",
          info: { experimental: { policies: [{ action: "integration.use", resource: "skill:*", effect: "deny" }] } },
        }),
        new Document({
          type: "document",
          info: {
            experimental: { policies: [{ action: "integration.use", resource: "skill:review", effect: "allow" }] },
          },
        }),
      ])
      const bus = yield* Bus.Service
      yield* registerIntegrationPolicy({ skill: skills, events: bus.subscribe() })
      expect(yield* skills.list()).toEqual([])
      expect(yield* skills.get(Skill.ID.make("review"))).toBeUndefined()
      yield* config.setEntries([])
      yield* bus.publish(Event.Updated, {})
      yield* waitUntil(skills.get(Skill.ID.make("review")).pipe(Effect.map((skill) => skill !== undefined)))
      expect(yield* skills.get(Skill.ID.make("review"))).toEqual(info("review", "Review"))
    }),
  )

  it.live("publishes catalog updates when managed policy changes", () =>
    Effect.gen(function* () {
      const skills = yield* Skill.Service
      const managed = yield* ManagedPolicy.Service
      const bus = yield* Bus.Service
      yield* skills.transform((editor) => editor.add(info("review", "Review")))
      yield* registerIntegrationPolicy({ skill: skills })
      const updated = yield* Deferred.make<Skill.Info[]>()
      yield* bus.subscribe(Skill.Event.Updated).pipe(
        Stream.runForEach(() => skills.list().pipe(Effect.flatMap((values) => Deferred.succeed(updated, values)))),
        Effect.forkScoped({ startImmediately: true }),
      )
      yield* managed.set({ statements: [{ action: "integration.use", resource: "skill:review", effect: "deny" }] })
      expect(yield* Deferred.await(updated).pipe(Effect.timeout("1 second"))).toEqual([])
    }),
  )

  it.effect("reads the current editor entry by ID", () =>
    Effect.gen(function* () {
      const skill = yield* Skill.Service
      yield* skill.transform((editor) => editor.add(info("review", "Initial")))
      yield* skill.transform((editor) => {
        expect(editor.get("review")).toBe(editor.list()[0])
        expect(editor.get("missing")).toBeUndefined()
        editor.update("review", (value) => {
          value.description = "Updated"
        })
        expect(editor.get("review")?.description).toBe("Updated")
        editor.remove("review")
        expect(editor.get("review")).toBeUndefined()
      })

      expect(yield* skill.list()).toEqual([])
    }),
  )

  it.effect("registers values with last-write-wins precedence", () =>
    Effect.gen(function* () {
      const skill = yield* Skill.Service
      yield* skill.transform((editor) => {
        editor.add(info("review", "First"))
        editor.add(info("deploy", "Deploy"))
        editor.add(info("review", "Second"))
        expect(editor.list().map((item) => item.id)).toEqual([Skill.ID.make("review"), Skill.ID.make("deploy")])
      })

      expect(yield* skill.list()).toEqual([info("review", "Second"), info("deploy", "Deploy")])
      expect(yield* skill.get(Skill.ID.make("review"))).toEqual(info("review", "Second"))
      expect(yield* skill.get(Skill.ID.make("missing"))).toBeUndefined()
    }),
  )

  it.effect("updates and removes registered values", () =>
    Effect.gen(function* () {
      const skill = yield* Skill.Service
      yield* skill.transform((editor) => {
        editor.add(info("review", "Initial"))
        editor.update("review", (value) => {
          value.description = "Updated"
          value.id = Skill.ID.make("ignored")
        })
        editor.update("missing", () => {
          throw new Error("unreachable")
        })
        editor.add(info("deploy", "Deploy"))
        editor.remove("deploy")
      })

      expect(yield* skill.list()).toEqual([info("review", "Updated")])
    }),
  )

  it.effect("restores earlier values when an updating transform is disposed", () =>
    Effect.gen(function* () {
      const skill = yield* Skill.Service
      const original = info("review", "Initial")
      yield* skill.transform((editor) => editor.add(original))
      const updated = yield* skill.transform((editor) =>
        editor.update("review", (value) => {
          value.description = "Updated"
        }),
      )

      expect((yield* skill.list())[0]?.description).toBe("Updated")
      yield* updated.dispose
      expect((yield* skill.list())[0]?.description).toBe("Initial")
      expect(original.description).toBe("Initial")
    }),
  )

  it.live("publishes updates after committed values are visible", () =>
    Effect.gen(function* () {
      const skill = yield* Skill.Service
      const bus = yield* Bus.Service
      const updated = yield* Deferred.make<Skill.Info[]>()
      const fiber = yield* bus.subscribe(Skill.Event.Updated).pipe(
        Stream.runForEach(() => skill.list().pipe(Effect.flatMap((values) => Deferred.succeed(updated, values)))),
        Effect.forkScoped,
      )
      yield* Effect.yieldNow

      yield* skill.transform((editor) => editor.add(info("review", "Visible")))
      expect(yield* Deferred.await(updated).pipe(Effect.timeout("1 second"))).toEqual([info("review", "Visible")])
      yield* Fiber.interrupt(fiber)
    }),
  )

  it.effect("filters values by agent permissions", () =>
    Effect.gen(function* () {
      const agents = yield* Agent.Service
      yield* agents.transform((editor) =>
        editor.update(Agent.ID.make("reviewer"), (agent) => {
          agent.permissions.push({ action: "skill", resource: "deploy", effect: "deny" })
        }),
      )
      const agent = yield* agents.get(Agent.ID.make("reviewer"))
      expect(Skill.available([info("deploy", "Deploy")], agent!.permissions)).toEqual([])
    }),
  )
})

const waitUntil = (condition: Effect.Effect<boolean>) =>
  condition.pipe(
    Effect.filterOrFail(
      (ready) => ready,
      () => new Error("Skill policy was not applied"),
    ),
    Effect.retry({ times: 200, schedule: Schedule.spaced("10 millis") }),
  )
