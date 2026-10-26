import { expect } from "bun:test"
import { LLMEvent } from "@opencode/ai"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { SessionModelOutcome } from "@opencode/core/session/model-outcome"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Session } from "@opencode/schema/session"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Deferred, Effect, Exit, Fiber } from "effect"
import { TestClock } from "effect/testing"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(LayerNode.group([PluginHooks.node])))
const input = {
  sessionID: Session.ID.make("ses_model_outcome"),
  agent: Agent.ID.make("build"),
  model: { providerID: Provider.ID.make("configured"), id: Model.ID.make("configured-alias") },
  kind: "primary" as const,
}

const collect = Effect.gen(function* () {
  const hooks = yield* PluginHooks.Service
  const outcomes: PluginHooks.Domains["session"]["model.outcome"][] = []
  yield* hooks.register("session", "model.outcome", (event) =>
    Effect.sync(() => {
      outcomes.push(event)
    }),
  )
  return { hooks, outcomes }
})

it.effect("observes the exact primary invocation and accepted identity without charging usage", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    let charges = 0
    yield* hooks.register("session", "usage", () =>
      Effect.sync(() => {
        charges++
      }),
    )
    const result = yield* SessionModelOutcome.observe(hooks, input, (observer) =>
      Effect.gen(function* () {
        yield* TestClock.adjust("25 millis")
        observer.event(LLMEvent.finish({ reason: { normalized: "stop" }, responseModel: "accepted-snapshot" }))
        return "answer"
      }),
    )
    expect(result).toBe("answer")
    expect(outcomes).toEqual([{ ...input, status: "completed", durationMs: 25, responseModel: "accepted-snapshot" }])
    expect(Object.isFrozen(outcomes[0])).toBe(true)
    expect(Object.isFrozen(outcomes[0].model)).toBe(true)
    expect(charges).toBe(0)
    expect(outcomes[0]).not.toHaveProperty("tokens")
    expect(outcomes[0]).not.toHaveProperty("cost")
  }),
)

for (const reason of ["error", "unknown", "content-filter"] as const) {
  it.effect(`does not report a ${reason} terminal as success`, () =>
    Effect.gen(function* () {
      const { hooks, outcomes } = yield* collect
      yield* SessionModelOutcome.observe(hooks, input, (observer) =>
        Effect.sync(() => {
          observer.event(LLMEvent.finish({ reason: { normalized: reason } }))
        }),
      )
      expect(outcomes[0].status).toBe("failed")
      expect(outcomes[0]).not.toHaveProperty("responseModel")
    }),
  )
}

it.effect("marks an unterminated stream and a swallowed provider error as failures", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    yield* SessionModelOutcome.observe(hooks, input, () => Effect.void)
    yield* SessionModelOutcome.observe(hooks, input, (observer) =>
      Effect.sync(() => {
        observer.event(LLMEvent.providerError({ message: "provider rejected" }))
        observer.finish({ normalized: "stop" })
      }),
    )
    expect(outcomes.map((outcome) => outcome.status)).toEqual(["failed", "failed"])
  }),
)

it.effect("preserves typed failure when the terminal observer defects", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    yield* hooks.register("session", "model.outcome", () => Effect.die("private callback payload"))
    const failure = { type: "original-failure" }
    const exit = yield* SessionModelOutcome.observe(hooks, input, () => Effect.fail(failure)).pipe(Effect.exit)
    expect(exit).toEqual(Exit.fail(failure))
    expect(outcomes[0].status).toBe("failed")
    expect(
      yield* SessionModelOutcome.observe(hooks, input, (observer) =>
        Effect.sync(() => {
          observer.finish()
          return "unchanged"
        }),
      ),
    ).toBe("unchanged")
  }),
)

it.effect("waits for cancellation cleanup and preserves interruption despite observer defects", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    const entered = yield* Deferred.make<void>()
    yield* hooks.register("session", "model.outcome", () => Effect.die("private callback payload"))
    const fiber = yield* SessionModelOutcome.observe(hooks, input, (observer) =>
      Effect.gen(function* () {
        observer.finish({ normalized: "stop" }, "partial-accepted")
        yield* Deferred.succeed(entered, undefined)
        return yield* Effect.never
      }).pipe(Effect.onInterrupt(() => TestClock.adjust("17 millis"))),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(entered)
    yield* Fiber.interrupt(fiber)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.hasInterrupts(exit)).toBe(true)
    expect(outcomes).toEqual([{ ...input, status: "interrupted", durationMs: 17, responseModel: "partial-accepted" }])
  }),
)

it.effect("keeps overlapping invocation identities separate and omits unknown response identity", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    const firstReady = yield* Deferred.make<void>()
    const releaseFirst = yield* Deferred.make<void>()
    const first = yield* SessionModelOutcome.observe(hooks, { ...input, kind: "title" }, (observer) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(firstReady, undefined)
        yield* Deferred.await(releaseFirst)
        observer.finish({ normalized: "stop" }, "title-accepted")
      }),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(firstReady)
    yield* SessionModelOutcome.observe(hooks, input, (observer) => Effect.sync(() => observer.finish()))
    yield* Deferred.succeed(releaseFirst, undefined)
    yield* Fiber.join(first)
    expect(outcomes.map((outcome) => [outcome.kind, outcome.responseModel])).toEqual([
      ["primary", undefined],
      ["title", "title-accepted"],
    ])
    expect(outcomes[0]).not.toHaveProperty("responseModel")
  }),
)
