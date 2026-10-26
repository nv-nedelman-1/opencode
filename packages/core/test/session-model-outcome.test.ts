import { expect } from "bun:test"
import {
  AIError,
  AuthenticationError,
  InvalidProviderOutputError,
  InvalidRequestError,
  LLMEvent,
  RateLimitError,
  TimeoutError,
} from "@opencode/ai"
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
  const outcomes: PluginHooks.Observations["session"]["model.outcome"][] = []
  yield* hooks.observe("session", "model.outcome", (event) => {
    outcomes.push(event)
    return undefined
  })
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
    expect(outcomes).toEqual([
      { ...input, status: "completed", durationMs: 25, responseModel: "accepted-snapshot", finishReason: "stop" },
    ])
    expect(outcomes[0]).not.toHaveProperty("firstOutputMs")
    expect(outcomes[0]).not.toHaveProperty("errorKind")
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
      expect(outcomes[0].finishReason).toBe(reason)
      expect(outcomes[0].errorKind).toBe(reason === "content-filter" ? "content-filter" : "unknown")
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
    expect(outcomes.map((outcome) => outcome.errorKind)).toEqual(["incomplete-stream", "unknown"])
  }),
)

it.effect("preserves typed failure when the terminal observer defects", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    yield* hooks.observe("session", "model.outcome", () => {
      throw new Error("private callback payload")
    })
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
    yield* hooks.observe("session", "model.outcome", () => {
      throw new Error("private callback payload")
    })
    const fiber = yield* SessionModelOutcome.observe(hooks, input, (observer) =>
      Effect.gen(function* () {
        observer.event(LLMEvent.textDelta({ id: "partial", text: "partial" }))
        observer.finish({ normalized: "stop" }, "partial-accepted")
        yield* Deferred.succeed(entered, undefined)
        return yield* Effect.never
      }).pipe(Effect.onInterrupt(() => TestClock.adjust("17 millis"))),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(entered)
    yield* Fiber.interrupt(fiber)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.hasInterrupts(exit)).toBe(true)
    expect(outcomes).toEqual([
      {
        ...input,
        status: "interrupted",
        durationMs: 17,
        responseModel: "partial-accepted",
        finishReason: "stop",
        firstOutputMs: 0,
        firstOutputKind: "text",
      },
    ])
    expect(outcomes[0]).not.toHaveProperty("errorKind")
  }),
)

it.effect("measures meaningful streamed output once at its owner, not at terminal delivery", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    const cases = [
      ["primary", "text", LLMEvent.textDelta({ id: "text", text: "answer" })],
      ["title", "reasoning", LLMEvent.reasoningDelta({ id: "reasoning", text: "thinking" })],
      ["compaction", "tool", LLMEvent.toolInputDelta({ id: "call", name: "lookup", text: "{" })],
      ["primary", "tool", LLMEvent.toolCall({ id: "call", name: "lookup", input: {} })],
    ] as const
    for (const [kind, outputKind, event] of cases) {
      yield* SessionModelOutcome.observe(hooks, { ...input, kind }, (observer) =>
        Effect.gen(function* () {
          observer.event(LLMEvent.textStart({ id: "text" }))
          observer.event(LLMEvent.textDelta({ id: "text", text: "" }))
          observer.event(LLMEvent.reasoningDelta({ id: "reasoning", text: "" }))
          observer.event(LLMEvent.toolInputStart({ id: "call", name: "lookup" }))
          observer.event(LLMEvent.toolInputDelta({ id: "call", name: "lookup", text: "" }))
          yield* TestClock.adjust("25 millis")
          observer.event(event)
          yield* TestClock.adjust("75 millis")
          observer.event(LLMEvent.textDelta({ id: "text", text: "later" }))
          observer.finish({ normalized: "stop", raw: "private raw finish", explanation: "private explanation" })
        }),
      )
      expect(outcomes.at(-1)).toEqual({
        ...input,
        kind,
        status: "completed",
        durationMs: 100,
        finishReason: "stop",
        firstOutputMs: 25,
        firstOutputKind: outputKind,
      })
    }
  }),
)

it.effect("classifies typed model failures without exposing provider payloads", () =>
  Effect.gen(function* () {
    const { hooks, outcomes } = yield* collect
    const cases = [
      ["authentication", new AuthenticationError({ message: "private secret" })],
      ["rate-limit", new RateLimitError({ message: "private secret", retryAfterMs: 10 })],
      ["timeout", new TimeoutError({ message: "private secret", timeoutMs: 100 })],
      [
        "context-overflow",
        new InvalidRequestError({ message: "private secret", classification: "context-overflow" }),
      ],
      [
        "incomplete-stream",
        new InvalidProviderOutputError({ message: "private secret", classification: "incomplete-stream" }),
      ],
    ] as const
    for (const [errorKind, reason] of cases) {
      const failure = new AIError({ reason })
      const exit = yield* SessionModelOutcome.observe(hooks, input, () => Effect.fail(failure)).pipe(Effect.exit)
      expect(exit).toEqual(Exit.fail(failure))
      expect(outcomes.at(-1)).toEqual({ ...input, status: "failed", durationMs: 0, errorKind })
    }
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

it.effect("invalid async or Effect observations cannot hold success, failure, or interruption settlement", () =>
  Effect.gen(function* () {
    const hooks = yield* PluginHooks.Service
    for (const status of ["completed", "failed", "interrupted"] as const) {
      yield* Effect.scoped(
        Effect.gen(function* () {
          const pending = Promise.withResolvers<undefined>()
          const release = yield* Deferred.make<void>()
          let foreignRan = false
          let settled = false
          let first: PluginHooks.Observations["session"]["model.outcome"] | undefined
          const received: PluginHooks.Observations["session"]["model.outcome"][] = []
          yield* hooks.observe("session", "model.outcome", (event) => {
            first = event
            throw new Error("private observer defect")
          })
          // @ts-expect-error Observations cannot return a pending promise, even from untyped plugins.
          yield* hooks.observe("session", "model.outcome", () => pending.promise)
          // @ts-expect-error Async callbacks are not source observers; rejected work is consumed, not awaited.
          yield* hooks.observe("session", "model.outcome", async () => {
            throw new Error("private async rejection")
          })
          const foreign = Effect.sync(() => {
            foreignRan = true
          }).pipe(Effect.andThen(Deferred.await(release)))
          // @ts-expect-error Returning an Effect must not run it or hold uninterruptible cleanup.
          yield* hooks.observe("session", "model.outcome", () => foreign)
          yield* hooks.observe("session", "model.outcome", (event) => {
            received.push(event)
            return undefined
          })
          const failure = new Error("original failure")
          const call = yield* SessionModelOutcome.observe(hooks, input, (observer) => {
            if (status === "failed") return Effect.fail(failure)
            if (status === "interrupted") return Effect.interrupt
            observer.finish({ normalized: "stop" }, "accepted")
            return Effect.succeed("unchanged")
          }).pipe(
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
          expect(received[0].status).toBe(status)
          expect(Object.isFrozen(received[0])).toBe(true)
          expect(Object.isFrozen(received[0].model)).toBe(true)
          if (status === "completed") expect(exit).toEqual(Exit.succeed("unchanged"))
          if (status === "failed") expect(exit).toEqual(Exit.fail(failure))
          if (status === "interrupted") expect(Exit.hasInterrupts(exit)).toBe(true)
        }),
      )
    }
  }),
)
