import { describe, expect } from "bun:test"
import { AIError, ProviderInternalError, TransportError } from "@opencode/ai"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/core/provider"
import { SessionSchema } from "@opencode/core/session/schema"
import { SessionRunnerRetry } from "@opencode/core/session/runner/retry"
import type { SessionRetryObservation } from "@opencode/plugin/effect/session"
import { toSessionError } from "@opencode/core/session/to-session-error"
import { Effect } from "effect"
import { it } from "./lib/effect"

const timeout = new AIError({
  reason: new TransportError({ message: "Timed out", transport: "http", operation: "request", code: "Timeout" }),
})
const internal = new AIError({ reason: new ProviderInternalError({ message: "internal" }) })

const input = (cause: AIError) => ({
  cause,
  error: toSessionError(cause),
  agent: Agent.ID.make("build"),
  model: Model.Ref.make({ id: Model.ID.make("model"), providerID: Provider.ID.make("provider") }),
  hook: () => Effect.void,
  retry: SessionRunnerRetry.isRetryable(cause),
})

// Decisions for `count` consecutive failures of the same cause within one step.
const decisions = (cause: AIError, count: number) =>
  Effect.gen(function* () {
    const policy = yield* SessionRunnerRetry.policy(SessionSchema.ID.make("ses_retry"))
    const results: boolean[] = []
    for (let i = 0; i < count; i++) results.push((yield* policy(input(cause))).retry)
    return results
  })

describe("SessionRunnerRetry.policy", () => {
  it.effect("observes the normalized final choice, hook overrides, and exhausted allowance", () =>
    Effect.gen(function* () {
      const policy = yield* SessionRunnerRetry.policy(SessionSchema.ID.make("ses_retry_observed"))
      const seen: SessionRetryObservation[] = []
      const observe = (event: SessionRetryObservation) =>
        Effect.sync(() => {
          seen.push(event)
        })
      const requested = input(internal)
      expect(
        yield* policy({
          ...requested,
          observe,
          hook: (event) =>
            Effect.sync(() => {
              event.decision = { retry: true, delay: 1.2 }
            }),
        }),
      ).toMatchObject({ retry: true, delay: 2 })
      expect(seen[0]).toMatchObject({
        retryable: true,
        source: "hook",
        reason: "scheduled",
        decision: { retry: true, delay: 2 },
      })
      expect(Object.isFrozen(seen[0]?.decision)).toBe(true)
      for (let i = 0; i < 10; i++) yield* policy({ ...requested, observe })
      expect(seen.at(-1)).toMatchObject({ source: "policy", reason: "exhausted", decision: { retry: false } })
      expect(seen).toHaveLength(11)
    }),
  )

  it.effect("distinguishes provider minimum delays and host refusal from retryability", () =>
    Effect.gen(function* () {
      const policy = yield* SessionRunnerRetry.policy(SessionSchema.ID.make("ses_retry_sources"))
      const seen: SessionRetryObservation[] = []
      const observe = (event: SessionRetryObservation) =>
        Effect.sync(() => {
          seen.push(event)
        })
      const cause = new AIError({ reason: new ProviderInternalError({ message: "busy", retryAfterMs: 30_000 }) })
      yield* policy({ ...input(cause), observe })
      yield* policy({ ...input(internal), observe, retry: false })
      expect(seen[0]).toMatchObject({ source: "retry-after", decision: { retry: true, delay: 30_000 } })
      expect(seen[1]).toMatchObject({
        retryable: true,
        source: "policy",
        reason: "rejected",
        decision: { retry: false },
      })
    }),
  )
  it.effect("stops retrying transport timeouts after three attempts", () =>
    Effect.gen(function* () {
      expect(yield* decisions(timeout, 4)).toEqual([true, true, true, false])
    }),
  )

  it.effect("keeps the general allowance for other transient failures", () =>
    Effect.gen(function* () {
      expect(yield* decisions(internal, 10)).toEqual(Array(10).fill(true))
    }),
  )
})
