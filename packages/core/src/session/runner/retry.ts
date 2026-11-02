export * as SessionRunnerRetry from "./retry.js"

import { AIError, isRetryable } from "@opencode/ai"
import type { SessionRequestKind } from "@opencode/plugin/effect/session"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { SessionError } from "@opencode/schema/session-error"
import { Clock, Duration, Effect, Pull, Schedule } from "effect"
import { Bus } from "../../bus.js"
import type { PluginHooks } from "../../plugin/hooks.js"
import { SessionEvent } from "../event.js"
import { SessionMessage } from "../message.js"
import { SessionSchema } from "../schema.js"

export { isRetryable }

interface Input {
  readonly cause: AIError
  readonly error: SessionError.Error
  readonly agent: Agent.ID
  readonly model: Model.Ref
  readonly kind?: SessionRequestKind
  readonly hook: (event: PluginHooks.Domains["session"]["retry"]) => Effect.Effect<void>
  readonly observe?: (event: PluginHooks.Domains["session"]["retry.decision"]) => Effect.Effect<void>
  readonly retry: boolean
}

export interface Decision {
  readonly retry: true
  readonly attempt: number
  readonly delay: number
}

/** Bound provider-requested delays so a hostile or buggy retry-after cannot stall a session for hours. */
const RETRY_AFTER_MAX = Duration.toMillis("15 minutes")

const retryAfter = (input: Input) => {
  if (input.cause.reason._tag === "RateLimit" || input.cause.reason._tag === "ProviderInternal")
    return input.cause.reason.retryAfterMs === undefined
      ? undefined
      : Math.min(input.cause.reason.retryAfterMs, RETRY_AFTER_MAX)
  return undefined
}

// Exponential from 2s capped at 10s per gap, for 10 retries: 2, 4, 8, then 10 × 7, about 84s of
// waiting when every attempt fails (67–101s with jitter). `min` takes the faster schedule, so the
// cap applies per gap; `max` with `recurs` bounds the count.
const schedule = Schedule.max([
  Schedule.min([Schedule.exponential("2 seconds"), Schedule.spaced("10 seconds")]),
  Schedule.recurs(10),
]).pipe(
  Schedule.jittered,
  Schedule.setInputType<Input>(),
  Schedule.modifyDelay(({ input, duration: delay }) => {
    const minimum = retryAfter(input)
    const duration = minimum === undefined ? delay : Duration.max(delay, Duration.millis(minimum))
    return Effect.succeed(Duration.millis(Math.ceil(Duration.toMillis(duration))))
  }),
)

// A timed-out attempt already waited minutes before failing, so the general allowance would let a
// dead provider hold a step for most of an hour. Cap those attempts well below it.
const MAX_TIMEOUT_RETRIES = 3

const isTimeout = (error: AIError) => error.reason._tag === "Transport" && error.reason.code === "Timeout"

export const policy = (sessionID: SessionSchema.ID) =>
  Effect.gen(function* () {
    const step = yield* Schedule.toStep(schedule)
    let attempt = 1
    let timeouts = 0
    return (input: Input) =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis
        const next = yield* step(now, input).pipe(Pull.catchDone(() => Effect.succeed(undefined)))
        if (!next) {
          yield* input.observe?.(
            Object.freeze({
              sessionID,
              agent: input.agent,
              model: input.model,
              kind: input.kind ?? "primary",
              error: input.error,
              attempt: attempt + 1,
              retryable: isRetryable(input.cause),
              decision: Object.freeze({ retry: false }),
              source: "policy",
              reason: "exhausted",
            }),
          ) ?? Effect.void
          return { retry: false as const }
        }
        const [, duration] = next
        attempt++
        if (isTimeout(input.cause)) timeouts++
        const delay = Math.ceil(Duration.toMillis(duration))
        const proposed =
          input.retry && timeouts <= MAX_TIMEOUT_RETRIES ? { retry: true as const, delay } : { retry: false as const }
        const event: PluginHooks.Domains["session"]["retry"] = {
          sessionID,
          agent: input.agent,
          model: input.model,
          error: input.error,
          attempt,
          decision: { ...proposed },
        }
        yield* input.hook(event)
        const normalized =
          event.decision.retry && Number.isFinite(event.decision.delay) && event.decision.delay >= 0
            ? Math.ceil(event.decision.delay)
            : delay
        const decision = event.decision.retry ? { retry: true as const, delay: normalized } : { retry: false as const }
        const changed =
          proposed.retry !== decision.retry || (proposed.retry && decision.retry && proposed.delay !== decision.delay)
        yield* input.observe?.(
          Object.freeze({
            sessionID,
            agent: input.agent,
            model: input.model,
            kind: input.kind ?? "primary",
            error: input.error,
            attempt,
            retryable: isRetryable(input.cause),
            decision: Object.freeze(decision),
            source: changed
              ? "hook"
              : decision.retry && Math.ceil(retryAfter(input) ?? -1) === normalized
                ? "retry-after"
                : "policy",
            reason: decision.retry ? "scheduled" : timeouts > MAX_TIMEOUT_RETRIES ? "timeout-limit" : "rejected",
          }),
        ) ?? Effect.void
        return decision.retry ? { ...decision, attempt } : decision
      })
  })

export const make = (bus: Bus.Interface, sessionID: SessionSchema.ID) =>
  Effect.gen(function* () {
    const decide = yield* policy(sessionID)
    const wait = (input: {
      readonly decision: Decision
      readonly assistantMessageID: SessionMessage.ID
      readonly error: SessionError.Error
    }) =>
      Effect.gen(function* () {
        const scheduled = yield* Clock.currentTimeMillis
        yield* bus.publish(SessionEvent.RetryScheduled, {
          sessionID,
          assistantMessageID: input.assistantMessageID,
          attempt: input.decision.attempt,
          at: scheduled + input.decision.delay,
          error: input.error,
        })
        const remaining = Math.max(0, scheduled + input.decision.delay - (yield* Clock.currentTimeMillis))
        yield* Effect.sleep(Duration.millis(remaining))
      })
    return { decide, wait }
  })
