export * as SessionModelOutcome from "./model-outcome.js"

import { LLMEvent, type FinishReasonDetails } from "@opencode/ai"
import type { SessionModelOutcome } from "@opencode/plugin/effect/session"
import { Clock, Effect, Exit } from "effect"
import type { PluginHooks } from "../plugin/hooks.js"

interface Observer {
  readonly event: (event: LLMEvent) => void
  /** Native compaction has no event stream; its accepted response is terminal. */
  readonly finish: (reason?: FinishReasonDetails, responseModel?: string) => void
}

/** The source owner supplies events; observational callback defects must not change the model call's exit. */
export const observe = <A, E, R>(
  hooks: PluginHooks.Interface | undefined,
  input: Pick<SessionModelOutcome, "sessionID" | "agent" | "model" | "kind">,
  run: (observer: Observer) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.gen(function* () {
    // Direct Step consumers can omit observation without acquiring a plugin-service dependency.
    if (!hooks) return yield* Effect.suspend(() => run({ event: () => {}, finish: () => {} }))
    const started = yield* Clock.monotonicTimeNanos
    let finished = false
    let failed = false
    let responseModel: string | undefined
    const finish: Observer["finish"] = (reason, model) => {
      finished = true
      if (reason?.normalized === "error" || reason?.normalized === "unknown" || reason?.normalized === "content-filter")
        failed = true
      if (model !== undefined) responseModel = model
    }
    return yield* Effect.suspend(() =>
      run({
        finish,
        event: (event) => {
          if (LLMEvent.is.providerError(event)) failed = true
          if (LLMEvent.is.finish(event)) finish(event.reason, event.responseModel)
        },
      }),
    ).pipe(
      Effect.onExit((exit) =>
        Effect.gen(function* () {
          const durationMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1_000_000
          yield* hooks.notify(
            "session",
            "model.outcome",
            Object.freeze({
              ...input,
              model: Object.freeze({ ...input.model }),
              status: Exit.hasInterrupts(exit)
                ? "interrupted"
                : Exit.isFailure(exit) || failed || !finished
                  ? "failed"
                  : "completed",
              durationMs,
              ...(responseModel === undefined ? {} : { responseModel }),
            }),
          )
        }).pipe(
          Effect.catchCause(() => Effect.logWarning("session model outcome observation failed", { kind: input.kind })),
        ),
      ),
    )
  })
