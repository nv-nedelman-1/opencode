export * as SessionModelOutcome from "./model-outcome.js"

import { AIError, LLMEvent, type FinishReasonDetails } from "@opencode/ai"
import type { SessionModelOutcome } from "@opencode/plugin/effect/session"
import { Cause, Clock, Effect, Exit } from "effect"
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
    const clock = yield* Clock.Clock
    const started = clock.monotonicTimeNanosUnsafe()
    let finished = false
    let failed = false
    let responseModel: string | undefined
    let finishReason: SessionModelOutcome["finishReason"]
    let errorKind: SessionModelOutcome["errorKind"]
    let firstOutputMs: number | undefined
    let firstOutputKind: SessionModelOutcome["firstOutputKind"]
    const finish: Observer["finish"] = (reason, model) => {
      finished = true
      finishReason = reason?.normalized
      if (reason?.normalized === "error" || reason?.normalized === "unknown" || reason?.normalized === "content-filter") {
        failed = true
        errorKind ??= reason.normalized === "content-filter" ? "content-filter" : "unknown"
      }
      if (model !== undefined) responseModel = model
    }
    return yield* Effect.suspend(() =>
      run({
        finish,
        event: (event) => {
          if (LLMEvent.is.providerError(event)) {
            failed = true
            errorKind = event.classification ?? "unknown"
          }
          if (LLMEvent.is.finish(event)) finish(event.reason, event.responseModel)
          if (firstOutputMs !== undefined) return
          const kind =
            LLMEvent.is.textDelta(event) && event.text.length > 0
              ? "text"
              : LLMEvent.is.reasoningDelta(event) && event.text.length > 0
                ? "reasoning"
                : LLMEvent.is.toolCall(event) || (LLMEvent.is.toolInputDelta(event) && event.text.length > 0)
                  ? "tool"
                  : undefined
          if (kind === undefined) return
          firstOutputMs = Number(clock.monotonicTimeNanosUnsafe() - started) / 1_000_000
          firstOutputKind = kind
        },
      }),
    ).pipe(
      Effect.onExit((exit) =>
        Effect.gen(function* () {
          const durationMs = Number(clock.monotonicTimeNanosUnsafe() - started) / 1_000_000
          const status = Exit.hasInterrupts(exit)
            ? "interrupted"
            : Exit.isFailure(exit) || failed || !finished
              ? "failed"
              : "completed"
          const failure = Exit.isFailure(exit) ? exit.cause.reasons.find(Cause.isFailReason)?.error : undefined
          const kind =
            failure instanceof AIError
              ? classify(failure.reason)
              : errorKind ?? (Exit.isSuccess(exit) && !finished ? "incomplete-stream" : "unknown")
          yield* hooks.notify(
            "session",
            "model.outcome",
            Object.freeze({
              ...input,
              model: Object.freeze({ ...input.model }),
              status,
              durationMs,
              ...(responseModel === undefined ? {} : { responseModel }),
              ...(finishReason === undefined ? {} : { finishReason }),
              ...(status === "failed" ? { errorKind: kind } : {}),
              ...(firstOutputMs === undefined ? {} : { firstOutputMs, firstOutputKind }),
            }),
          )
        }).pipe(
          Effect.catchCause(() => Effect.logWarning("session model outcome observation failed", { kind: input.kind })),
        ),
      ),
    )
  })

function classify(reason: AIError["reason"]): SessionModelOutcome["errorKind"] {
  if (reason._tag === "InvalidRequest" && reason.classification) return reason.classification
  if (reason._tag === "InvalidProviderOutput" && reason.classification) return reason.classification
  return ({
    Authentication: "authentication",
    RateLimit: "rate-limit",
    QuotaExceeded: "quota",
    ContentPolicy: "content-filter",
    InvalidRequest: "invalid-request",
    UnsupportedOperation: "unsupported-operation",
    NoRoute: "no-route",
    ProviderInternal: "provider-internal",
    Transport: "transport",
    Timeout: "timeout",
    InvalidProviderOutput: "invalid-output",
    UnknownProvider: "unknown",
  } as const)[reason._tag]
}
