export * as RelayTool from "./tool.js"

import type { ToolMiddlewares } from "@opencode/plugin/effect/tool"
import type { Session } from "@opencode/schema/session"
import { Tool } from "@opencode/schema/tool"
import { Cause, Effect, Exit, Option, Schema } from "effect"
import { RelayHost } from "./host.js"

const Result = Schema.Struct({
  output: Schema.optional(Schema.Unknown),
  content: Schema.optional(Schema.Union([Schema.String, Schema.Array(Tool.Content)])),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
})
const decodeResult = Schema.decodeUnknownOption(Result)

/**
 * Runs every local tool call through Relay managed execution: conditional-execution guardrails may
 * block it, request intercepts may rewrite its input (which the tool then validates), and execution
 * intercepts may wrap or replace it. Each host tool runs at most once; declines, defects, and caller
 * interruption keep their meaning even when Relay processing fails.
 */
export const middleware =
  (
    runtime: RelayHost.Runtime,
    parentID: (sessionID: Session.ID) => Effect.Effect<Session.ID | undefined>,
  ): ToolMiddlewares["execute"] =>
  (execution, next) =>
    Effect.gen(function* () {
      const operation = yield* runtime.lease(execution.sessionID, parentID(execution.sessionID))
      const abort = new AbortController()
      operation.onCancel(() => abort.abort())
      const run = Effect.runPromiseExitWith(yield* Effect.context<never>())
      const inner: {
        task?: Promise<Exit.Exit<Tool.Result, Tool.Error>>
        result?: unknown
        aborted?: AbortSignal
      } = {}
      const managed = yield* Effect.tryPromise({
        try: (signal) =>
          operation.run(() =>
            runtime.relay.toolCallExecuteAsync(
              execution.tool,
              RelayHost.json(execution.input),
              async (args, aborted) => {
                if (inner.task) throw new Error("OpenCode tool middleware may call next at most once")
                inner.aborted = aborted
                inner.task = run(
                  Effect.suspend(() => next(args)),
                  { signal: AbortSignal.any([signal, aborted, abort.signal]) },
                )
                const exit = await inner.task
                if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause))
                inner.result = RelayHost.json(exit.value)
                return { result: inner.result }
              },
              operation.parent,
              null,
              null,
              {
                "opencode.session_id": execution.sessionID,
                "opencode.message_id": execution.messageID,
                "opencode.agent": execution.agent,
              },
              execution.id,
            ),
          ),
        catch: (error) => error,
      }).pipe(
        Effect.ensuring(
          Effect.promise(async () => {
            await inner.task
          }),
        ),
        Effect.onExit(() => Effect.sync(operation.release)),
        Effect.exit,
      )

      // A replacing intercept can settle while its already-started continuation is being revoked.
      const task = inner.task
      const exit = task === undefined ? undefined : yield* Effect.promise(() => task)
      if (exit !== undefined) {
        if (
          Exit.isFailure(exit) &&
          (Exit.isFailure(managed) ||
            Cause.hasDies(exit.cause) ||
            (Cause.hasInterrupts(exit.cause) && !inner.aborted?.aborted))
        )
          return yield* Effect.failCause(exit.cause)
        // A failure after the tool ran comes from Relay's response processing; the tool result stands.
        if (Exit.isSuccess(exit) && (Exit.isFailure(managed) || RelayHost.sameJson(managed.value.result, inner.result)))
          return exit.value
      }
      if (Exit.isSuccess(managed)) return replaced(managed.value.result)
      if (Cause.hasInterrupts(managed.cause)) return yield* Effect.failCause(managed.cause).pipe(Effect.orDie)
      const reason = RelayHost.rejection(managed.cause)
      if (reason !== undefined)
        return yield* new Tool.Error({ message: `NeMo Relay blocked this tool call: ${reason}` })
      return yield* new Tool.Error({
        message: "NeMo Relay could not manage this tool call",
        metadata: { "nemo_relay.failure_stage": "before_dispatch" },
      })
    })

/** A result an execution intercept substituted for the tool's own. */
const replaced = (value: unknown): Tool.Result =>
  Option.getOrElse(decodeResult(value), () => ({ content: typeof value === "string" ? value : JSON.stringify(value) }))
