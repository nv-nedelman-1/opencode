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
 * intercepts may wrap or replace it. The tool's own exit is re-raised unchanged so declines, defects,
 * and interruption keep their meaning.
 */
export const middleware =
  (
    runtime: RelayHost.Runtime,
    parentID: (sessionID: Session.ID) => Effect.Effect<Session.ID | undefined>,
  ): ToolMiddlewares["execute"] =>
  (execution, next) =>
    Effect.gen(function* () {
      const parent = yield* runtime.scope(execution.sessionID, parentID(execution.sessionID))
      const run = Effect.runPromiseExitWith(yield* Effect.context<never>())
      const inner: { exit?: Exit.Exit<Tool.Result, Tool.Error> } = {}
      const managed = yield* Effect.tryPromise({
        try: (signal) =>
          runtime.relay.toolCallExecuteAsync(
            execution.tool,
            RelayHost.json(execution.input),
            async (args, aborted) => {
              inner.exit = await run(next(args), { signal: AbortSignal.any([signal, aborted]) })
              if (Exit.isSuccess(inner.exit)) return { result: RelayHost.json(inner.exit.value) }
              throw new Error(Cause.pretty(inner.exit.cause))
            },
            parent,
            null,
            null,
            {
              "opencode.session_id": execution.sessionID,
              "opencode.message_id": execution.messageID,
              "opencode.agent": execution.agent,
            },
            execution.id,
          ),
        catch: (error) => error,
      }).pipe(Effect.exit)

      if (inner.exit !== undefined) {
        if (Exit.isFailure(inner.exit)) return yield* Effect.failCause(inner.exit.cause)
        // A failure after the tool ran comes from Relay's response processing; the tool result stands.
        if (Exit.isFailure(managed) || RelayHost.sameJson(managed.value.result, RelayHost.json(inner.exit.value)))
          return inner.exit.value
      }
      if (Exit.isSuccess(managed)) return replaced(managed.value.result)
      const reason = RelayHost.rejection(managed.cause)
      if (reason !== undefined)
        return yield* new Tool.Error({ message: `NeMo Relay blocked this tool call: ${reason}` })
      yield* Effect.logWarning("NeMo Relay could not manage a tool call; running it unmanaged", {
        tool: execution.tool,
        cause: Cause.pretty(managed.cause),
      })
      return yield* next(execution.input)
    })

/** A result an execution intercept substituted for the tool's own. */
const replaced = (value: unknown): Tool.Result =>
  Option.getOrElse(decodeResult(value), () => ({ content: typeof value === "string" ? value : JSON.stringify(value) }))
