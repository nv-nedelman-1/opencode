import { Tool } from "@opencode/schema/tool"
import type { Agent } from "@opencode/schema/agent"
import type { Session } from "@opencode/schema/session"
import type { SessionMessage } from "@opencode/schema/session-message"
import type { Effect, Types } from "effect"
import type { Hooks, Middleware, Transform } from "./registration.js"

export interface ToolEditor {
  list(): readonly (Tool.Info & { readonly id: string })[]
  get(id: string): (Tool.Info & { readonly id: string }) | undefined
  namespace(namespace: Tool.Namespace): void
  add<Input extends Tool.ValueSchema<any>, Output extends Tool.ValueSchema<any> | undefined>(
    tool: Tool.Info<Input, Output>,
  ): void
  /** Updates an existing tool; missing IDs are ignored. */
  update(id: string, update: (tool: Types.Mutable<Tool.Info>) => void): void
  remove(id: string): void
}

export interface ToolHooks {
  readonly "execute.before": {
    tool: string
    readonly sessionID: Session.ID
    readonly agent: Agent.ID
    readonly messageID: SessionMessage.ID
    readonly id: Tool.CallID
    input: unknown
  }
  readonly "execute.after": {
    readonly tool: string
    readonly sessionID: Session.ID
    readonly agent: Agent.ID
    readonly messageID: SessionMessage.ID
    readonly id: Tool.CallID
    readonly input: unknown
  } & (
    | {
        readonly status: "completed"
        result: Tool.Result
      }
    | {
        readonly status: "error"
        error: Tool.Error
      }
  )
}

// Only execute.before may fail: a Tool.Error rejects the call before the tool runs.
export interface ToolFailures extends Record<keyof ToolHooks, unknown> {
  readonly "execute.before": Tool.Error
  readonly "execute.after": never
}

/** One local tool call, after `execute.before` hooks have run. */
export interface ToolExecution {
  readonly tool: string
  readonly sessionID: Session.ID
  readonly agent: Agent.ID
  readonly messageID: SessionMessage.ID
  readonly id: Tool.CallID
  readonly input: unknown
}

export interface ToolMiddlewares {
  /**
   * Wraps every local tool execution, including CodeMode calls, regardless of when the tool was
   * registered. `next` validates the given input and runs the tool; call it at most once. Failures
   * other than `Tool.Error`, such as permission declines and interruption, must propagate unchanged.
   */
  readonly execute: (
    call: ToolExecution,
    next: (input: unknown) => Effect.Effect<Tool.Result, Tool.Error>,
  ) => Effect.Effect<Tool.Result, Tool.Error>
}

export interface ToolDomain {
  readonly transform: Transform<ToolEditor>
  readonly reload: () => Effect.Effect<void>
  /** Currently registered tools, after every transform, keyed by effective name. */
  readonly list: () => Effect.Effect<readonly (Tool.Info & { readonly id: string })[]>
  readonly hook: Hooks<ToolHooks, ToolFailures>
  readonly middleware: Middleware<ToolMiddlewares>
}
