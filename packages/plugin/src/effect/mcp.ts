import type { McpApi } from "@opencode/client/effect/api"
import type { Mcp } from "@opencode/schema/mcp"
import type { Session } from "@opencode/schema/session"
import type { Effect, Types } from "effect"
import type { Observers, Transform } from "./registration.js"

/** Host recovery of an expired MCP session, not an SDK-internal retry or tool business outcome. */
export interface MCPRecovery {
  readonly operation: "tool" | "resource" | "prompt" | "catalog" | "background"
  readonly sessionID?: Session.ID
  readonly reason: "session_expired"
  readonly phase: "reconnect" | "retry"
  readonly outcome: "started" | "succeeded" | "failed" | "interrupted"
  readonly durationMs?: number
}

export interface MCPObservations {
  readonly recovery: MCPRecovery
}

export interface MCPEditor {
  list(): readonly [string, Types.DeepMutable<Mcp.ServerConfig>][]
  get(name: string): Types.DeepMutable<Mcp.ServerConfig> | undefined
  set(name: string, config: Mcp.ServerConfig): void
  update(name: string, update: (config: Types.DeepMutable<Mcp.ServerConfig>) => void): void
  remove(name: string): void
}

export interface MCPDomain extends Pick<McpApi<unknown>, "list"> {
  readonly observe: Observers<MCPObservations>
  readonly transform: Transform<MCPEditor>
  readonly reload: () => Effect.Effect<void>
}
