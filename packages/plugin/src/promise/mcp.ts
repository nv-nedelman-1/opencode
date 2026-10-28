import type { McpApi } from "@opencode/client/promise/api"
import type { Mcp } from "@opencode/schema/mcp"
import type { Session } from "@opencode/schema/session"
import type { Observers, Transform } from "./registration.js"
import type { DeepMutable } from "./types.js"

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
  list(): readonly [string, DeepMutable<Mcp.ServerConfig>][]
  get(name: string): DeepMutable<Mcp.ServerConfig> | undefined
  set(name: string, config: Mcp.ServerConfig): void
  update(name: string, update: (config: DeepMutable<Mcp.ServerConfig>) => void): void
  remove(name: string): void
}

export interface MCPDomain extends Pick<McpApi, "list"> {
  readonly observe: Observers<MCPObservations>
  readonly transform: Transform<MCPEditor>
  readonly reload: () => Promise<void>
}
