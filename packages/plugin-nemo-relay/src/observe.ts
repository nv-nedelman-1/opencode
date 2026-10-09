export * as RelayObserve from "./observe.js"

import type { SessionContext } from "@opencode/plugin/effect/session"
import type { EventManifest } from "@opencode/schema/event-manifest"
import type { Session } from "@opencode/schema/session"
import type { SessionMessage } from "@opencode/schema/session-message"
import { Effect } from "effect"
import type { RelayHost } from "./host.js"

const errors = new Set([
  "aborted",
  "permission.rejected",
  "tool.execution",
  "provider.rate-limit",
  "provider.auth",
  "provider.quota",
  "provider.content-filter",
  "provider.transport",
  "provider.internal",
  "provider.invalid-output",
  "provider.invalid-request",
  "provider.unsupported-operation",
  "provider.no-route",
  "provider.unknown",
  "provider.timeout",
])

interface SessionState {
  started?: number
  compaction?: number
  readonly steps: Map<SessionMessage.ID, number>
}

/** Host accounting complements managed provider telemetry; it is not a second provider-token counter. */
export const make = (runtime: Pick<RelayHost.Runtime, "open" | "mark" | "close">) => {
  const parents = new Map<Session.ID, Session.ID>()
  const sessions = new Map<Session.ID, SessionState>()
  const state = (id: Session.ID) => {
    const existing = sessions.get(id)
    if (existing) return existing
    const created: SessionState = { steps: new Map() }
    sessions.set(id, created)
    return created
  }
  const event = (event: EventManifest.ServerEvent) =>
    Effect.sync(() => {
      const metadata = {
        "opencode.observation.schema_version": "1",
        "opencode.observation.source": "host",
        "opencode.event_id": event.id,
      }
      const mark = (sessionID: Session.ID | undefined, name: string, data: unknown) =>
        runtime.mark(sessionID, name, data, metadata, event.created * 1_000)

      switch (event.type) {
        case "session.created":
        case "session.forked":
          if (event.data.parentID) {
            parents.set(event.data.sessionID, event.data.parentID)
            mark(event.data.sessionID, "opencode.session.child.created", { count: 1 })
          }
          return
        case "session.execution.started":
          state(event.data.sessionID).started = event.created
          runtime.open(event.data.sessionID, parents.get(event.data.sessionID), event.created * 1_000)
          mark(event.data.sessionID, "opencode.agent.run.started", { count: 1 })
          return
        case "session.execution.succeeded":
        case "session.execution.failed":
        case "session.execution.interrupted": {
          const outcome =
            event.type === "session.execution.succeeded"
              ? "success"
              : event.type === "session.execution.failed"
                ? "failed"
                : "cancelled"
          mark(event.data.sessionID, "opencode.agent.run.completed", {
            count: 1,
            outcome,
            ...duration(sessions.get(event.data.sessionID)?.started, event.created),
            ...(event.type === "session.execution.interrupted" ? { reason: event.data.reason } : {}),
            ...(event.type === "session.execution.failed" ? failure(event.data.error) : {}),
          })
          sessions.delete(event.data.sessionID)
          runtime.close(event.data.sessionID, outcome)
          return
        }
        case "session.step.started":
          state(event.data.sessionID).steps.set(event.data.assistantMessageID, event.data.started)
          // This event is published after provider output starts, not at the dispatch boundary.
          mark(event.data.sessionID, "opencode.agent.step.started", { count: 1, started_ms: event.data.started })
          return
        case "session.step.streamed":
          mark(event.data.sessionID, "opencode.agent.step.streamed", {
            ...duration(sessions.get(event.data.sessionID)?.steps.get(event.data.assistantMessageID), event.created),
          })
          return
        case "session.step.ended":
        case "session.step.failed":
          mark(event.data.sessionID, "opencode.agent.step.completed", {
            count: 1,
            outcome: event.type === "session.step.ended" ? "success" : "failed",
            ...duration(sessions.get(event.data.sessionID)?.steps.get(event.data.assistantMessageID), event.created),
            ...(event.data.finish === undefined ? {} : { finish: event.data.finish }),
            accounting_source: "host_step",
            ...(event.data.cost === undefined ? {} : { cost_usd: event.data.cost }),
            ...(event.data.tokens === undefined ? {} : { tokens: event.data.tokens }),
            ...(event.type === "session.step.failed" ? failure(event.data.error) : {}),
          })
          sessions.get(event.data.sessionID)?.steps.delete(event.data.assistantMessageID)
          return
        case "session.retry.scheduled":
          mark(event.data.sessionID, "opencode.llm.host_retry.scheduled", {
            count: 1,
            attempt: event.data.attempt,
            delay_ms: Math.max(0, event.data.at - event.created),
            ...failure(event.data.error),
          })
          return
        case "session.compaction.started":
          state(event.data.sessionID).compaction = event.created
          mark(event.data.sessionID, "opencode.compaction.started", { count: 1, reason: event.data.reason })
          return
        case "session.compaction.ended":
        case "session.compaction.failed":
          mark(event.data.sessionID, "opencode.compaction.completed", {
            count: 1,
            reason: event.data.reason,
            outcome: event.type === "session.compaction.ended" ? "success" : "failed",
            ...duration(sessions.get(event.data.sessionID)?.compaction, event.created),
            accounting_source: "host_compaction",
            ...(event.data.cost === undefined ? {} : { cost_usd: event.data.cost }),
            ...(event.data.tokens === undefined ? {} : { tokens: event.data.tokens }),
            ...(event.type === "session.compaction.failed" ? failure(event.data.error) : {}),
          })
          if (sessions.has(event.data.sessionID)) delete state(event.data.sessionID).compaction
          return
        case "session.instructions.updated":
          mark(event.data.sessionID, "opencode.instructions.updated", {
            count: 1,
            changed_sources: Object.keys(event.data.delta).length,
          })
          return
        case "session.deleted":
          sessions.delete(event.data.sessionID)
          parents.delete(event.data.sessionID)
          runtime.close(event.data.sessionID, "cancelled")
          return
      }
    })

  const context = (event: SessionContext) =>
    Effect.sync(() =>
      runtime.mark(
        event.sessionID,
        "opencode.context.prepared",
        {
          call_role: "primary",
          message_count: event.messages.length,
          part_count: event.messages.reduce((count, message) => count + message.content.length, 0),
          system_part_count: event.system.length,
          system_character_count: event.system.reduce((count, part) => count + part.text.length, 0),
          tool_count: Object.keys(event.tools).length,
        },
        {
          "opencode.observation.schema_version": "1",
          "opencode.observation.source": "host",
        },
      ),
    )

  return { event, context }
}

const duration = (start: number | undefined, end: number) =>
  start === undefined ? {} : { duration_ms: Math.max(0, end - start) }

const failure = (error: { readonly type: string; readonly status?: number }) => ({
  error_type: errors.has(error.type) ? error.type : "unknown",
  ...(error.status === undefined ? {} : { status: error.status }),
})
