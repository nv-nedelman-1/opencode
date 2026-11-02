export * as RelayObserve from "./observe.js"

import type { SessionContext, SessionHooks, SessionRequest, SessionRequestKind } from "@opencode/plugin/effect/session"
import type { PermissionHooks } from "@opencode/plugin/effect/permission"
import type { EventManifest } from "@opencode/schema/event-manifest"
import { Session } from "@opencode/schema/session"
import type { SessionMessage } from "@opencode/schema/session-message"
import { Effect, Option, Schema } from "effect"
import type { RelayHost } from "./host.js"
import { RelayMetrics } from "./metrics.js"

const errors = new Set([
  "aborted",
  "permission.rejected",
  "tool.execution",
  "tool.unknown",
  "compaction.interrupted",
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
  readonly steps: Map<
    SessionMessage.ID,
    { readonly started: number; readonly model: SessionContext["model"]; streamed?: number; firstOutput?: true }
  >
  readonly permissions: Map<string, { readonly started: number; readonly family: string }>
  readonly tools: Map<string, { readonly category: string; started?: number; blocked?: true }>
  readonly counts: {
    steps: number
    tools: number
    retries: number
    permissions: number
    forms: number
    compactions: number
  }
}

/** Host billing and native provider accounting are separate, non-additive planes. */
export const make = (runtime: Pick<RelayHost.Runtime, "admit" | "open" | "mark" | "close">) => {
  const parents = new Map<Session.ID, Session.ID>()
  const sessions = new Map<Session.ID, SessionState>()
  const forms = new Map<string, { readonly sessionID?: Session.ID; readonly started: number }>()
  const state = (id: Session.ID) => {
    const existing = sessions.get(id)
    if (existing) return existing
    const created: SessionState = {
      steps: new Map(),
      permissions: new Map(),
      tools: new Map(),
      counts: { steps: 0, tools: 0, retries: 0, permissions: 0, forms: 0, compactions: 0 },
    }
    sessions.set(id, created)
    return created
  }
  const closeWaits = (
    sessionID: Session.ID,
    created: number,
    eventID: string,
    source: "session_terminal" | "session_deleted",
  ) => {
    sessions.get(sessionID)?.permissions.forEach((pending, id) =>
      runtime.mark(
        sessionID,
        "opencode.permission.wait.completed",
        {
          count: 1,
          family: pending.family,
          resolution: "cancelled",
          cancellation_source: source,
          ...duration(pending.started, created),
        },
        { ...hostMetadata, "opencode.event_id": `${eventID}:permission:${id}` },
        created * 1_000,
      ),
    )
    sessions.get(sessionID)?.permissions.clear()
    forms.forEach((pending, id) => {
      if (pending.sessionID !== sessionID) return
      runtime.mark(
        sessionID,
        "opencode.form.wait.completed",
        {
          count: 1,
          resolution: "cancelled",
          cancellation_source: source,
          ...duration(pending.started, created),
        },
        { ...hostMetadata, "opencode.event_id": `${eventID}:form:${id}` },
        created * 1_000,
      )
      forms.delete(id)
    })
  }
  const event = (event: EventManifest.ServerEvent) =>
    Effect.sync(() => {
      if (
        event.type === "session.text.delta" ||
        event.type === "session.text.ended" ||
        event.type === "session.reasoning.delta" ||
        event.type === "session.reasoning.ended" ||
        event.type === "session.tool.input.ended" ||
        event.type === "session.tool.input.delta"
      ) {
        const step = sessions.get(event.data.sessionID)?.steps.get(event.data.assistantMessageID)
        const content =
          event.type === "session.text.ended" ||
          event.type === "session.reasoning.ended" ||
          event.type === "session.tool.input.ended"
            ? event.data.text
            : event.data.delta
        if (content.length === 0 || !step || step.firstOutput) return
        step.firstOutput = true
        runtime.mark(
          event.data.sessionID,
          "opencode.llm.logical.first_output",
          {
            count: 1,
            call_role: "primary",
            output_kind:
              event.type === "session.text.delta" || event.type === "session.text.ended"
                ? "text"
                : event.type === "session.reasoning.delta" || event.type === "session.reasoning.ended"
                  ? "reasoning"
                  : "tool",
            ...duration(step.started, event.created),
            ...RelayMetrics.route(step.model),
          },
          hostMetadata,
          event.created * 1_000,
        )
        return
      }
      if (event.type === "session.tool.progress" || event.type === "session.compaction.delta") return
      if (!runtime.admit(event.id)) return
      const metadata = {
        ...hostMetadata,
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
          const current = sessions.get(event.data.sessionID)
          closeWaits(event.data.sessionID, event.created, event.id, "session_terminal")
          mark(event.data.sessionID, "opencode.agent.run.completed", {
            count: 1,
            outcome,
            ...duration(current?.started, event.created),
            ...(current === undefined
              ? {}
              : {
                  step_count: current.counts.steps,
                  logical_llm_count: current.counts.steps,
                  tool_call_count: current.counts.tools,
                  retry_count: current.counts.retries,
                  permission_wait_count: current.counts.permissions,
                  form_wait_count: current.counts.forms,
                  compaction_count: current.counts.compactions,
                }),
            ...(event.type === "session.execution.interrupted" ? { reason: event.data.reason } : {}),
            ...(event.type === "session.execution.failed" ? failure(event.data.error) : {}),
          })
          sessions.delete(event.data.sessionID)
          runtime.close(event.data.sessionID, outcome)
          return
        }
        case "session.step.started": {
          const current = state(event.data.sessionID)
          if (current.steps.has(event.data.assistantMessageID)) return
          current.steps.set(event.data.assistantMessageID, { started: event.data.started, model: event.data.model })
          current.counts.steps++
          // This event is published after provider output starts, not at the dispatch boundary.
          mark(event.data.sessionID, "opencode.agent.step.started", { count: 1, started_ms: event.data.started })
          return
        }
        case "session.step.streamed": {
          const step = sessions.get(event.data.sessionID)?.steps.get(event.data.assistantMessageID)
          if (step) step.streamed = event.created
          mark(event.data.sessionID, "opencode.agent.step.streamed", {
            ...duration(step?.started, event.created),
          })
          return
        }
        case "session.step.ended":
        case "session.step.failed": {
          const current = sessions.get(event.data.sessionID)
          const step = current?.steps.get(event.data.assistantMessageID)
          const outcome =
            event.type === "session.step.ended"
              ? "success"
              : event.data.error.type === "aborted"
                ? "cancelled"
                : "failed"
          mark(event.data.sessionID, "opencode.agent.step.completed", {
            count: 1,
            outcome:
              event.type === "session.step.ended"
                ? "success"
                : event.data.error.type === "aborted"
                  ? "cancelled"
                  : "failed",
            ...duration(step?.started, event.created),
            ...(event.data.finish === undefined ? {} : { finish: event.data.finish }),
            accounting_source: "host_step",
            ...(event.data.cost === undefined ? {} : { cost_usd: event.data.cost }),
            ...(event.data.tokens === undefined ? {} : { tokens: event.data.tokens }),
            ...(event.type === "session.step.failed" ? failure(event.data.error) : {}),
          })
          mark(event.data.sessionID, "opencode.llm.logical.completed", {
            count: 1,
            call_role: "primary",
            outcome,
            duration_boundary: step?.streamed === undefined ? "step_terminal" : "streamed",
            ...duration(step?.started, step?.streamed ?? event.created),
            ...(step === undefined ? {} : RelayMetrics.route(step.model)),
            ...(event.data.finish === undefined ? {} : { finish: event.data.finish }),
            ...(event.type === "session.step.failed" ? failure(event.data.error) : {}),
          })
          if (event.data.tokens !== undefined && event.data.cost !== undefined)
            mark(event.data.sessionID, "opencode.llm.usage.recorded", {
              call_role: "primary",
              accounting_source: "host_step",
              cost_source: "host_calculated",
              tokens: event.data.tokens,
              cost_usd: event.data.cost,
            })
          current?.steps.delete(event.data.assistantMessageID)
          return
        }
        case "session.retry.scheduled":
          state(event.data.sessionID).counts.retries++
          mark(event.data.sessionID, "opencode.llm.host_retry.scheduled", {
            count: 1,
            attempt: event.data.attempt,
            remaining_delay_ms: Math.max(0, event.data.at - event.created),
            ...failure(event.data.error),
          })
          return
        case "session.compaction.started":
          state(event.data.sessionID).compaction = event.created
          state(event.data.sessionID).counts.compactions++
          mark(event.data.sessionID, "opencode.compaction.started", { count: 1, reason: event.data.reason })
          return
        case "session.compaction.ended":
        case "session.compaction.failed":
          mark(event.data.sessionID, "opencode.compaction.completed", {
            count: 1,
            reason: event.data.reason,
            outcome:
              event.type === "session.compaction.ended"
                ? "success"
                : event.data.error.type === "aborted" || event.data.error.type === "compaction.interrupted"
                  ? "cancelled"
                  : "failed",
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
        case "session.inbox.enqueued":
          if (event.data.item.type === "user")
            mark(event.data.sessionID, "opencode.prompt.admitted", {
              count: 1,
              delivery: event.data.item.delivery,
              file_count: event.data.item.payload.files?.length ?? 0,
              agent_count: event.data.item.payload.agents?.length ?? 0,
              skill_count: event.data.item.payload.skills?.length ?? 0,
            })
          return
        case "session.skill.activated":
          mark(event.data.sessionID, "opencode.skill.activated", {
            count: 1,
            source: "user",
            character_count: event.data.text.length,
          })
          return
        case "session.tool.input.started":
          state(event.data.sessionID).tools.set(`${event.data.assistantMessageID}:${event.data.id}`, {
            category: toolCategory(event.data.name),
          })
          if (event.data.name === "skill") {
            mark(event.data.sessionID, "opencode.skill.tool.requested", { count: 1 })
          }
          return
        case "session.tool.called": {
          const current = state(event.data.sessionID)
          const tool = current.tools.get(`${event.data.assistantMessageID}:${event.data.id}`)
          if (tool?.started !== undefined) return
          if (tool) tool.started = event.created
          current.counts.tools++
          const step = current.steps.get(event.data.assistantMessageID)
          if (step && !step.firstOutput) {
            step.firstOutput = true
            mark(event.data.sessionID, "opencode.llm.logical.first_output", {
              count: 1,
              call_role: "primary",
              output_kind: "tool",
              ...duration(step.started, event.created),
              ...RelayMetrics.route(step.model),
            })
          }
          return
        }
        case "session.tool.success":
        case "session.tool.failed": {
          const key = `${event.data.assistantMessageID}:${event.data.id}`
          const tools = sessions.get(event.data.sessionID)?.tools
          const tool = tools?.get(key)
          const category = tool?.category ?? "unknown"
          const data = {
            count: 1,
            category,
            outcome:
              event.type === "session.tool.success"
                ? "success"
                : event.data.error.type === "aborted"
                  ? "cancelled"
                  : "failed",
            provider_executed: event.data.executed,
            execution: event.data.executed ? "provider" : "unknown",
            ...duration(tool?.started, event.created),
            result_family:
              tool?.blocked || (event.type === "session.tool.failed" && event.data.error.type === "permission.rejected")
                ? "blocked"
                : event.type === "session.tool.failed"
                  ? event.data.error.type === "aborted"
                    ? "cancelled"
                    : "error"
                  : category === "terminal"
                    ? terminalFamily(event.data.metadata)
                    : "success",
            ...(event.type === "session.tool.failed" ? failure(event.data.error) : {}),
          }
          mark(event.data.sessionID, "opencode.tool.completed", data)
          if (category === "skill") mark(event.data.sessionID, "opencode.skill.tool.completed", data)
          tools?.delete(key)
          return
        }
        case "permission.asked": {
          const family = permissionFamily(event.data.action)
          const current = state(event.data.sessionID)
          if (current.permissions.has(event.data.id)) return
          current.permissions.set(event.data.id, { started: event.created, family })
          current.counts.permissions++
          mark(event.data.sessionID, "opencode.permission.wait.started", { count: 1, family })
          return
        }
        case "permission.replied": {
          const pending = sessions.get(event.data.sessionID)?.permissions.get(event.data.requestID)
          if (!pending) return
          mark(event.data.sessionID, "opencode.permission.wait.completed", {
            count: 1,
            resolution: event.data.reply,
            family: pending.family,
            ...duration(pending.started, event.created),
          })
          sessions.get(event.data.sessionID)?.permissions.delete(event.data.requestID)
          return
        }
        case "form.created": {
          const sessionID = Option.getOrUndefined(Schema.decodeUnknownOption(Session.ID)(event.data.form.sessionID))
          if (forms.has(event.data.form.id)) return
          forms.set(event.data.form.id, { sessionID, started: event.created })
          if (sessionID) state(sessionID).counts.forms++
          mark(sessionID, "opencode.form.wait.started", { count: 1, field_count: event.data.form.fields.length })
          return
        }
        case "form.replied":
        case "form.cancelled": {
          const pending = forms.get(event.data.id)
          if (!pending) return
          mark(pending.sessionID, "opencode.form.wait.completed", {
            count: 1,
            resolution: event.type === "form.replied" ? "answered" : "cancelled",
            ...duration(pending?.started, event.created),
          })
          forms.delete(event.data.id)
          return
        }
        case "skill.updated":
          mark(undefined, "opencode.skill.catalog.updated", { count: 1 })
          return
        case "mcp.status.changed":
        case "mcp.resources.changed":
          mark(undefined, `opencode.${event.type}`, { count: 1 })
          return
        case "session.deleted":
          closeWaits(event.data.sessionID, event.created, event.id, "session_deleted")
          sessions.delete(event.data.sessionID)
          parents.delete(event.data.sessionID)
          runtime.close(event.data.sessionID, "cancelled")
          return
      }
    })

  const context = (
    event: SessionRequest & { readonly tools?: SessionContext["tools"] },
    kind: SessionRequestKind = "primary",
  ) =>
    Effect.sync(() =>
      runtime.mark(
        event.sessionID,
        "opencode.context.prepared",
        {
          call_role: kind,
          message_count: event.messages.length,
          part_count: event.messages.reduce((count, message) => count + message.content.length, 0),
          system_part_count: event.system.length,
          system_character_count: event.system.reduce((count, part) => count + part.text.length, 0),
          ...(event.tools === undefined ? {} : { tool_count: Object.keys(event.tools).length }),
        },
        hostMetadata,
      ),
    )

  const contextUsage = (event: SessionHooks["context.usage"]) =>
    Effect.sync(() =>
      runtime.mark(
        event.sessionID,
        "opencode.context.usage",
        {
          call_role: event.kind,
          measured_tokens: event.measured,
          estimated_tokens: event.estimated,
          ...(event.limit === undefined
            ? {}
            : {
                context_limit: event.limit,
                ...(event.limit > 0 ? { context_utilization: (event.measured + event.estimated) / event.limit } : {}),
              }),
          ...RelayMetrics.route(event.model),
        },
        hostMetadata,
      ),
    )
  const retryDecision = (event: SessionHooks["retry.decision"]) =>
    Effect.sync(() =>
      runtime.mark(
        event.sessionID,
        "opencode.llm.retry.decision",
        {
          count: 1,
          call_role: event.kind,
          attempt: event.attempt,
          will_retry: event.decision.retry,
          retryable: event.retryable,
          ...(event.decision.retry ? { selected_delay_ms: event.decision.delay } : {}),
          delay_source: event.source,
          decision_reason: event.reason,
          ...failure(event.error),
          ...RelayMetrics.route(event.model),
        },
        hostMetadata,
      ),
    )
  const compactionOutcome = (event: SessionHooks["compaction.outcome"]) =>
    Effect.sync(() =>
      runtime.mark(
        event.sessionID,
        "opencode.compaction.outcome",
        {
          count: 1,
          trigger: event.trigger === "auto" ? "proactive" : event.trigger,
          compaction_status: event.status === "interrupted" ? "cancelled" : event.status,
          estimate_source: "host_context",
          ...(event.before === undefined ? {} : { before_tokens: event.before }),
          ...(event.after === undefined ? {} : { after_tokens: event.after }),
          ...(event.limit === undefined ? {} : { context_limit: event.limit }),
          ...(event.sourceEstimatedTokens === undefined
            ? {}
            : { source_estimated_tokens: event.sourceEstimatedTokens }),
          ...(event.summaryEstimatedTokens === undefined
            ? {}
            : { summary_estimated_tokens: event.summaryEstimatedTokens }),
          ...(event.retainedEstimatedTokens === undefined
            ? {}
            : { retained_estimated_tokens: event.retainedEstimatedTokens }),
          ...(event.before === undefined || event.before <= 0 || event.after === undefined
            ? {}
            : { retained_ratio: event.after / event.before }),
          ...RelayMetrics.route(event.model),
        },
        hostMetadata,
      ),
    )
  const permissionDecision = (event: PermissionHooks["decision"]) =>
    Effect.sync(() => {
      if (event.effect === "deny" && event.source?.type === "tool") {
        const tool = sessions.get(event.sessionID)?.tools.get(`${event.source.messageID}:${event.source.id}`)
        if (tool) tool.blocked = true
      }
      runtime.mark(
        event.sessionID,
        "opencode.permission.evaluated",
        {
          count: 1,
          family: permissionFamily(event.action),
          effect: event.effect,
          origin: event.origin,
        },
        hostMetadata,
      )
    })
  const usage = (event: SessionHooks["usage"]) =>
    Effect.sync(() =>
      runtime.mark(
        event.sessionID,
        "opencode.llm.usage.recorded",
        {
          call_role: event.source,
          accounting_source: "host_usage",
          cost_source: event.costSource,
          cost_usd: event.cost,
          tokens: event.tokens,
        },
        hostMetadata,
      ),
    )

  return { event, context, contextUsage, retryDecision, compactionOutcome, permissionDecision, usage }
}

const hostMetadata = { "opencode.observation.schema_version": "1", "opencode.observation.source": "host" }

const terminalMetadata = Schema.decodeUnknownOption(
  Schema.Struct({
    exit: Schema.optional(Schema.Finite),
    signal: Schema.optional(Schema.String),
    timeout: Schema.optional(Schema.Boolean),
    status: Schema.optional(Schema.Literals(["completed", "running"])),
  }),
)
const terminalFamily = (metadata: unknown) => {
  const result = Option.getOrUndefined(terminalMetadata(metadata))
  if (result?.timeout) return "timeout"
  if (result?.signal !== undefined) return "signal"
  if (result?.status === "running") return "background"
  if (result?.exit !== undefined) return result.exit === 0 ? "zero_exit" : "nonzero_exit"
  return "unknown"
}

const duration = (start: number | undefined, end: number) =>
  start === undefined ? {} : { duration_ms: Math.max(0, end - start) }

const failure = (error: { readonly type: string; readonly status?: number }) => ({
  error_type: errors.has(error.type) ? error.type : "unknown",
  ...(error.status === undefined ? {} : { status: error.status }),
})

const permissionFamily = (action: string) => {
  if (["read", "list", "glob", "grep"].includes(action)) return "file_read"
  if (["edit", "write", "apply_patch"].includes(action)) return "file_write"
  if (["bash", "shell", "terminal"].includes(action)) return "terminal"
  if (action === "skill") return "skill"
  if (["task", "subagent"].includes(action)) return "delegation"
  return "other"
}

const toolCategory = (name: string) => {
  if (["read", "list", "ls"].includes(name)) return "file_read"
  if (["write", "edit", "apply_patch", "patch"].includes(name)) return "file_write"
  if (["bash", "shell"].includes(name)) return "terminal"
  if (["grep", "glob", "lsp", "code_search"].includes(name)) return "code_search"
  if (["webfetch", "websearch"].includes(name)) return "web"
  if (["todo", "todowrite", "plan", "plan_exit"].includes(name)) return "planning"
  if (["task", "subagent"].includes(name)) return "delegation"
  if (name === "question") return "human_input"
  if (name === "skill") return "skill"
  if (name === "execute") return "code_execution"
  if (["list_mcp_resources", "read_mcp_resource"].includes(name)) return "mcp_resource"
  return "extension"
}
