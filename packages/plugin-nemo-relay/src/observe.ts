export * as RelayObserve from "./observe.js"

import type {
  SessionContext,
  SessionHooks,
  SessionModelOutcome,
  SessionRequest,
  SessionRequestKind,
} from "@opencode/plugin/effect/session"
import type { PermissionHooks } from "@opencode/plugin/effect/permission"
import type { MCPRecovery } from "@opencode/plugin/effect/mcp"
import type { SkillCatalog, SkillLoad } from "@opencode/plugin/effect/skill"
import type { ToolExecution, ToolProvenance } from "@opencode/plugin/effect/tool"
import type { EventManifest } from "@opencode/schema/event-manifest"
import { Session } from "@opencode/schema/session"
import type { SessionMessage } from "@opencode/schema/session-message"
import { Effect, Option, Schema } from "effect"
import { createHash } from "node:crypto"
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

interface ToolState {
  provenance?: ToolProvenance
  started?: number
  /** Gate history, not the cause of a potentially recovered terminal result. */
  approval?: "automatic" | "asked" | "allowed_after_ask" | "rejected" | "denied"
}

interface SessionState {
  started?: number
  compaction?: number
  readonly steps: Map<
    SessionMessage.ID,
    { readonly started: number; readonly model: SessionContext["model"]; streamed?: number; firstOutput?: true }
  >
  readonly permissions: Map<string, { readonly started: number; readonly family: string; readonly toolKey?: string }>
  readonly counts: {
    steps: number
    tools: number
    retries: number
    permissions: number
    forms: number
    compactions: number
  }
}

const observers = new WeakMap<Parameters<typeof create>[0], ReturnType<typeof create>>()

/** All Location subscribers share the state belonging to their process runtime. */
export const make = (runtime: Parameters<typeof create>[0]) => {
  const existing = observers.get(runtime)
  if (existing) return existing
  const observer = create(runtime)
  observers.set(runtime, observer)
  return observer
}

/** Host billing and native provider accounting are separate, non-additive planes. */
const create = (runtime: Pick<RelayHost.Runtime, "admit" | "open" | "mark" | "close">) => {
  const parents = new Map<Session.ID, Session.ID>()
  const sessions = new Map<Session.ID, SessionState>()
  // Source callbacks can lead the bus across run boundaries; calls settle by message/call ID, not run end.
  const calls = new Map<Session.ID, Map<string, ToolState>>()
  const toolState = (id: Session.ID) => {
    const existing = calls.get(id)
    if (existing) return existing
    const created = new Map<string, ToolState>()
    calls.set(id, created)
    return created
  }
  const forms = new Map<string, { readonly sessionID?: Session.ID; readonly started: number }>()
  // Bounded, process-local comparison only; neither skill identities nor content hashes are exported.
  const skillLoads = new Map<Session.ID, Map<string, string>>()
  const state = (id: Session.ID) => {
    const existing = sessions.get(id)
    if (existing) return existing
    const created: SessionState = {
      steps: new Map(),
      permissions: new Map(),
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
        case "session.tool.input.started": {
          const tools = toolState(event.data.sessionID)
          const key = `${event.data.assistantMessageID}:${event.data.id}`
          if (!tools.has(key)) tools.set(key, {})
          return
        }
        case "session.tool.called": {
          const current = state(event.data.sessionID)
          const tool = calls.get(event.data.sessionID)?.get(`${event.data.assistantMessageID}:${event.data.id}`)
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
          const tools = calls.get(event.data.sessionID)
          const tool = tools?.get(key)
          const category = tool?.provenance?.category ?? "unknown"
          const metadata = event.type === "session.tool.success" ? event.data.metadata : undefined
          const delegation =
            category === "delegation" && tool?.provenance?.origin === "builtin"
              ? Option.getOrUndefined(delegationMetadata(metadata?.delegation))
              : undefined
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
            tool_origin: tool?.provenance?.origin ?? "unknown",
            execution: event.data.executed ? "provider" : (tool?.provenance?.execution ?? "unknown"),
            approval: tool?.approval ?? "unknown",
            ...duration(tool?.started, event.created),
            result_family:
              event.type === "session.tool.failed" && event.data.error.type === "permission.rejected"
                ? "blocked"
                : event.type === "session.tool.failed"
                  ? event.data.error.type === "aborted"
                    ? "cancelled"
                    : "error"
                  : category === "terminal"
                    ? terminalFamily(metadata)
                    : "success",
            ...(typeof metadata?.truncated !== "boolean" ? {} : { output_truncated: metadata.truncated }),
            ...(delegation === undefined
              ? {}
              : {
                  delegation_kind: delegation.kind,
                  requested_mode: delegation.requestedMode,
                  return_mode: delegation.returnMode,
                }),
            ...(event.type === "session.tool.failed" ? failure(event.data.error) : {}),
          }
          mark(event.data.sessionID, "opencode.tool.completed", data)
          if (category === "skill") mark(event.data.sessionID, "opencode.skill.tool.completed", data)
          tools?.delete(key)
          if (tools?.size === 0) calls.delete(event.data.sessionID)
          return
        }
        case "permission.asked": {
          const family = permissionFamily(event.data.action)
          const current = state(event.data.sessionID)
          if (current.permissions.has(event.data.id)) return
          const toolKey =
            event.data.source?.type === "tool" ? `${event.data.source.messageID}:${event.data.source.id}` : undefined
          const tool = toolKey === undefined ? undefined : calls.get(event.data.sessionID)?.get(toolKey)
          if (tool && tool.approval !== "denied" && tool.approval !== "rejected") tool.approval = "asked"
          current.permissions.set(event.data.id, { started: event.created, family, toolKey })
          current.counts.permissions++
          mark(event.data.sessionID, "opencode.permission.wait.started", { count: 1, family })
          return
        }
        case "permission.replied": {
          const pending = sessions.get(event.data.sessionID)?.permissions.get(event.data.requestID)
          if (!pending) return
          const tool = pending.toolKey === undefined ? undefined : calls.get(event.data.sessionID)?.get(pending.toolKey)
          if (tool && tool.approval !== "denied" && tool.approval !== "rejected")
            tool.approval = event.data.reply === "reject" ? "rejected" : "allowed_after_ask"
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
          skillLoads.delete(event.data.sessionID)
          calls.delete(event.data.sessionID)
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
        kind === "primary" ? event.sessionID : undefined,
        "opencode.context.prepared",
        {
          call_role: kind,
          message_count: event.messages.length,
          part_count: event.messages.reduce((count, message) => count + message.content.length, 0),
          system_part_count: event.system.length,
          system_character_count: event.system.reduce((count, part) => count + part.text.length, 0),
          ...(event.tools === undefined ? {} : { tool_count: Object.keys(event.tools).length }),
        },
        kind === "primary" ? hostMetadata : auxiliaryMetadata,
      ),
    )

  const contextUsage = (event: SessionHooks["context.usage"]) =>
    Effect.sync(() =>
      runtime.mark(
        event.kind === "primary" ? event.sessionID : undefined,
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
        event.kind === "primary" ? hostMetadata : auxiliaryMetadata,
      ),
    )
  const retryDecision = (event: SessionHooks["retry.decision"]) =>
    Effect.sync(() =>
      runtime.mark(
        event.kind === "primary" ? event.sessionID : undefined,
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
        event.kind === "primary" ? hostMetadata : auxiliaryMetadata,
      ),
    )
  const compactionOutcome = (event: SessionHooks["compaction.outcome"]) =>
    Effect.sync(() =>
      runtime.mark(
        undefined,
        "opencode.compaction.outcome",
        {
          count: 1,
          trigger: event.trigger === "auto" ? "proactive" : event.trigger,
          compaction_status: event.status === "interrupted" ? "cancelled" : event.status,
          ...(event.durationMs === undefined ? {} : { duration_ms: event.durationMs }),
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
        auxiliaryMetadata,
      ),
    )
  const permissionDecision = (event: PermissionHooks["decision"]) =>
    Effect.sync(() => {
      const tool =
        event.source?.type === "tool"
          ? calls.get(event.sessionID)?.get(`${event.source.messageID}:${event.source.id}`)
          : undefined
      if (tool) {
        if (event.effect === "deny") tool.approval = "denied"
        else if (event.effect === "ask" && tool.approval !== "denied" && tool.approval !== "rejected")
          tool.approval = "asked"
        else if (event.effect === "allow" && tool.approval === undefined) tool.approval = "automatic"
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
        undefined,
        "opencode.llm.usage.recorded",
        {
          call_role: event.source,
          accounting_source: "host_usage",
          cost_source: event.costSource,
          cost_usd: event.cost,
          tokens: event.tokens,
        },
        auxiliaryMetadata,
      ),
    )

  const toolExecution = (call: ToolExecution) =>
    Effect.sync(() => {
      const tools = toolState(call.sessionID)
      const key = `${call.messageID}:${call.id}`
      const tool = tools.get(key) ?? {}
      tools.set(key, tool)
      // CodeMode's nested tools share the outer call IDs; its first middleware entry owns the host summary.
      if (tool.provenance || !call.provenance) return
      tool.provenance = call.provenance
      if (call.provenance.category === "skill")
        runtime.mark(call.sessionID, "opencode.skill.tool.requested", { count: 1 }, hostMetadata)
    })
  const modelOutcome = (event: SessionModelOutcome) =>
    Effect.sync(() =>
      runtime.mark(
        event.kind === "primary" ? event.sessionID : undefined,
        "opencode.llm.host_call.completed",
        {
          count: 1,
          call_role: event.kind,
          outcome: event.status === "completed" ? "success" : event.status === "interrupted" ? "cancelled" : "failed",
          duration_ms: event.durationMs,
          ...(event.finishReason === undefined ? {} : { finish: event.finishReason }),
          ...(event.errorKind === undefined ? {} : { error_kind: event.errorKind }),
          ...(event.firstOutputMs === undefined
            ? {}
            : { first_output_ms: event.firstOutputMs, output_kind: event.firstOutputKind }),
          ...RelayMetrics.route(event.model),
          ...(event.responseModel === undefined
            ? {}
            : { response_model_family: RelayMetrics.modelFamily(event.responseModel) }),
        },
        event.kind === "primary" ? hostMetadata : auxiliaryMetadata,
      ),
    )
  const skillLoad = (event: SkillLoad) =>
    Effect.sync(() => {
      const comparison: { repeat_load?: true; content_changed?: boolean } = {}
      if (event.status === "completed" && event.skill) {
        const loads = skillLoads.get(event.sessionID) ?? new Map<string, string>()
        skillLoads.delete(event.sessionID)
        skillLoads.set(event.sessionID, loads)
        const fingerprint = createHash("sha256").update(event.skill.content).digest("hex")
        const previous = loads.get(event.skill.id)
        if (previous !== undefined)
          Object.assign(comparison, { repeat_load: true, content_changed: previous !== fingerprint })
        loads.delete(event.skill.id)
        loads.set(event.skill.id, fingerprint)
        if (loads.size > 64) loads.delete(loads.keys().next().value!)
        if (skillLoads.size > 256) skillLoads.delete(skillLoads.keys().next().value!)
      }
      runtime.mark(
        event.sessionID,
        "opencode.skill.load.completed",
        {
          count: 1,
          load_channel: event.channel,
          load_status: event.status,
          source_kind: event.sourceKind,
          comparison_window: "session_memory",
          ...comparison,
        },
        hostMetadata,
      )
    })

  const skillCatalog = (event: SkillCatalog) =>
    Effect.sync(() =>
      event.changes.forEach((change) =>
        runtime.mark(
          undefined,
          "opencode.skill.catalog.changed",
          {
            count: 1,
            catalog_action: change.action,
            source_kind: change.sourceKind,
            ...(change.previousSourceKind === undefined ? {} : { previous_source_kind: change.previousSourceKind }),
            content_changed: change.contentChanged,
          },
          hostMetadata,
        ),
      ),
    )

  const mcpRecovery = (event: MCPRecovery) =>
    Effect.sync(() =>
      runtime.mark(
        undefined,
        event.outcome === "started" ? "opencode.mcp.recovery.started" : "opencode.mcp.recovery.completed",
        {
          count: 1,
          mcp_operation: event.operation,
          recovery_phase: event.phase,
          recovery_reason: event.reason,
          ...(event.outcome === "started"
            ? {}
            : {
                outcome:
                  event.outcome === "succeeded" ? "success" : event.outcome === "interrupted" ? "cancelled" : "failed",
              }),
          ...(event.durationMs === undefined ? {} : { duration_ms: event.durationMs }),
        },
        hostMetadata,
      ),
    )

  return {
    event,
    context,
    contextUsage,
    retryDecision,
    compactionOutcome,
    permissionDecision,
    usage,
    toolExecution,
    modelOutcome,
    skillLoad,
    skillCatalog,
    mcpRecovery,
  }
}

const hostMetadata = { "opencode.observation.schema_version": "1", "opencode.observation.source": "host" }
// Auxiliary summaries have no invocation owner token; a currently open Session execution may be unrelated.
const auxiliaryMetadata = { ...hostMetadata, "opencode.execution.attribution": "unavailable" }

const terminalMetadata = Schema.decodeUnknownOption(
  Schema.Struct({
    exit: Schema.optional(Schema.Finite),
    signal: Schema.optional(Schema.String),
    timeout: Schema.optional(Schema.Boolean),
    status: Schema.optional(Schema.Literals(["completed", "running"])),
  }),
)
const delegationMetadata = Schema.decodeUnknownOption(
  Schema.Struct({
    kind: Schema.Literals(["new", "continued"]),
    requestedMode: Schema.Literals(["foreground", "background"]),
    returnMode: Schema.Literals(["foreground", "background"]),
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
