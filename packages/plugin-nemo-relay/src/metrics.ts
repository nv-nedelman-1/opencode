export * as RelayMetrics from "./metrics.js"

import { Option, Schema } from "effect"
import type { Model } from "@opencode/schema/model"
import type { MetricMeasurement } from "nemo-relay-node"

const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))
const Duration = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
const decodeTokens = Schema.decodeUnknownOption(
  Schema.Struct({
    input: Count,
    output: Count,
    reasoning: Count,
    cache: Schema.Struct({ read: Count, write: Count }),
  }),
)
const Attributes = Schema.Struct({
  outcome: Schema.optional(Schema.Literals(["success", "failed", "cancelled"])),
  reason: Schema.optional(Schema.Literals(["auto", "manual", "user", "shutdown", "superseded", "inactivity"])),
  category: Schema.optional(
    Schema.Literals([
      "file_read",
      "file_write",
      "terminal",
      "code_search",
      "web",
      "planning",
      "delegation",
      "human_input",
      "skill",
      "code_execution",
      "mcp_resource",
      "extension",
      "unknown",
    ]),
  ),
  family: Schema.optional(Schema.Literals(["file_read", "file_write", "terminal", "skill", "delegation", "other"])),
  provider_executed: Schema.optional(Schema.Boolean),
  resolution: Schema.optional(Schema.Literals(["once", "always", "reject", "answered", "cancelled"])),
  call_role: Schema.optional(Schema.Literals(["primary", "title", "compaction", "generate"])),
  streaming: Schema.optional(Schema.Boolean),
  duration_boundary: Schema.optional(Schema.Literals(["streamed", "step_terminal"])),
  provider_family: Schema.optional(
    Schema.Literals(["openai", "anthropic", "google", "nvidia", "amazon", "azure", "other"]),
  ),
  model_family: Schema.optional(
    Schema.Literals(["gpt", "claude", "gemini", "nemotron", "llama", "qwen", "deepseek", "other"]),
  ),
  protocol_family: Schema.optional(
    Schema.Literals(["openai-chat", "openai-responses", "anthropic-messages", "gemini", "bedrock", "ai-sdk", "other"]),
  ),
  finish: Schema.optional(Schema.Literals(["stop", "length", "tool-calls", "content-filter", "error", "unknown"])),
  accounting_source: Schema.optional(Schema.Literals(["host_step", "host_usage", "host_compaction"])),
  cost_source: Schema.optional(Schema.Literals(["host_calculated", "catalog", "unavailable"])),
  http_status_class: Schema.optional(Schema.Literals(["2xx", "3xx", "4xx", "5xx", "other"])),
  error_category: Schema.optional(Schema.Literals(["http", "transport", "body", "cancelled", "protocol"])),
  output_kind: Schema.optional(Schema.Literals(["text", "reasoning", "tool"])),
  retryable: Schema.optional(Schema.Boolean),
  will_retry: Schema.optional(Schema.Boolean),
  decision_reason: Schema.optional(Schema.Literals(["scheduled", "rejected", "timeout-limit", "exhausted"])),
  error_type: Schema.optional(
    Schema.Literals([
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
      "unknown",
    ]),
  ),
  delay_source: Schema.optional(Schema.Literals(["policy", "hook", "retry-after", "backoff", "none"])),
  result_family: Schema.optional(
    Schema.Literals([
      "success",
      "error",
      "cancelled",
      "blocked",
      "zero_exit",
      "nonzero_exit",
      "timeout",
      "signal",
      "background",
      "unknown",
    ]),
  ),
  execution: Schema.optional(Schema.Literals(["local", "mcp", "provider", "unknown"])),
  trigger: Schema.optional(Schema.Literals(["manual", "proactive", "overflow"])),
  compaction_status: Schema.optional(Schema.Literals(["completed", "failed", "cancelled", "skipped"])),
  effect: Schema.optional(Schema.Literals(["allow", "deny", "ask"])),
  origin: Schema.optional(Schema.Literals(["rules", "hook"])),
})
const Payload = Schema.Struct({
  ...Attributes.fields,
  count: Schema.optional(Count),
  duration_ms: Schema.optional(Duration),
  remaining_delay_ms: Schema.optional(Duration),
  message_count: Schema.optional(Count),
  part_count: Schema.optional(Count),
  system_part_count: Schema.optional(Count),
  system_character_count: Schema.optional(Count),
  tool_count: Schema.optional(Count),
  selected_delay_ms: Schema.optional(Duration),
  context_utilization: Schema.optional(Duration),
  tokens: Schema.optional(Schema.Unknown),
  cost_usd: Schema.optional(Duration),
  step_count: Schema.optional(Count),
  logical_llm_count: Schema.optional(Count),
  tool_call_count: Schema.optional(Count),
  retry_count: Schema.optional(Count),
  permission_wait_count: Schema.optional(Count),
  form_wait_count: Schema.optional(Count),
  compaction_count: Schema.optional(Count),
  source_estimated_tokens: Schema.optional(Count),
  summary_estimated_tokens: Schema.optional(Count),
  retained_estimated_tokens: Schema.optional(Count),
  before_tokens: Schema.optional(Count),
  after_tokens: Schema.optional(Count),
  retained_ratio: Schema.optional(Duration),
})
const decode = Schema.decodeUnknownOption(Payload)
const DURATION_BUCKETS = [10, 100, 1_000, 10_000, 60_000, 300_000]
const SIZE_BUCKETS = [1, 10, 100, 1_000, 10_000, 100_000]
const RATIO_BUCKETS = [0.25, 0.5, 0.75, 0.9, 1, 1.1, 1.5, 2]
const labels = { "opencode.metric.schema_version": "1", "opencode.observation.source": "host" }

/** Compiles bounded host observations; custom accounting uses host billing, never native usage a second time. */
export function measurements(name: string, data: unknown): MetricMeasurement[] {
  if (!supported.has(name)) return []
  const decoded = decode(data)
  if (Option.isNone(decoded)) return []
  const payload = decoded.value
  const count = (attributes: Record<string, string | boolean> = {}) =>
    payload.count === undefined ? [] : [counter(`${name}.count`, payload.count, attributes)]
  const duration = (instrument: string, attributes: Record<string, string | boolean>) =>
    payload.duration_ms === undefined
      ? []
      : [histogram(instrument, payload.duration_ms, "ms", attributes, DURATION_BUCKETS)]

  switch (name) {
    case "opencode.agent.run.completed": {
      const attributes = select(payload, ["outcome"])
      return [
        ...count(attributes),
        ...duration("opencode.agent.run.duration", attributes),
        ...(
          [
            "step_count",
            "logical_llm_count",
            "tool_call_count",
            "retry_count",
            "permission_wait_count",
            "form_wait_count",
            "compaction_count",
          ] as const
        ).flatMap((kind) =>
          payload[kind] === undefined
            ? []
            : [
                histogram(
                  "opencode.agent.run.operation_count",
                  payload[kind],
                  "{operation}",
                  { ...attributes, kind },
                  SIZE_BUCKETS,
                ),
              ],
        ),
      ]
    }
    case "opencode.agent.step.completed": {
      const attributes = select(payload, ["outcome"])
      return [...count(attributes), ...duration("opencode.agent.step.duration", attributes)]
    }
    case "opencode.runtime.activation":
      return count()
    case "opencode.llm.logical.completed": {
      const attributes = {
        ...select(payload, ["outcome", "call_role", "duration_boundary", "provider_family", "model_family"]),
        scope: "logical_step",
      }
      return [
        ...count(attributes),
        ...duration("opencode.llm.logical.duration", attributes),
        ...(payload.count === undefined ? [] : [counter("opencode.model_route.count", payload.count, attributes)]),
        ...(payload.finish === undefined
          ? []
          : [counter("opencode.llm.finish_reason.count", 1, { ...attributes, finish: payload.finish })]),
        ...(payload.outcome !== "failed" || !payload.error_type?.startsWith("provider.")
          ? []
          : [counter("opencode.llm.error.count", 1, { ...attributes, error_type: payload.error_type })]),
      ]
    }
    case "opencode.llm.usage.recorded": {
      // Aggregate compaction completion repeats usage already billed by individual requests.
      if (
        !(["host_step", "host_usage"] as const).some((source) => source === payload.accounting_source) ||
        payload.call_role === undefined
      )
        return []
      const attributes = select(payload, ["accounting_source", "call_role", "cost_source"])
      const tokens = Option.getOrUndefined(decodeTokens(payload.tokens))
      return [
        ...(tokens === undefined
          ? []
          : (
              [
                ["input_total", tokens.input + tokens.cache.read + tokens.cache.write],
                ["input_non_cached", tokens.input],
                ["input_cache_read", tokens.cache.read],
                ["input_cache_write", tokens.cache.write],
                ["output_total", tokens.output + tokens.reasoning],
                ["output_reasoning", tokens.reasoning],
              ] as const
            ).flatMap(([kind, value]) =>
              Number.isSafeInteger(value)
                ? [counter("opencode.llm.tokens", value, { ...attributes, kind }, "{token}")]
                : [],
            )),
        ...(payload.cost_usd === undefined || payload.cost_source === "unavailable"
          ? []
          : [counter("opencode.llm.cost_usd", payload.cost_usd, attributes, "USD", 2)]),
      ]
    }
    case "opencode.llm.provider_attempt.started": {
      const attributes = select(payload, [
        "call_role",
        "streaming",
        "provider_family",
        "model_family",
        "protocol_family",
      ])
      return [
        ...count(attributes),
        ...(payload.count === undefined
          ? []
          : [counter("opencode.llm.provider_route.count", payload.count, attributes)]),
      ]
    }
    case "opencode.llm.provider_attempt.headers":
      return duration(
        "opencode.llm.provider_attempt.time_to_headers",
        select(payload, ["call_role", "http_status_class", "provider_family", "model_family", "protocol_family"]),
      )
    case "opencode.llm.provider_attempt.completed": {
      const attributes = select(payload, [
        "call_role",
        "outcome",
        "streaming",
        "error_category",
        "provider_family",
        "model_family",
        "protocol_family",
      ])
      return [
        ...count(attributes),
        ...duration("opencode.llm.provider_attempt.duration", attributes),
        ...(payload.outcome !== "failed" ||
        payload.error_category === undefined ||
        payload.error_category === "cancelled"
          ? []
          : [counter("opencode.llm.error.count", 1, { ...attributes, scope: "provider_attempt" })]),
      ]
    }
    case "opencode.llm.first_output":
    case "opencode.llm.logical.first_output":
      return duration("opencode.llm.time_to_first_output", {
        ...select(payload, [
          "call_role",
          "output_kind",
          "streaming",
          "provider_family",
          "model_family",
          "protocol_family",
        ]),
        scope: name === "opencode.llm.first_output" ? "provider_attempt" : "logical_step",
      })
    case "opencode.llm.retry.decision": {
      const attributes = select(payload, [
        "call_role",
        "retryable",
        "will_retry",
        "delay_source",
        "error_type",
        "decision_reason",
      ])
      return [
        ...count(attributes),
        ...(payload.error_type === undefined ? [] : [counter("opencode.llm.retry.error.count", 1, attributes)]),
        ...(payload.will_retry !== true ? [] : [counter("opencode.llm.provider_retry.scheduled.count", 1, attributes)]),
        ...(payload.will_retry !== true || payload.selected_delay_ms === undefined
          ? []
          : [
              histogram(
                "opencode.llm.provider_retry.delay",
                payload.selected_delay_ms,
                "ms",
                attributes,
                DURATION_BUCKETS,
              ),
            ]),
      ]
    }
    case "opencode.llm.host_retry.scheduled":
      return [
        ...count(),
        ...(payload.remaining_delay_ms === undefined
          ? []
          : [
              histogram(
                "opencode.llm.host_retry.remaining_delay",
                payload.remaining_delay_ms,
                "ms",
                {},
                DURATION_BUCKETS,
              ),
            ]),
      ]
    case "opencode.compaction.started":
      return count(select(payload, ["reason"]))
    case "opencode.compaction.completed": {
      const attributes = select(payload, ["reason", "outcome"])
      return [...count(attributes), ...duration("opencode.compaction.duration", attributes)]
    }
    case "opencode.tool.completed":
      return [
        ...count(select(payload, ["category", "outcome", "provider_executed", "execution", "result_family"])),
        ...duration("opencode.tool.duration", select(payload, ["category", "outcome", "execution"])),
        ...(payload.category !== "terminal" || payload.result_family === undefined
          ? []
          : [counter("opencode.tool.terminal_result.count", 1, select(payload, ["outcome", "result_family"]))]),
      ]
    case "opencode.skill.tool.completed":
      return count(select(payload, ["outcome", "provider_executed"]))
    case "opencode.skill.activated":
      return count({ activation_source: "user" })
    case "opencode.permission.wait.started":
      return count(select(payload, ["family"]))
    case "opencode.permission.evaluated":
      return count(select(payload, ["family", "effect", "origin"]))
    case "opencode.permission.wait.completed": {
      const attributes = select(payload, ["family", "resolution"])
      return [...count(attributes), ...duration("opencode.permission.wait.duration", attributes)]
    }
    case "opencode.form.wait.completed": {
      const attributes = select(payload, ["resolution"])
      return [...count(attributes), ...duration("opencode.form.wait.duration", attributes)]
    }
    case "opencode.context.prepared": {
      const attributes = select(payload, ["call_role"])
      return (
        [
          ["messages", payload.message_count, "{message}"],
          ["parts", payload.part_count, "{part}"],
          ["system_parts", payload.system_part_count, "{part}"],
          ["system_characters", payload.system_character_count, "{character}"],
          ["tools", payload.tool_count, "{tool}"],
        ] as const
      ).flatMap(([field, value, unit]) =>
        value === undefined ? [] : [histogram(`opencode.context.${field}`, value, unit, attributes, SIZE_BUCKETS)],
      )
    }
    case "opencode.context.usage":
      return payload.context_utilization === undefined
        ? []
        : [
            histogram(
              "opencode.llm.input_context_utilization",
              payload.context_utilization,
              "1",
              select(payload, ["call_role"]),
              RATIO_BUCKETS,
            ),
          ]
    case "opencode.compaction.outcome": {
      const attributes = select(payload, ["trigger", "compaction_status"])
      return [
        ...count(attributes),
        ...duration("opencode.compaction.attempt.duration", attributes),
        ...(
          [
            ["source", payload.source_estimated_tokens],
            ["summary", payload.summary_estimated_tokens],
            ["retained_recent", payload.retained_estimated_tokens],
          ] as const
        ).flatMap(([kind, value]) =>
          value === undefined
            ? []
            : [counter("opencode.compaction.estimated_tokens", value, { ...attributes, kind }, "{token}")],
        ),
        ...(
          [
            ["before", payload.before_tokens],
            ["after", payload.after_tokens],
          ] as const
        ).flatMap(([kind, value]) =>
          value === undefined
            ? []
            : [
                histogram(
                  "opencode.compaction.context_tokens",
                  value,
                  "{token}",
                  { ...attributes, kind },
                  SIZE_BUCKETS,
                ),
              ],
        ),
        ...(payload.retained_ratio === undefined
          ? []
          : [histogram("opencode.compaction.retained_ratio", payload.retained_ratio, "1", attributes, RATIO_BUCKETS)]),
      ]
    }
    default:
      return count()
  }
}

const supported = new Set([
  "opencode.agent.run.started",
  "opencode.agent.run.completed",
  "opencode.agent.step.started",
  "opencode.agent.step.completed",
  "opencode.llm.host_retry.scheduled",
  "opencode.compaction.started",
  "opencode.compaction.completed",
  "opencode.tool.completed",
  "opencode.skill.tool.requested",
  "opencode.skill.tool.completed",
  "opencode.skill.activated",
  "opencode.skill.catalog.updated",
  "opencode.permission.wait.started",
  "opencode.permission.wait.completed",
  "opencode.form.wait.started",
  "opencode.form.wait.completed",
  "opencode.context.prepared",
  "opencode.session.child.created",
  "opencode.mcp.status.changed",
  "opencode.mcp.resources.changed",
  "opencode.prompt.admitted",
  "opencode.instructions.updated",
  "opencode.runtime.activation",
  "opencode.llm.logical.completed",
  "opencode.llm.usage.recorded",
  "opencode.llm.provider_attempt.started",
  "opencode.llm.provider_attempt.headers",
  "opencode.llm.provider_attempt.completed",
  "opencode.llm.first_output",
  "opencode.llm.logical.first_output",
  "opencode.llm.retry.decision",
  "opencode.context.usage",
  "opencode.compaction.outcome",
  "opencode.permission.evaluated",
])

const select = (payload: typeof Attributes.Type, keys: readonly (keyof typeof Attributes.Type)[]) =>
  Object.fromEntries(keys.flatMap((key) => (payload[key] === undefined ? [] : [[key, payload[key]]])))

// Relay declares ambient const enums, so isolated modules use their verified numeric discriminants.
const counter = (
  name: string,
  value: number,
  attributes: Record<string, string | boolean>,
  unit = "{event}",
  valueType = 0,
): MetricMeasurement => ({
  name,
  kind: 0,
  valueType,
  value,
  unit,
  attributes: { ...labels, ...attributes },
})

/** Bounded configured-route labels, not proof of a remote provider's identity. */
export function route(model: Model.Ref, protocol = "") {
  const provider = model.providerID.toLowerCase()
  const family = model.id
    .toLowerCase()
    .split("/")
    .at(-1)
    ?.match(/^(nemotron|claude|gemini|llama|qwen|deepseek|gpt)(?:[-_.0-9]|$)/)?.[1]
  return {
    provider_family:
      provider === "anthropic"
        ? "anthropic"
        : provider === "nvidia"
          ? "nvidia"
          : provider === "azure"
            ? "azure"
            : provider === "amazon-bedrock"
              ? "amazon"
              : ["google", "google-vertex", "google-vertex-anthropic"].includes(provider)
                ? "google"
                : provider === "openai"
                  ? "openai"
                  : "other",
    model_family:
      (family as "nemotron" | "claude" | "gemini" | "llama" | "qwen" | "deepseek" | "gpt" | undefined) ?? "other",
    protocol_family:
      protocol === "openai-compatible-chat" || protocol === "openai-chat"
        ? "openai-chat"
        : ["openai-responses", "open-responses", "openai-compatible-responses", "xai-responses"].includes(protocol)
          ? "openai-responses"
          : protocol === "anthropic-messages"
            ? "anthropic-messages"
            : protocol === "gemini" || protocol === "gemini-generate-content"
              ? "gemini"
              : protocol === "bedrock-converse"
                ? "bedrock"
                : protocol === "ai-sdk"
                  ? "ai-sdk"
                  : "other",
  } as const
}
const histogram = (
  name: string,
  value: number,
  unit: string,
  attributes: Record<string, string | boolean>,
  boundaries: number[],
): MetricMeasurement => ({
  name,
  kind: 3,
  valueType: 2,
  value,
  unit,
  attributes: { ...labels, ...attributes },
  boundaries,
})
