export * as RelayMetrics from "./metrics.js"

import { Option, Schema } from "effect"
import type { MetricMeasurement } from "nemo-relay-node"

const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))
const Duration = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
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
})
const decode = Schema.decodeUnknownOption(Payload)
const DURATION_BUCKETS = [10, 100, 1_000, 10_000, 60_000, 300_000]
const SIZE_BUCKETS = [1, 10, 100, 1_000, 10_000, 100_000]
const labels = { "opencode.metric.schema_version": "1", "opencode.observation.source": "host" }

/** Converts the integration's host marks only; native provider usage and cost remain a separate plane. */
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
      return [...count(attributes), ...duration("opencode.agent.run.duration", attributes)]
    }
    case "opencode.agent.step.completed": {
      const attributes = select(payload, ["outcome"])
      return [...count(attributes), ...duration("opencode.agent.step.duration", attributes)]
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
      return count(select(payload, ["category", "outcome", "provider_executed"]))
    case "opencode.skill.tool.completed":
      return count(select(payload, ["outcome", "provider_executed"]))
    case "opencode.skill.activated":
      return count({ activation_source: "user" })
    case "opencode.permission.wait.started":
      return count(select(payload, ["family"]))
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
])

const select = (payload: typeof Attributes.Type, keys: readonly (keyof typeof Attributes.Type)[]) =>
  Object.fromEntries(keys.flatMap((key) => (payload[key] === undefined ? [] : [[key, payload[key]]])))

// Relay declares ambient const enums, so isolated modules use their verified numeric discriminants.
const counter = (name: string, value: number, attributes: Record<string, string | boolean>): MetricMeasurement => ({
  name,
  kind: 0,
  valueType: 0,
  value,
  unit: "{event}",
  attributes: { ...labels, ...attributes },
})
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
