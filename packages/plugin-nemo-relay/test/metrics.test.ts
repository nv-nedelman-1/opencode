import { expect, test } from "bun:test"
import { RelayBinding } from "../src/binding"
import { RelayMetrics } from "../src/metrics"

test("host instruments preserve missing timing boundaries and ignore provider token/cost data", () => {
  const completed = RelayMetrics.measurements("opencode.agent.step.completed", {
    count: 1,
    outcome: "success",
    duration_ms: 25,
    tokens: { input: 123 },
    cost_usd: 456,
    session_id: "SECRET",
    model: "SECRET",
  })
  expect(completed.map((metric) => [metric.name, metric.kind, metric.valueType, metric.value, metric.unit])).toEqual([
    ["opencode.agent.step.completed.count", 0, 0, 1, "{event}"],
    ["opencode.agent.step.duration", 3, 2, 25, "ms"],
  ])
  expect(completed[0].attributes).toEqual({
    "opencode.metric.schema_version": "1",
    "opencode.observation.source": "host",
    outcome: "success",
  })
  expect(JSON.stringify(completed)).not.toContain("SECRET")
  expect(RelayMetrics.measurements("opencode.agent.run.completed", { count: 1, outcome: "cancelled" })).toHaveLength(1)
  expect(RelayMetrics.measurements("gen_ai.usage", { count: 1, tokens: 123 })).toEqual([])
  expect(RelayMetrics.measurements("opencode.agent.step.streamed", { duration_ms: 5 })).toEqual([])
})

test("tool, skill, compaction, and human wait dimensions are finite allowlists", () => {
  const cases = [
    ["opencode.tool.completed", { count: 1, category: "terminal", provider_executed: false, outcome: "cancelled" }, 1],
    ["opencode.skill.tool.completed", { count: 1, outcome: "success", provider_executed: false }, 1],
    ["opencode.skill.activated", { count: 1, source: "user", name: "SECRET", character_count: 1 }, 1],
    ["opencode.compaction.completed", { count: 1, reason: "auto", outcome: "failed", duration_ms: 30 }, 2],
    [
      "opencode.permission.wait.completed",
      { count: 1, family: "terminal", resolution: "reject", duration_ms: 20, resource: "SECRET" },
      2,
    ],
    ["opencode.form.wait.completed", { count: 1, resolution: "answered", duration_ms: 40, answer: "SECRET" }, 2],
  ] as const
  cases.forEach(([name, data, size]) => {
    const result = RelayMetrics.measurements(name, data)
    expect(result).toHaveLength(size)
    expect(JSON.stringify(result)).not.toContain("SECRET")
  })
  ;[
    { count: -1 },
    { count: 1.5 },
    { count: Number.MAX_SAFE_INTEGER + 1 },
    { count: 1, outcome: "SECRET" },
    { count: 1, duration_ms: Infinity },
    { count: "1" },
  ].forEach((data) => expect(RelayMetrics.measurements("opencode.agent.run.completed", data)).toEqual([]))
})

test("context samples are per-request histograms and retry delay is explicitly the remaining delay", () => {
  const context = RelayMetrics.measurements("opencode.context.prepared", {
    call_role: "title",
    message_count: 3,
    part_count: 4,
    system_part_count: 2,
    system_character_count: 50,
    tools: "SECRET",
  })
  expect(context.map((metric) => [metric.name, metric.value, metric.kind])).toEqual([
    ["opencode.context.messages", 3, 3],
    ["opencode.context.parts", 4, 3],
    ["opencode.context.system_parts", 2, 3],
    ["opencode.context.system_characters", 50, 3],
  ])
  expect(context.every((metric) => metric.attributes?.call_role === "title")).toBe(true)
  const retry = RelayMetrics.measurements("opencode.llm.host_retry.scheduled", {
    count: 1,
    remaining_delay_ms: 0,
    attempt: 123,
  })
  expect(retry.map((metric) => [metric.name, metric.value])).toEqual([
    ["opencode.llm.host_retry.scheduled.count", 1],
    ["opencode.llm.host_retry.remaining_delay", 0],
  ])
  expect(JSON.stringify(retry)).not.toContain("attempt")
})

test("the native binding accepts compiled instruments and serializes a typed metric event", async () => {
  const [relay] = await RelayBinding.load()
  const events: unknown[] = []
  relay.registerSubscriber("host-metric-contract", (event) => {
    events.push(event)
  })
  try {
    relay.metric(
      "host.metric.contract",
      RelayMetrics.measurements("opencode.agent.run.completed", {
        count: 1,
        outcome: "success",
        duration_ms: 50,
      }),
      null,
      { "opencode.observation.source": "host" },
      123_000,
    )
    await relay.flushSubscribers()
    expect(events).toContainEqual(
      expect.objectContaining({
        name: "host.metric.contract",
        data_schema: { name: "nemo.relay.metric_measurements", version: "1" },
        data: {
          measurements: expect.arrayContaining([
            expect.objectContaining({
              name: "opencode.agent.run.completed.count",
              kind: "counter",
              value_type: "u64",
              value: 1,
            }),
            expect.objectContaining({
              name: "opencode.agent.run.duration",
              kind: "histogram",
              value_type: "f64",
              value: 50,
              unit: "ms",
            }),
          ]),
        },
      }),
    )
  } finally {
    relay.deregisterSubscriber("host-metric-contract")
  }
})
