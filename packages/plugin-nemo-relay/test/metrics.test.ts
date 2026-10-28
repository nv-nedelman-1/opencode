import { expect, test } from "bun:test"
import { RelayBinding } from "../src/binding"
import { RelayMetrics } from "../src/metrics"
import { Model } from "@opencode/schema/model"

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
  expect(
    RelayMetrics.measurements("opencode.agent.run.completed", { count: 1, outcome: "cancelled", reason: "shutdown" })[0]
      ?.attributes,
  ).toMatchObject({ outcome: "cancelled", reason: "shutdown" })
  expect(RelayMetrics.measurements("gen_ai.usage", { count: 1, tokens: 123 })).toEqual([])
  expect(RelayMetrics.measurements("opencode.agent.step.streamed", { duration_ms: 5 })).toEqual([])
})

test("run efficiency is an observed usage distribution, with unknown and zero input kept distinct", () => {
  const data = {
    count: 1,
    outcome: "success",
    usage_sample_count: 2,
    input_tokens: 20,
    output_tokens: 8,
    cache_read_tokens: 5,
  }
  const result = RelayMetrics.measurements("opencode.agent.run.completed", data)
  expect(
    result
      .filter((metric) => metric.name === "opencode.agent.run.observed_tokens")
      .map((metric) => [metric.attributes?.kind, metric.kind, metric.value]),
  ).toEqual([
    ["input_total", 3, 20],
    ["output_total", 3, 8],
    ["cache_read", 3, 5],
  ])
  expect(result.find((metric) => metric.name === "opencode.agent.run.cache_read_ratio")?.value).toBe(0.25)
  expect(result.every((metric) => !metric.name.startsWith("opencode.llm.tokens"))).toBe(true)
  for (const payload of [{ count: 1 }, { ...data, input_tokens: 0, cache_read_tokens: 0 }]) {
    expect(
      RelayMetrics.measurements("opencode.agent.run.completed", payload).some(
        (metric) => metric.name === "opencode.agent.run.cache_read_ratio",
      ),
    ).toBe(false)
  }
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

test("source-owned model, skill and tool measurements remain bounded and nonadditive", () => {
  const model = RelayMetrics.measurements("opencode.llm.host_call.completed", {
    count: 1,
    call_role: "title",
    outcome: "success",
    duration_ms: 20,
    model_family: "gpt",
    response_model_family: "nemotron",
    responseModel: "SECRET",
    tokens: { input: 123 },
    cost_usd: 9,
  })
  expect(model.map((metric) => metric.name)).toEqual([
    "opencode.llm.host_call.completed.count",
    "opencode.llm.host_call.duration",
  ])
  expect(model[0]?.attributes).toMatchObject({
    scope: "host_call",
    call_role: "title",
    model_family: "gpt",
    response_model_family: "nemotron",
  })
  const skill = RelayMetrics.measurements("opencode.skill.load.completed", {
    count: 1,
    load_channel: "user_mention",
    load_status: "completed",
    source_kind: "url",
    comparison_window: "session_memory",
    repeat_load: true,
    content_changed: false,
    id: "SECRET",
    content: "SECRET",
  })
  expect(skill).toHaveLength(1)
  expect(skill[0]?.attributes).toMatchObject({
    load_channel: "user_mention",
    load_status: "completed",
    source_kind: "url",
    repeat_load: true,
    content_changed: false,
  })
  const tool = RelayMetrics.measurements("opencode.tool.completed", {
    count: 1,
    category: "extension",
    tool_origin: "mcp",
    execution: "mcp",
    approval: "allowed_after_ask",
    name: "SECRET",
  })
  expect(tool[0]?.attributes).toMatchObject({
    category: "extension",
    tool_origin: "mcp",
    execution: "mcp",
    approval: "allowed_after_ask",
  })
  expect(JSON.stringify([model, skill, tool])).not.toContain("SECRET")
  ;[
    ["opencode.llm.host_call.completed", { response_model_family: "SECRET" }],
    ["opencode.skill.load.completed", { source_kind: "SECRET" }],
    ["opencode.tool.completed", { approval: "SECRET" }],
  ].forEach(([name, data]) =>
    expect(RelayMetrics.measurements(name as string, { count: 1, ...(data as object) })).toEqual([]),
  )
})

test("source-owned call detail keeps first output optional and provider errors bounded", () => {
  const result = RelayMetrics.measurements("opencode.llm.host_call.completed", {
    count: 1,
    call_role: "compaction",
    outcome: "failed",
    duration_ms: 25,
    finish: "length",
    error_kind: "incomplete-stream",
    first_output_ms: 10,
    output_kind: "reasoning",
    message: "SECRET",
    responseModel: "SECRET",
    tokens: { input: 99 },
    cost_usd: 9,
  })
  expect(result.map((metric) => [metric.name, metric.value])).toEqual([
    ["opencode.llm.host_call.completed.count", 1],
    ["opencode.llm.host_call.duration", 25],
    ["opencode.llm.host_call.first_output", 10],
  ])
  expect(result[2]?.attributes).toMatchObject({
    scope: "host_call",
    call_role: "compaction",
    output_kind: "reasoning",
    error_kind: "incomplete-stream",
    finish: "length",
  })
  expect(JSON.stringify(result)).not.toMatch(/SECRET|cost_usd|tokens/)
  expect(RelayMetrics.measurements("opencode.llm.host_call.completed", { count: 1, error_kind: "SECRET" })).toEqual([])
})

test("skill catalog metrics distinguish effective changes from authored lifecycle intent", () => {
  const result = RelayMetrics.measurements("opencode.skill.catalog.changed", {
    count: 2,
    catalog_action: "updated",
    source_kind: "embedded",
    previous_source_kind: "directory",
    content_changed: true,
    skill: { id: "SECRET", content: "SECRET", path: "/SECRET" },
  })
  expect(result).toHaveLength(1)
  expect(result[0]?.value).toBe(2)
  expect(result[0]?.attributes).toMatchObject({
    scope: "effective_catalog",
    catalog_action: "updated",
    source_kind: "embedded",
    previous_source_kind: "directory",
    content_changed: true,
  })
  expect(JSON.stringify(result)).not.toContain("SECRET")
  expect(
    RelayMetrics.measurements("opencode.skill.catalog.changed", { count: 1, catalog_action: "installed" }),
  ).toEqual([])
})

test("tool result metrics count reported truncation, terminal timeouts and real delegation modes", () => {
  const terminal = RelayMetrics.measurements("opencode.tool.completed", {
    count: 1,
    category: "terminal",
    outcome: "success",
    result_family: "timeout",
    output_truncated: true,
  })
  expect(terminal.map((metric) => metric.name)).toEqual([
    "opencode.tool.completed.count",
    "opencode.tool.terminal_result.count",
    "opencode.tool.output_truncation.count",
    "opencode.tool.timeout.count",
  ])
  const delegate = RelayMetrics.measurements("opencode.tool.completed", {
    count: 1,
    category: "delegation",
    delegation_kind: "continued",
    requested_mode: "foreground",
    return_mode: "background",
    sessionID: "SECRET",
  })
  expect(delegate[1]?.name).toBe("opencode.delegation.return.count")
  expect(delegate[1]?.attributes).toMatchObject({
    delegation_kind: "continued",
    requested_mode: "foreground",
    return_mode: "background",
  })
  expect(JSON.stringify(delegate)).not.toContain("SECRET")
  expect(RelayMetrics.measurements("opencode.tool.completed", { count: 1, output_truncated: "yes" })).toEqual([])
  expect(
    RelayMetrics.measurements("opencode.tool.completed", {
      count: 1,
      category: "terminal",
      result_family: "unknown",
    }).some((metric) => metric.name === "opencode.tool.timeout.count"),
  ).toBe(false)
})

test("MCP recovery exports source phase, terminal outcome and elapsed duration, never endpoint identity", () => {
  const result = RelayMetrics.measurements("opencode.mcp.recovery.completed", {
    count: 1,
    mcp_operation: "resource",
    recovery_phase: "retry",
    recovery_reason: "session_expired",
    outcome: "failed",
    duration_ms: 12,
    server: "SECRET",
    url: "SECRET",
    error: "SECRET",
  })
  expect(result.map((metric) => [metric.name, metric.value])).toEqual([
    ["opencode.mcp.recovery.completed.count", 1],
    ["opencode.mcp.recovery.duration", 12],
  ])
  expect(result[0]?.attributes).toMatchObject({ mcp_operation: "resource", recovery_phase: "retry", outcome: "failed" })
  expect(JSON.stringify(result)).not.toContain("SECRET")
  expect(RelayMetrics.measurements("opencode.mcp.recovery.started", { count: 1, mcp_operation: "SECRET" })).toEqual([])
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

test("host billing counters preserve cached/reasoning categories without re-counting completion aggregates", () => {
  const usage = {
    call_role: "compaction",
    accounting_source: "host_usage",
    cost_source: "catalog",
    tokens: { input: 8, output: 5, reasoning: 3, cache: { read: 2, write: 1 } },
    cost_usd: 0.025,
  }
  const compiled = RelayMetrics.measurements("opencode.llm.usage.recorded", usage)
  expect(compiled.map((metric) => [metric.name, metric.attributes?.kind, metric.value, metric.unit])).toEqual([
    ["opencode.llm.tokens", "input_total", 11, "{token}"],
    ["opencode.llm.tokens", "input_non_cached", 8, "{token}"],
    ["opencode.llm.tokens", "input_cache_read", 2, "{token}"],
    ["opencode.llm.tokens", "input_cache_write", 1, "{token}"],
    ["opencode.llm.tokens", "output_total", 8, "{token}"],
    ["opencode.llm.tokens", "output_reasoning", 3, "{token}"],
    ["opencode.llm.cost_usd", undefined, 0.025, "USD"],
  ])
  expect(compiled.at(-1)?.valueType).toBe(2)
  expect(
    RelayMetrics.measurements("opencode.compaction.completed", { ...usage, count: 1 }).map((metric) => metric.name),
  ).toEqual(["opencode.compaction.completed.count"])
  expect(
    RelayMetrics.measurements("opencode.llm.usage.recorded", { ...usage, cost_source: "unavailable" }).some((metric) =>
      metric.name.endsWith("cost_usd"),
    ),
  ).toBe(false)
  expect(RelayMetrics.measurements("opencode.llm.usage.recorded", { tokens: usage.tokens })).toEqual([])
})

test("attempt headers, first meaningful output, and final retry decisions remain separate measurements", () => {
  const route = RelayMetrics.route(Model.Ref.parse("nvidia/nemotron-test"), "openai-compatible-chat")
  expect(route).toEqual({ provider_family: "nvidia", model_family: "nemotron", protocol_family: "openai-chat" })
  expect(RelayMetrics.route(Model.Ref.parse("not-anthropic/not-gpt"), "openai-compatible-chat")).toEqual({
    provider_family: "other",
    model_family: "other",
    protocol_family: "openai-chat",
  })
  expect(RelayMetrics.route(Model.Ref.parse("google-vertex/anthropic/claude-sonnet-4")).model_family).toBe("claude")
  const started = RelayMetrics.measurements("opencode.llm.provider_attempt.started", {
    ...route,
    count: 1,
    streaming: true,
    call_role: "primary",
  })
  expect(started.map((metric) => metric.name)).toEqual([
    "opencode.llm.provider_attempt.started.count",
    "opencode.llm.provider_route.count",
  ])
  const cases = [
    [
      "opencode.llm.provider_attempt.headers",
      { duration_ms: 12, http_status_class: "2xx" },
      "opencode.llm.provider_attempt.time_to_headers",
    ],
    ["opencode.llm.first_output", { duration_ms: 30, output_kind: "reasoning" }, "opencode.llm.time_to_first_output"],
    [
      "opencode.llm.provider_attempt.completed",
      { duration_ms: 80, outcome: "success" },
      "opencode.llm.provider_attempt.duration",
    ],
  ] as const
  cases.forEach(([name, data, instrument]) => {
    const compiled = RelayMetrics.measurements(name, { ...route, ...data, providerID: "SECRET", modelID: "SECRET" })
    expect(compiled.map((metric) => metric.name)).toEqual([instrument])
    expect(JSON.stringify(compiled)).not.toContain("SECRET")
  })
  const rejected = RelayMetrics.measurements("opencode.llm.retry.decision", {
    count: 1,
    retryable: true,
    will_retry: false,
    error_type: "provider.rate-limit",
    decision_reason: "exhausted",
    selected_delay_ms: 500,
    status: 429,
  })
  expect(rejected.map((metric) => metric.name)).toEqual([
    "opencode.llm.retry.decision.count",
    "opencode.llm.retry.error.count",
  ])
  const scheduled = RelayMetrics.measurements("opencode.llm.retry.decision", {
    count: 1,
    retryable: true,
    will_retry: true,
    delay_source: "retry-after",
    selected_delay_ms: 500,
    status: 200,
  })
  expect(scheduled.map((metric) => [metric.name, metric.value])).toEqual([
    ["opencode.llm.retry.decision.count", 1],
    ["opencode.llm.provider_retry.scheduled.count", 1],
    ["opencode.llm.provider_retry.delay", 500],
  ])
})

test("error views distinguish actual I/O, host provider failures, and policy decisions without counting cancellation", () => {
  const name = "opencode.llm.error.count"
  const physical = RelayMetrics.measurements("opencode.llm.provider_attempt.completed", {
    outcome: "failed",
    error_category: "transport",
  })
  expect(physical).toHaveLength(1)
  expect(physical[0]).toMatchObject({ name, attributes: { scope: "provider_attempt", error_category: "transport" } })
  const logical = RelayMetrics.measurements("opencode.llm.logical.completed", {
    outcome: "failed",
    error_type: "provider.invalid-output",
  })
  expect(logical).toHaveLength(1)
  expect(logical[0]).toMatchObject({
    name,
    attributes: { scope: "logical_step", error_type: "provider.invalid-output" },
  })
  ;["aborted", "tool.execution", "permission.rejected"].forEach((error_type) =>
    expect(RelayMetrics.measurements("opencode.llm.logical.completed", { outcome: "failed", error_type })).toEqual([]),
  )
  expect(
    RelayMetrics.measurements("opencode.llm.provider_attempt.completed", {
      outcome: "cancelled",
      error_category: "cancelled",
    }),
  ).toEqual([])
})

test("utilization, compaction estimates, and per-run operation mix omit unknowns rather than recording zero", () => {
  expect(RelayMetrics.measurements("opencode.context.usage", {})).toEqual([])
  const context = RelayMetrics.measurements("opencode.context.usage", {
    context_utilization: 1.2,
    call_role: "primary",
  })
  expect(context[0].value).toBe(1.2)
  expect(context[0].unit).toBe("1")
  const compaction = RelayMetrics.measurements("opencode.compaction.outcome", {
    count: 1,
    compaction_status: "completed",
    trigger: "proactive",
    duration_ms: 125,
    before_tokens: 1_000,
    after_tokens: 200,
    retained_ratio: 0.2,
  })
  expect(compaction.map((metric) => [metric.name, metric.value])).toEqual([
    ["opencode.compaction.outcome.count", 1],
    ["opencode.compaction.attempt.duration", 125],
    ["opencode.compaction.context_tokens", 1_000],
    ["opencode.compaction.context_tokens", 200],
    ["opencode.compaction.retained_ratio", 0.2],
  ])
  const run = RelayMetrics.measurements("opencode.agent.run.completed", {
    count: 1,
    outcome: "success",
    step_count: 2,
    tool_call_count: 0,
  })
  expect(
    run
      .filter((metric) => metric.name.endsWith("operation_count"))
      .map((metric) => [metric.attributes?.kind, metric.value]),
  ).toEqual([
    ["step_count", 2],
    ["tool_call_count", 0],
  ])
  expect(RelayMetrics.measurements("opencode.compaction.outcome", { count: 1, before_tokens: Number.NaN })).toEqual([])
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
