import { expect, test } from "bun:test"
import type { SessionContext } from "@opencode/plugin/effect/session"
import type { ToolProvenance } from "@opencode/plugin/effect/tool"
import { Agent } from "@opencode/schema/agent"
import { Event } from "@opencode/schema/event"
import { Form } from "@opencode/schema/form"
import { McpEvent } from "@opencode/schema/mcp-event"
import { Model } from "@opencode/schema/model"
import { Money } from "@opencode/schema/money"
import { Provider } from "@opencode/schema/provider"
import { Permission } from "@opencode/schema/permission"
import { RelativePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
import { Skill } from "@opencode/schema/skill"
import { Tool } from "@opencode/schema/tool"
import { AbsolutePath } from "@opencode/schema/schema"
import { Effect, Schema } from "effect"
import type { ScopeHandle } from "nemo-relay-node"
import { RelayHost } from "../src/host"
import { RelayObserve } from "../src/observe"

const sessionID = Session.ID.make("ses_observe")
const sibling = Session.ID.make("ses_sibling")
const assistantMessageID = SessionMessage.ID.make("msg_observe")
const tokens = { input: 5, output: 3, reasoning: 1, cache: { read: 2, write: 0 } }
const model = { providerID: Provider.ID.make("provider"), id: Model.ID.make("model") }

const published = <D extends Event.Definition>(
  definition: D & Schema.ConstraintDecoder<unknown, never>,
  data: Event.Data<D>,
  created: number,
) =>
  Schema.decodeUnknownSync(definition)({
    id: Event.ID.create(),
    type: definition.type,
    created,
    data,
    ...(definition.durability === "durable"
      ? { durable: { aggregateID: sessionID, seq: 1, version: definition.durable.version } }
      : {}),
  }) as Event.Payload<D>

const makeFixture = (seen = new Set<string>()) => {
  const marks: Parameters<RelayHost.Runtime["mark"]>[] = []
  const opened: Parameters<RelayHost.Runtime["open"]>[] = []
  const closed: Parameters<RelayHost.Runtime["close"]>[] = []
  const runtime: Pick<RelayHost.Runtime, "admit" | "open" | "mark" | "close"> = {
    admit: (id) => {
      if (seen.has(id)) return false
      seen.add(id)
      return true
    },
    open: (...args) => {
      opened.push(args)
      return {} as ScopeHandle
    },
    mark: (...args) => marks.push(args),
    close: (...args) => closed.push(args),
  }
  const observer = RelayObserve.make(runtime)
  const capture = (id: string, provenance: ToolProvenance, target = sessionID) =>
    Effect.runSync(
      observer.toolExecution({
        sessionID: target,
        messageID: assistantMessageID,
        id: Tool.CallID.make(id),
        agent: Agent.ID.make("build"),
        tool: "SECRET",
        input: { SECRET: "SECRET" },
        provenance,
      }),
    )
  return { observer, capture, runtime, marks, opened, closed }
}

test("interleaved executions retain independent host timings and outcomes", () => {
  const fixture = makeFixture()
  ;[
    published(SessionEvent.Execution.Started, { sessionID }, 100),
    published(SessionEvent.Execution.Started, { sessionID: sibling }, 120),
    published(SessionEvent.Execution.Succeeded, { sessionID }, 180),
    published(SessionEvent.Execution.Interrupted, { sessionID: sibling, reason: "user" }, 200),
    published(SessionEvent.Execution.Succeeded, { sessionID }, 210),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  expect(fixture.opened).toEqual([
    [sessionID, undefined, 100_000],
    [sibling, undefined, 120_000],
  ])
  expect(fixture.closed).toEqual([
    [sessionID, "success"],
    [sibling, "cancelled"],
    [sessionID, "success"],
  ])
  expect(fixture.marks.map((mark) => mark[2])).toMatchObject([
    { count: 1 },
    { count: 1 },
    { count: 1, outcome: "success", duration_ms: 80 },
    { count: 1, outcome: "cancelled", duration_ms: 80, reason: "user" },
    { count: 1, outcome: "success" },
  ])
  expect(fixture.marks[0][4]).toBe(100_000)
  expect(fixture.marks[0][3]).toMatchObject({
    "opencode.observation.source": "host",
    "opencode.event_id": expect.any(String),
  })
})

test("step, retry, and compaction observations whitelist host data without duplicating provider accounting", () => {
  const fixture = makeFixture()
  ;[
    published(
      SessionEvent.Step.Started,
      { sessionID, assistantMessageID, model, agent: Agent.ID.make("build"), started: 100 },
      120,
    ),
    published(
      SessionEvent.RetryScheduled,
      {
        sessionID,
        assistantMessageID,
        attempt: 2,
        at: 200,
        error: { type: "provider.rate-limit", message: "SECRET", status: 429, response: { body: "SECRET" } },
      },
      150,
    ),
    published(
      SessionEvent.Step.Ended,
      {
        sessionID,
        assistantMessageID,
        finish: "stop",
        cost: Money.USD.make(0.01),
        tokens,
        files: [RelativePath.make("SECRET")],
      },
      220,
    ),
    published(SessionEvent.Compaction.Started, { sessionID, reason: "auto", recent: "SECRET" }, 240),
    published(
      SessionEvent.Compaction.Failed,
      { sessionID, reason: "auto", error: { type: "SECRET", message: "SECRET" } },
      270,
    ),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  expect(fixture.marks[1][2]).toEqual({
    count: 1,
    attempt: 2,
    remaining_delay_ms: 50,
    error_type: "provider.rate-limit",
    status: 429,
  })
  expect(fixture.marks[2][2]).toEqual({
    count: 1,
    outcome: "success",
    duration_ms: 120,
    finish: "stop",
    accounting_source: "host_step",
    cost_usd: 0.01,
    tokens,
  })
  expect(fixture.marks.find((mark) => mark[1] === "opencode.compaction.completed")?.[2]).toEqual({
    count: 1,
    reason: "auto",
    outcome: "failed",
    duration_ms: 30,
    accounting_source: "host_compaction",
    error_type: "unknown",
  })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
  expect(fixture.opened).toEqual([])
})

test("generic retries keep one observed step record and its earliest start", () => {
  const fixture = makeFixture()
  ;[
    published(
      SessionEvent.Step.Started,
      { sessionID, assistantMessageID, model, agent: Agent.ID.make("build"), started: 100 },
      120,
    ),
    published(
      SessionEvent.Step.Started,
      { sessionID, assistantMessageID, model, agent: Agent.ID.make("build"), started: 200 },
      220,
    ),
    published(
      SessionEvent.Step.Ended,
      { sessionID, assistantMessageID, finish: "stop", cost: Money.USD.make(0), tokens },
      300,
    ),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  expect(fixture.marks.filter((mark) => mark[1] === "opencode.agent.step.started")).toHaveLength(1)
  expect(fixture.marks.filter((mark) => mark[1] === "opencode.llm.logical.completed")).toHaveLength(1)
  expect(fixture.marks[1][2]).toMatchObject({ duration_ms: 200 })
  Effect.runSync(
    fixture.observer.event(
      published(
        SessionEvent.Step.Failed,
        {
          sessionID,
          assistantMessageID: SessionMessage.ID.make("msg_missing_start"),
          error: { type: "aborted", message: "SECRET" },
        },
        320,
      ),
    ),
  )
  expect(fixture.marks.filter((mark) => mark[1] === "opencode.agent.step.completed")[1]?.[2]).toEqual({
    count: 1,
    outcome: "cancelled",
    accounting_source: "host_step",
    error_type: "aborted",
  })
  expect(fixture.marks.filter((mark) => mark[1] === "opencode.llm.logical.completed")[1]?.[2]).toMatchObject({
    outcome: "cancelled",
    error_type: "aborted",
  })
})

test("context observations count structure, not request content or invented token estimates", () => {
  const fixture = makeFixture()
  const event: SessionContext = {
    sessionID,
    agent: Agent.ID.make("SECRET"),
    model,
    system: [{ type: "text", text: "SECRET" }],
    messages: [{ role: "user", content: [{ type: "text", text: "SECRET" }], metadata: { token: "SECRET" } }],
    tools: { SECRET: { description: "SECRET", input: {} } },
    options: { secret: "SECRET" },
  }
  Effect.runSync(fixture.observer.context(event))

  expect(fixture.marks[0][2]).toEqual({
    call_role: "primary",
    message_count: 1,
    part_count: 1,
    system_part_count: 1,
    system_character_count: 6,
    tool_count: 1,
  })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("shared event admission prevents a second Location observer reopening or closing the same execution", () => {
  const seen = new Set<string>()
  const first = makeFixture(seen)
  const second = makeFixture(seen)
  const started = published(SessionEvent.Execution.Started, { sessionID }, 100)
  const ended = published(SessionEvent.Execution.Succeeded, { sessionID }, 200)
  ;[started, ended].forEach((event) => Effect.runSync(first.observer.event(event)))
  ;[started, ended].forEach((event) => Effect.runSync(second.observer.event(event)))

  expect(first.opened).toHaveLength(1)
  expect(first.closed).toHaveLength(1)
  expect(second.opened).toEqual([])
  expect(second.closed).toEqual([])
  expect(second.marks).toEqual([])
})

test("interleaved Location observers share run accounting and complete pending waits once", () => {
  const fixture = makeFixture()
  const second = RelayObserve.make(fixture.runtime)
  const permission = Permission.ID.create()
  ;[
    published(SessionEvent.Execution.Started, { sessionID }, 100),
    published(
      SessionEvent.Step.Started,
      { sessionID, assistantMessageID, model, agent: Agent.ID.make("build"), started: 110 },
      120,
    ),
    published(Permission.Event.Asked, { sessionID, id: permission, action: "bash", resources: ["SECRET"] }, 130),
  ].forEach((event) => {
    Effect.runSync(fixture.observer.event(event))
    Effect.runSync(second.event(event))
  })
  ;[
    published(SessionEvent.Text.Delta, { sessionID, assistantMessageID, ordinal: 0, delta: "SECRET" }, 150),
    published(SessionEvent.Execution.Succeeded, { sessionID }, 200),
  ].forEach((event) => {
    Effect.runSync(second.event(event))
    Effect.runSync(fixture.observer.event(event))
  })

  expect(fixture.marks.filter((mark) => mark[1] === "opencode.agent.run.completed").map((mark) => mark[2])).toEqual([
    expect.objectContaining({ duration_ms: 100, step_count: 1, permission_wait_count: 1 }),
  ])
  expect(
    fixture.marks.filter((mark) => mark[1] === "opencode.permission.wait.completed").map((mark) => mark[2]),
  ).toEqual([expect.objectContaining({ resolution: "cancelled", duration_ms: 70 })])
  expect(fixture.marks.filter((mark) => mark[1] === "opencode.llm.logical.first_output")).toHaveLength(1)
  expect(fixture.opened).toHaveLength(1)
  expect(fixture.closed).toHaveLength(1)
})

test("Location turnover preserves child parentage and first-output state while the runtime remains active", () => {
  const fixture = makeFixture()
  ;[
    published(SessionEvent.Execution.Started, { sessionID }, 100),
    published(
      SessionEvent.Step.Started,
      { sessionID, assistantMessageID, model, agent: Agent.ID.make("build"), started: 100 },
      110,
    ),
    published(
      SessionEvent.Forked,
      { sessionID: sibling, parentID: sessionID, boundary: { type: "through", messageID: assistantMessageID } },
      120,
    ),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  // Only the surviving/new Location receives later events; the process runtime is unchanged.
  const surviving = RelayObserve.make(fixture.runtime)
  ;[
    published(SessionEvent.Execution.Started, { sessionID: sibling }, 130),
    published(SessionEvent.Text.Delta, { sessionID, assistantMessageID, ordinal: 0, delta: "SECRET" }, 140),
    published(SessionEvent.Text.Delta, { sessionID, assistantMessageID, ordinal: 0, delta: "SECRET" }, 150),
  ].forEach((event) => Effect.runSync(surviving.event(event)))

  expect(fixture.opened).toEqual([
    [sessionID, undefined, 100_000],
    [sibling, sessionID, 130_000],
  ])
  expect(
    fixture.marks.filter((mark) => mark[1] === "opencode.llm.logical.first_output").map((mark) => mark[2]),
  ).toEqual([expect.objectContaining({ duration_ms: 40, output_kind: "text" })])
})

test("skill requests and successful loads are distinct from user activation and never export skill content", () => {
  const fixture = makeFixture()
  ;[
    published(
      SessionEvent.Skill.Activated,
      { sessionID, id: Skill.ID.make("SECRET"), name: Skill.Name.make("SECRET"), text: "SECRET" },
      100,
    ),
    published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id: "call1", name: "skill" }, 110),
    published(
      SessionEvent.Tool.Success,
      { sessionID, assistantMessageID, id: "unrelated", content: [{ type: "text", text: "SECRET" }], executed: false },
      120,
    ),
    published(
      SessionEvent.Tool.Success,
      { sessionID, assistantMessageID, id: "call1", content: [{ type: "text", text: "SECRET" }], executed: false },
      130,
    ),
    published(Skill.Event.Updated, {}, 140),
    published(McpEvent.StatusChanged, { server: "SECRET" }, 150),
  ].forEach((event) => {
    Effect.runSync(fixture.observer.event(event))
    if (event.type === "session.tool.input.started")
      fixture.capture("call1", { origin: "builtin", execution: "local", category: "skill" })
  })

  expect(
    fixture.marks.filter((mark) => mark[1] !== "opencode.tool.completed").map((mark) => [mark[1], mark[2]]),
  ).toEqual([
    ["opencode.skill.activated", { count: 1, source: "user", character_count: 6 }],
    ["opencode.skill.tool.requested", { count: 1 }],
    [
      "opencode.skill.tool.completed",
      {
        count: 1,
        category: "skill",
        outcome: "success",
        provider_executed: false,
        execution: "local",
        tool_origin: "builtin",
        approval: "unknown",
        result_family: "success",
      },
    ],
    ["opencode.skill.catalog.updated", { count: 1 }],
    ["opencode.mcp.status.changed", { count: 1 }],
  ])
  expect(fixture.marks.slice(-2).map((mark) => mark[0])).toEqual([undefined, undefined])
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("provider tools and concurrent host tools get distinct nonadditive host summaries", () => {
  const fixture = makeFixture()
  fixture.capture("shared", { origin: "builtin", execution: "local", category: "terminal" }, sibling)
  ;[
    published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id: "shared", name: "websearch" }, 100),
    published(
      SessionEvent.Tool.Input.Started,
      { sessionID: sibling, assistantMessageID, id: "shared", name: "bash" },
      110,
    ),
    published(
      SessionEvent.Tool.Success,
      { sessionID, assistantMessageID, id: "shared", executed: true, content: [{ type: "text", text: "SECRET" }] },
      120,
    ),
    published(
      SessionEvent.Tool.Failed,
      {
        sessionID: sibling,
        assistantMessageID,
        id: "shared",
        executed: false,
        error: { type: "aborted", message: "SECRET" },
      },
      130,
    ),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  expect(fixture.marks.map((mark) => [mark[0], mark[1], mark[2]])).toEqual([
    [
      sessionID,
      "opencode.tool.completed",
      {
        count: 1,
        category: "unknown",
        outcome: "success",
        provider_executed: true,
        execution: "provider",
        tool_origin: "unknown",
        approval: "unknown",
        result_family: "success",
      },
    ],
    [
      sibling,
      "opencode.tool.completed",
      {
        count: 1,
        category: "terminal",
        outcome: "cancelled",
        provider_executed: false,
        execution: "local",
        tool_origin: "builtin",
        approval: "unknown",
        result_family: "cancelled",
        error_type: "aborted",
      },
    ],
  ])
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("V2 subagent tool calls and their permissions are classified as delegation", () => {
  const fixture = makeFixture()
  fixture.capture("call", { origin: "builtin", execution: "local", category: "delegation" })
  const permission = Permission.ID.create()
  ;[
    published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id: "call", name: "subagent" }, 100),
    published(
      Permission.Event.Asked,
      {
        sessionID,
        id: permission,
        action: "subagent",
        resources: ["SECRET"],
        source: { type: "tool", messageID: assistantMessageID, id: "call" },
      },
      110,
    ),
    published(Permission.Event.Replied, { sessionID, requestID: permission, reply: "once" }, 120),
    published(
      SessionEvent.Tool.Success,
      { sessionID, assistantMessageID, id: "call", executed: false, content: [{ type: "text", text: "SECRET" }] },
      130,
    ),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  expect(fixture.marks.map((mark) => mark[2])).toEqual([
    { count: 1, family: "delegation" },
    { count: 1, family: "delegation", resolution: "once", duration_ms: 10 },
    {
      count: 1,
      category: "delegation",
      outcome: "success",
      provider_executed: false,
      execution: "local",
      tool_origin: "builtin",
      approval: "allowed_after_ask",
      result_family: "success",
    },
  ])
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("parallel permission and form waits use their own source timestamps and omit human answers", () => {
  const fixture = makeFixture()
  const permission = Permission.ID.create()
  const form = Form.ID.create()
  ;[
    published(
      Permission.Event.Asked,
      { sessionID, id: permission, action: "bash", resources: ["SECRET"], message: "SECRET" },
      100,
    ),
    published(
      Form.Event.Created,
      { form: { id: form, sessionID: "global", title: "SECRET", fields: [{ type: "string", key: "SECRET" }] } },
      120,
    ),
    published(Permission.Event.Replied, { sessionID, requestID: permission, reply: "reject" }, 180),
    published(Form.Event.Replied, { id: form, sessionID: "global", answer: { SECRET: "SECRET" } }, 220),
    published(Form.Event.Cancelled, { id: Form.ID.create(), sessionID }, 230),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  expect(fixture.marks[2][2]).toEqual({ count: 1, resolution: "reject", family: "terminal", duration_ms: 80 })
  expect(fixture.marks[3][0]).toBeUndefined()
  expect(fixture.marks[3][2]).toEqual({ count: 1, resolution: "answered", duration_ms: 100 })
  expect(fixture.marks).toHaveLength(4)
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("logical first output ignores empty fragments and stops once, while stream duration excludes tool settlement", () => {
  const fixture = makeFixture()
  ;[
    published(
      SessionEvent.Step.Started,
      { sessionID, assistantMessageID, model, agent: Agent.ID.make("build"), started: 100 },
      110,
    ),
    published(SessionEvent.Text.Started, { sessionID, assistantMessageID, ordinal: 0 }, 120),
    published(SessionEvent.Text.Delta, { sessionID, assistantMessageID, ordinal: 0, delta: "" }, 130),
    published(SessionEvent.Reasoning.Delta, { sessionID, assistantMessageID, ordinal: 0, delta: "SECRET" }, 140),
    published(SessionEvent.Text.Delta, { sessionID, assistantMessageID, ordinal: 0, delta: "SECRET" }, 150),
    published(SessionEvent.Step.Streamed, { sessionID, assistantMessageID }, 180),
    published(
      SessionEvent.Step.Ended,
      { sessionID, assistantMessageID, finish: "tool-calls", cost: Money.USD.make(0), tokens },
      300,
    ),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  const first = fixture.marks.filter((mark) => mark[1] === "opencode.llm.logical.first_output")
  expect(first).toHaveLength(1)
  expect(first[0][2]).toMatchObject({ output_kind: "reasoning", duration_ms: 40 })
  expect(fixture.marks.find((mark) => mark[1] === "opencode.llm.logical.completed")?.[2]).toMatchObject({
    count: 1,
    duration_ms: 80,
    duration_boundary: "streamed",
    finish: "tool-calls",
    outcome: "success",
  })
  expect(fixture.marks.find((mark) => mark[1] === "opencode.agent.step.completed")?.[2]).toMatchObject({
    duration_ms: 200,
  })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("multiple pending waits cancel independently before run closure and late replies do not complete twice", () => {
  const fixture = makeFixture()
  const permissions = [Permission.ID.create(), Permission.ID.create()]
  const form = Form.ID.create()
  ;[
    published(SessionEvent.Execution.Started, { sessionID }, 100),
    ...permissions.map((id, index) =>
      published(Permission.Event.Asked, { sessionID, id, action: "read", resources: ["SECRET"] }, 110 + index),
    ),
    published(
      Form.Event.Created,
      { form: { id: form, sessionID, title: "SECRET", fields: [{ type: "string", key: "SECRET" }] } },
      120,
    ),
    published(SessionEvent.Execution.Interrupted, { sessionID, reason: "user" }, 200),
    published(Permission.Event.Replied, { sessionID, requestID: permissions[0], reply: "reject" }, 210),
    published(Form.Event.Cancelled, { id: form, sessionID }, 220),
    published(SessionEvent.Execution.Interrupted, { sessionID, reason: "user" }, 230),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  const waits = fixture.marks.filter((mark) => mark[1].endsWith("wait.completed"))
  expect(waits.map((mark) => mark[2])).toEqual([
    {
      count: 1,
      family: "file_read",
      resolution: "cancelled",
      cancellation_source: "session_terminal",
      duration_ms: 90,
    },
    {
      count: 1,
      family: "file_read",
      resolution: "cancelled",
      cancellation_source: "session_terminal",
      duration_ms: 89,
    },
    { count: 1, resolution: "cancelled", cancellation_source: "session_terminal", duration_ms: 80 },
  ])
  expect(new Set(waits.map((mark) => (mark[3] as Record<string, unknown>)["opencode.event_id"])).size).toBe(3)
  expect(fixture.marks.find((mark) => mark[1] === "opencode.agent.run.completed")?.[2]).toMatchObject({
    permission_wait_count: 2,
    form_wait_count: 1,
  })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test.each([SessionEvent.Text.Ended, SessionEvent.Reasoning.Ended, SessionEvent.Tool.Input.Ended])(
  "final-only output fallback records actual availability once: %s",
  (definition) => {
    const fixture = makeFixture()
    ;[
      published(
        SessionEvent.Step.Started,
        { sessionID, assistantMessageID, model, agent: Agent.ID.make("build"), started: 100 },
        110,
      ),
      published(definition, { sessionID, assistantMessageID, id: "call", ordinal: 0, text: "" }, 120),
      published(definition, { sessionID, assistantMessageID, id: "call", ordinal: 0, text: "SECRET" }, 200),
      published(definition, { sessionID, assistantMessageID, id: "call", ordinal: 1, text: "SECRET" }, 300),
    ].forEach((event) => Effect.runSync(fixture.observer.event(event)))
    const first = fixture.marks.filter((mark) => mark[1] === "opencode.llm.logical.first_output")
    expect(first).toHaveLength(1)
    expect(first[0][2]).toMatchObject({
      duration_ms: 100,
      output_kind:
        definition.type === "session.text.ended"
          ? "text"
          : definition.type === "session.reasoning.ended"
            ? "reasoning"
            : "tool",
    })
    expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
  },
)

test("host billing counters use primary terminals and auxiliary charges, never compaction aggregate twice", () => {
  const fixture = makeFixture()
  const ended = published(
    SessionEvent.Step.Ended,
    { sessionID, assistantMessageID, finish: "stop", cost: Money.USD.make(0.01), tokens },
    200,
  )
  Effect.runSync(fixture.observer.event(ended))
  Effect.runSync(fixture.observer.event(ended))
  Effect.runSync(
    fixture.observer.usage({
      sessionID,
      agent: Agent.ID.make("build"),
      model,
      source: "compaction",
      cost: Money.USD.make(0.02),
      tokens,
      costSource: "host_calculated",
    }),
  )
  Effect.runSync(
    fixture.observer.event(
      published(
        SessionEvent.Compaction.Ended,
        { sessionID, reason: "auto", text: "SECRET", recent: "SECRET", cost: Money.USD.make(0.02), tokens },
        300,
      ),
    ),
  )

  expect(fixture.marks.filter((mark) => mark[1] === "opencode.llm.usage.recorded").map((mark) => mark[2])).toEqual([
    { call_role: "primary", accounting_source: "host_step", cost_source: "host_calculated", cost_usd: 0.01, tokens },
    {
      call_role: "compaction",
      accounting_source: "host_usage",
      cost_source: "host_calculated",
      cost_usd: 0.02,
      tokens,
    },
  ])
})

test("source observations preserve missing native estimates and finalized retry/permission facts", () => {
  const fixture = makeFixture()
  const identity = { sessionID, agent: Agent.ID.make("build"), model }
  Effect.runSync(
    fixture.observer.contextUsage({ ...identity, kind: "primary", measured: 600, estimated: 200, limit: 1000 }),
  )
  Effect.runSync(
    fixture.observer.compactionOutcome({
      ...identity,
      trigger: "overflow",
      status: "completed",
      durationMs: 125,
      before: 800,
      after: 200,
      sourceEstimatedTokens: 700,
      summaryEstimatedTokens: 100,
      retainedEstimatedTokens: 50,
    }),
  )
  Effect.runSync(fixture.observer.compactionOutcome({ ...identity, trigger: "manual", status: "completed" }))
  Effect.runSync(
    fixture.observer.retryDecision({
      ...identity,
      kind: "compaction",
      error: { type: "provider.rate-limit", message: "SECRET", status: 429 },
      attempt: 2,
      retryable: true,
      decision: { retry: true, delay: 1000 },
      source: "retry-after",
      reason: "scheduled",
    }),
  )
  Effect.runSync(
    fixture.observer.permissionDecision({
      sessionID,
      action: "read",
      resources: ["SECRET"],
      effect: "deny",
      origin: "rules",
    }),
  )

  expect(fixture.marks[0][2]).toMatchObject({
    measured_tokens: 600,
    estimated_tokens: 200,
    context_limit: 1000,
    context_utilization: 0.8,
  })
  expect(fixture.marks[1][2]).toMatchObject({
    trigger: "overflow",
    compaction_status: "completed",
    duration_ms: 125,
    before_tokens: 800,
    after_tokens: 200,
    retained_ratio: 0.25,
    source_estimated_tokens: 700,
    summary_estimated_tokens: 100,
    retained_estimated_tokens: 50,
  })
  expect(fixture.marks[2][2]).not.toHaveProperty("before_tokens")
  expect(fixture.marks[2][2]).not.toHaveProperty("after_tokens")
  expect(fixture.marks[2][2]).not.toHaveProperty("retained_ratio")
  expect(fixture.marks[2][2]).not.toHaveProperty("duration_ms")
  expect(fixture.marks[3][2]).toMatchObject({
    call_role: "compaction",
    will_retry: true,
    retryable: true,
    selected_delay_ms: 1000,
    delay_source: "retry-after",
    decision_reason: "scheduled",
    error_type: "provider.rate-limit",
    status: 429,
  })
  expect(fixture.marks[4][2]).toEqual({ count: 1, family: "file_read", effect: "deny", origin: "rules" })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("tool execution durations exclude input streaming and terminal families never inspect output text", () => {
  const fixture = makeFixture()
  const cases = [
    [{ exit: 0 }, "zero_exit"],
    [{ exit: 1 }, "nonzero_exit"],
    [{ timeout: true, exit: 0 }, "timeout"],
    [{ signal: "SECRET" }, "signal"],
    [{ status: "running" }, "background"],
    [{}, "unknown"],
  ] as const
  cases.forEach(([metadata, result], index) => {
    const id = String(index)
    fixture.capture(id, { origin: "builtin", execution: "local", category: "terminal" })
    ;[
      published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id, name: "shell" }, 100),
      published(
        SessionEvent.Tool.Called,
        { sessionID, assistantMessageID, id, executed: false, input: { SECRET: "SECRET" } },
        150,
      ),
      published(
        SessionEvent.Tool.Success,
        { sessionID, assistantMessageID, id, executed: false, content: [{ type: "text", text: "SECRET" }], metadata },
        200,
      ),
    ].forEach((event) => Effect.runSync(fixture.observer.event(event)))
    expect(fixture.marks.filter((mark) => mark[1] === "opencode.tool.completed")[index]?.[2]).toMatchObject({
      outcome: "success",
      duration_ms: 50,
      result_family: result,
    })
  })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("run operation mix counts logical boundaries, not retries as extra steps, and keeps configured denies distinct", () => {
  const fixture = makeFixture()
  const run = (event: Parameters<typeof fixture.observer.event>[0]) => Effect.runSync(fixture.observer.event(event))
  run(published(SessionEvent.Execution.Started, { sessionID }, 100))
  ;[110, 120].forEach((started) =>
    run(
      published(
        SessionEvent.Step.Started,
        {
          sessionID,
          assistantMessageID,
          model,
          agent: Agent.ID.make("build"),
          started,
        },
        started,
      ),
    ),
  )
  run(
    published(
      SessionEvent.RetryScheduled,
      {
        sessionID,
        assistantMessageID,
        attempt: 2,
        at: 150,
        error: { type: "provider.rate-limit", message: "SECRET", status: 429 },
      },
      130,
    ),
  )
  run(published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id: "call", name: "read" }, 140))
  run(
    published(SessionEvent.Tool.Called, { sessionID, assistantMessageID, id: "call", executed: false, input: {} }, 150),
  )
  Effect.runSync(
    fixture.observer.permissionDecision({
      sessionID,
      action: "read",
      resources: ["SECRET"],
      effect: "deny",
      origin: "rules",
      source: { type: "tool", messageID: assistantMessageID, id: "call" },
    }),
  )
  run(
    published(
      SessionEvent.Tool.Failed,
      { sessionID, assistantMessageID, id: "call", executed: false, error: { type: "aborted", message: "SECRET" } },
      160,
    ),
  )
  run(published(SessionEvent.Compaction.Started, { sessionID, reason: "auto", recent: "SECRET" }, 170))
  run(
    published(
      SessionEvent.Compaction.Failed,
      { sessionID, reason: "auto", error: { type: "provider.unknown", message: "SECRET" } },
      180,
    ),
  )
  run(
    published(
      SessionEvent.Step.Ended,
      { sessionID, assistantMessageID, finish: "stop", cost: Money.USD.make(0), tokens },
      190,
    ),
  )
  run(published(SessionEvent.Execution.Succeeded, { sessionID }, 200))
  expect(fixture.marks.find((mark) => mark[1] === "opencode.tool.completed")?.[2]).toMatchObject({
    result_family: "cancelled",
    outcome: "cancelled",
  })
  expect(fixture.marks.find((mark) => mark[1] === "opencode.agent.run.completed")?.[2]).toMatchObject({
    step_count: 1,
    logical_llm_count: 1,
    tool_call_count: 1,
    retry_count: 1,
    compaction_count: 1,
  })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("source model outcomes cover auxiliary calls without duplicating logical steps or billing", () => {
  const fixture = makeFixture()
  ;(["primary", "title", "compaction", "generate"] as const).forEach((kind) =>
    Effect.runSync(
      fixture.observer.modelOutcome({
        sessionID,
        agent: Agent.ID.make("SECRET"),
        model,
        kind,
        status: "completed",
        durationMs: 25,
        ...(kind === "title" ? {} : { responseModel: "nvidia/nemotron-SECRET" }),
      }),
    ),
  )
  expect(fixture.marks).toHaveLength(4)
  expect(fixture.marks.every((mark) => mark[1] === "opencode.llm.host_call.completed")).toBe(true)
  expect(fixture.marks[0][2]).toMatchObject({
    count: 1,
    call_role: "primary",
    outcome: "success",
    duration_ms: 25,
    response_model_family: "nemotron",
  })
  expect(fixture.marks[1][2]).not.toHaveProperty("response_model_family")
  expect(fixture.marks.map((mark) => mark[0])).toEqual([sessionID, undefined, undefined, undefined])
  expect(JSON.stringify(fixture.marks)).not.toMatch(/SECRET|tokens|cost_usd/)
})

test("auxiliary summaries preserve metrics without borrowing a concurrent execution's parent", () => {
  const fixture = makeFixture()
  const identity = { sessionID, agent: Agent.ID.make("build"), model }
  Effect.runSync(fixture.observer.event(published(SessionEvent.Execution.Started, { sessionID }, 100)))
  fixture.marks.length = 0
  Effect.runSync(fixture.observer.context({ ...identity, messages: [], system: [], options: {} }, "generate"))
  Effect.runSync(fixture.observer.contextUsage({ ...identity, kind: "generate", measured: 60, estimated: 20 }))
  Effect.runSync(
    fixture.observer.retryDecision({
      ...identity,
      kind: "generate",
      error: { type: "provider.rate-limit", message: "SECRET" },
      attempt: 1,
      retryable: true,
      decision: { retry: false },
      source: "policy",
      reason: "exhausted",
    }),
  )
  Effect.runSync(fixture.observer.compactionOutcome({ ...identity, trigger: "manual", status: "completed" }))
  Effect.runSync(
    fixture.observer.usage({
      ...identity,
      source: "generate",
      tokens,
      cost: Money.USD.make(0.01),
      costSource: "host_calculated",
    }),
  )
  Effect.runSync(fixture.observer.modelOutcome({ ...identity, kind: "generate", status: "completed", durationMs: 25 }))
  expect(fixture.marks.map((mark) => mark[1])).toEqual([
    "opencode.context.prepared",
    "opencode.context.usage",
    "opencode.llm.retry.decision",
    "opencode.compaction.outcome",
    "opencode.llm.usage.recorded",
    "opencode.llm.host_call.completed",
  ])
  expect(fixture.marks.every((mark) => mark[0] === undefined)).toBe(true)
  expect(
    fixture.marks.every(
      (mark) => (mark[3] as Record<string, unknown>)["opencode.execution.attribution"] === "unavailable",
    ),
  ).toBe(true)
  expect(fixture.marks[4][2]).toMatchObject({ tokens, cost_usd: 0.01, call_role: "generate" })
  expect(fixture.marks[5][2]).toMatchObject({ count: 1, duration_ms: 25, outcome: "success", call_role: "generate" })
  Effect.runSync(fixture.observer.modelOutcome({ ...identity, kind: "primary", status: "completed", durationMs: 50 }))
  expect(fixture.marks.at(-1)?.[0]).toBe(sessionID)
  expect(fixture.marks.at(-1)?.[3]).not.toHaveProperty("opencode.execution.attribution")
})

test("skill loads compare only successful, privately retained session snapshots", () => {
  const fixture = makeFixture()
  const skill = {
    id: Skill.ID.make("SECRET"),
    name: Skill.Name.make("SECRET"),
    path: AbsolutePath.make("/SECRET"),
    content: "SECRET",
  }
  const load = (target: Session.ID, status: "completed" | "failed", content = skill.content) =>
    Effect.runSync(
      fixture.observer.skillLoad({
        sessionID: target,
        channel: "tool",
        status,
        sourceKind: "directory",
        skill: { ...skill, content },
      }),
    )
  load(sessionID, "completed")
  load(sibling, "completed", "SECRET_SIBLING")
  load(sessionID, "failed", "SECRET_CHANGED")
  load(sessionID, "completed")
  load(sessionID, "completed", "SECRET_CHANGED")
  expect(fixture.marks.map((mark) => mark[2])).toEqual([
    expect.not.objectContaining({ repeat_load: true }),
    expect.not.objectContaining({ repeat_load: true }),
    expect.not.objectContaining({ repeat_load: true }),
    expect.objectContaining({ repeat_load: true, content_changed: false, comparison_window: "session_memory" }),
    expect.objectContaining({ repeat_load: true, content_changed: true }),
  ])
  Effect.runSync(fixture.observer.event(published(SessionEvent.Deleted, { sessionID }, 100)))
  load(sessionID, "completed")
  expect(fixture.marks.at(-1)?.[2]).not.toHaveProperty("repeat_load")
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test.each(["skills", "sessions"])("skill comparison evicts old %s without claiming first-ever use", (dimension) => {
  const fixture = makeFixture()
  const load = (index: number) =>
    Effect.runSync(
      fixture.observer.skillLoad({
        sessionID: dimension === "sessions" ? Session.ID.make(`ses_${index}`) : sessionID,
        channel: "user_activation",
        status: "completed",
        sourceKind: "unknown",
        skill: {
          id: Skill.ID.make(dimension === "skills" ? String(index) : "skill"),
          name: Skill.Name.make("SECRET"),
          path: AbsolutePath.make("/SECRET"),
          content: "SECRET",
        },
      }),
    )
  const capacity = dimension === "sessions" ? 256 : 64
  for (let index = 0; index <= capacity; index++) load(index)
  load(0)
  expect(fixture.marks.at(-1)?.[2]).not.toHaveProperty("repeat_load")
  expect(fixture.marks.at(-1)?.[2]).not.toHaveProperty("content_changed")
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("host provenance survives delayed bus events and nested CodeMode entries without guessing tool names", () => {
  const fixture = makeFixture()
  fixture.capture("call", { origin: "builtin", execution: "local", category: "code_execution" })
  fixture.capture("call", { origin: "mcp", execution: "mcp", category: "extension" })
  fixture.capture("mcp", { origin: "mcp", execution: "mcp", category: "extension" })
  ;["call", "mcp", "unobserved"].forEach((id) => {
    Effect.runSync(
      fixture.observer.event(
        published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id, name: "bash" }, 100),
      ),
    )
    Effect.runSync(
      fixture.observer.event(
        published(
          SessionEvent.Tool.Success,
          { sessionID, assistantMessageID, id, executed: false, content: [{ type: "text", text: "SECRET" }] },
          200,
        ),
      ),
    )
  })
  expect(fixture.marks.map((mark) => mark[2])).toMatchObject([
    { category: "code_execution", tool_origin: "builtin", execution: "local" },
    { category: "extension", tool_origin: "mcp", execution: "mcp" },
    { category: "unknown", tool_origin: "unknown", execution: "unknown" },
  ])
})

test("an older queued run terminal cannot erase a subsequent run's captured tool provenance", () => {
  const fixture = makeFixture()
  Effect.runSync(fixture.observer.event(published(SessionEvent.Execution.Started, { sessionID }, 100)))
  fixture.capture("next", { origin: "mcp", execution: "mcp", category: "extension" })
  ;[
    published(SessionEvent.Execution.Succeeded, { sessionID }, 150),
    published(SessionEvent.Execution.Started, { sessionID }, 160),
    published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id: "next", name: "bash" }, 170),
    published(
      SessionEvent.Tool.Success,
      { sessionID, assistantMessageID, id: "next", executed: false, content: [{ type: "text", text: "SECRET" }] },
      180,
    ),
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))
  expect(fixture.marks.find((mark) => mark[1] === "opencode.tool.completed")?.[2]).toMatchObject({
    tool_origin: "mcp",
    execution: "mcp",
    category: "extension",
  })
})

test("permission facts attach only to the matching call and denial remains sticky", () => {
  const fixture = makeFixture()
  ;["one", "two"].forEach((id) => fixture.capture(id, { origin: "plugin", execution: "local", category: "extension" }))
  ;(["deny", "allow"] as const).forEach((effect) =>
    Effect.runSync(
      fixture.observer.permissionDecision({
        sessionID,
        action: "bash",
        resources: ["SECRET"],
        effect,
        origin: "rules",
        source: { type: "tool", messageID: assistantMessageID, id: "one" },
      }),
    ),
  )
  ;["one", "two"].forEach((id) =>
    Effect.runSync(
      fixture.observer.event(
        published(
          SessionEvent.Tool.Failed,
          {
            sessionID,
            assistantMessageID,
            id,
            executed: false,
            error: { type: "aborted", message: "SECRET" },
          },
          200,
        ),
      ),
    ),
  )
  expect(fixture.marks.filter((mark) => mark[1] === "opencode.tool.completed").map((mark) => mark[2])).toMatchObject([
    { approval: "denied", result_family: "cancelled", outcome: "cancelled" },
    { approval: "unknown", result_family: "cancelled", outcome: "cancelled" },
  ])
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("cleanup rejection never claims a user actor or gets erased by a parallel allowed ask", () => {
  const fixture = makeFixture()
  fixture.capture("call", { origin: "plugin", execution: "local", category: "extension" })
  const requests = [Permission.ID.create(), Permission.ID.create()]
  requests.forEach((id) =>
    Effect.runSync(
      fixture.observer.event(
        published(
          Permission.Event.Asked,
          {
            sessionID,
            id,
            action: "bash",
            resources: ["SECRET"],
            source: { type: "tool", messageID: assistantMessageID, id: "call" },
          },
          100,
        ),
      ),
    ),
  )
  requests.forEach((requestID, index) =>
    Effect.runSync(
      fixture.observer.event(
        published(
          Permission.Event.Replied,
          {
            sessionID,
            requestID,
            reply: index === 0 ? "reject" : "once",
          },
          110,
        ),
      ),
    ),
  )
  Effect.runSync(
    fixture.observer.event(
      published(
        SessionEvent.Tool.Failed,
        {
          sessionID,
          assistantMessageID,
          id: "call",
          executed: false,
          error: { type: "aborted", message: "SECRET" },
        },
        120,
      ),
    ),
  )
  expect(fixture.marks.at(-1)?.[2]).toMatchObject({ approval: "rejected", result_family: "cancelled" })
  expect(JSON.stringify(fixture.marks)).not.toMatch(/SECRET|user_rejected/)
})

test("denied nested gates do not misclassify a recovered success or unrelated terminal failure", () => {
  const fixture = makeFixture()
  ;["success", "failure", "blocked"].forEach((id) => {
    fixture.capture(id, { origin: "builtin", execution: "local", category: "code_execution" })
    Effect.runSync(
      fixture.observer.permissionDecision({
        sessionID,
        action: "read",
        resources: ["SECRET"],
        effect: "deny",
        origin: "rules",
        source: { type: "tool", messageID: assistantMessageID, id },
      }),
    )
    Effect.runSync(
      fixture.observer.event(
        id === "success"
          ? published(
              SessionEvent.Tool.Success,
              { sessionID, assistantMessageID, id, executed: false, content: [{ type: "text", text: "SECRET" }] },
              100,
            )
          : published(
              SessionEvent.Tool.Failed,
              {
                sessionID,
                assistantMessageID,
                id,
                executed: false,
                error: { type: id === "blocked" ? "permission.rejected" : "tool.execution", message: "SECRET" },
              },
              100,
            ),
      ),
    )
  })
  expect(fixture.marks.filter((mark) => mark[1] === "opencode.tool.completed").map((mark) => mark[2])).toMatchObject([
    { approval: "denied", result_family: "success", outcome: "success" },
    { approval: "denied", result_family: "error", outcome: "failed" },
    { approval: "denied", result_family: "blocked", outcome: "failed" },
  ])
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})
