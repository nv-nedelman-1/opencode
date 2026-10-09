import { expect, test } from "bun:test"
import type { SessionContext } from "@opencode/plugin/effect/session"
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
  const observer = RelayObserve.make({
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
  })
  return { observer, marks, opened, closed }
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
  expect(fixture.marks.map((mark) => mark[2])).toEqual([
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
  expect(fixture.marks[4][2]).toEqual({
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

  expect(fixture.marks).toHaveLength(2)
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
  expect(fixture.marks[2][2]).toEqual({
    count: 1,
    outcome: "cancelled",
    accounting_source: "host_step",
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
  ].forEach((event) => Effect.runSync(fixture.observer.event(event)))

  expect(
    fixture.marks.filter((mark) => mark[1] !== "opencode.tool.completed").map((mark) => [mark[1], mark[2]]),
  ).toEqual([
    ["opencode.skill.activated", { count: 1, source: "user", character_count: 6 }],
    ["opencode.skill.tool.requested", { count: 1 }],
    ["opencode.skill.tool.completed", { count: 1, category: "skill", outcome: "success", provider_executed: false }],
    ["opencode.skill.catalog.updated", { count: 1 }],
    ["opencode.mcp.status.changed", { count: 1 }],
  ])
  expect(fixture.marks.slice(-2).map((mark) => mark[0])).toEqual([undefined, undefined])
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("provider tools and concurrent host tools get distinct nonadditive host summaries", () => {
  const fixture = makeFixture()
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
    [sessionID, "opencode.tool.completed", { count: 1, category: "web", outcome: "success", provider_executed: true }],
    [
      sibling,
      "opencode.tool.completed",
      { count: 1, category: "terminal", outcome: "cancelled", provider_executed: false },
    ],
  ])
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})

test("V2 subagent tool calls and their permissions are classified as delegation", () => {
  const fixture = makeFixture()
  const permission = Permission.ID.create()
  ;[
    published(SessionEvent.Tool.Input.Started, { sessionID, assistantMessageID, id: "call", name: "subagent" }, 100),
    published(Permission.Event.Asked, { sessionID, id: permission, action: "subagent", resources: ["SECRET"] }, 110),
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
    { count: 1, category: "delegation", outcome: "success", provider_executed: false },
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
  expect(fixture.marks[4][2]).toEqual({ count: 1, resolution: "cancelled" })
  expect(JSON.stringify(fixture.marks)).not.toContain("SECRET")
})
