import { expect, test } from "bun:test"
import type { SessionContext } from "@opencode/plugin/effect/session"
import { Agent } from "@opencode/schema/agent"
import { Event } from "@opencode/schema/event"
import { Model } from "@opencode/schema/model"
import { Money } from "@opencode/schema/money"
import { Provider } from "@opencode/schema/provider"
import { RelativePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
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

const makeFixture = () => {
  const marks: Parameters<RelayHost.Runtime["mark"]>[] = []
  const opened: Parameters<RelayHost.Runtime["open"]>[] = []
  const closed: Parameters<RelayHost.Runtime["close"]>[] = []
  const observer = RelayObserve.make({
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
    delay_ms: 50,
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
