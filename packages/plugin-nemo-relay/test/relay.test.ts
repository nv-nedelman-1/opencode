import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { Tool } from "@opencode/schema/tool"
import type { SessionHttpCall } from "@opencode/plugin/effect/session"
import type { ToolExecution } from "@opencode/plugin/effect/tool"
import { Cause, Effect, Exit, Scope } from "effect"
import relay from "nemo-relay-node"
import observability from "nemo-relay-node/observability"
import plugin from "nemo-relay-node/plugin"
import { mkdtemp, readFile, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { RelayHost } from "../src/host"
import { RelayModel } from "../src/model"
import { RelayTool } from "../src/tool"

const sessionID = Session.ID.make("ses_relay_test")
const noParent = () => Effect.succeed(undefined)

let directory: string
let scope: Scope.Closeable
let runtime: RelayHost.Runtime

beforeAll(async () => {
  directory = await mkdtemp(path.join(os.tmpdir(), "opencode-relay-"))
  const config = plugin.defaultConfig()
  config.components = [
    observability.ComponentSpec({
      version: 4,
      atof: observability.atofConfig({
        enabled: true,
        sinks: [{ type: "file", output_directory: directory, filename: "events.jsonl", mode: "overwrite" }],
      }),
    }),
  ]
  scope = Effect.runSync(Scope.make())
  const acquired = await Effect.runPromise(RelayHost.acquire({ config }).pipe(Scope.provide(scope)))
  if (!acquired) throw new Error("Expected an active Relay runtime")
  runtime = acquired
})

afterAll(() => rm(directory, { recursive: true, force: true }))

const execution = (tool: string, input: unknown): ToolExecution => ({
  tool,
  sessionID,
  agent: Agent.ID.make("build"),
  messageID: SessionMessage.ID.make("msg_relay_test"),
  id: Tool.CallID.make(`call_${tool}`),
  input,
})

const runTool = (tool: string, input: unknown, next: Parameters<ReturnType<typeof RelayTool.middleware>>[1]) =>
  Effect.runPromiseExit(RelayTool.middleware(runtime, noParent)(execution(tool, input), next))

const chunks = [
  {
    id: "c1",
    object: "chat.completion.chunk",
    model: "m",
    choices: [{ index: 0, delta: { role: "assistant", content: "Hel" } }],
  },
  {
    id: "c1",
    object: "chat.completion.chunk",
    model: "m",
    choices: [{ index: 0, delta: { content: "lo" }, finish_reason: "stop" }],
  },
  {
    id: "c1",
    object: "chat.completion.chunk",
    model: "m",
    choices: [],
    usage: { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 },
  },
]

const sse = (items: ReadonlyArray<unknown>) =>
  new Response(items.map((item) => `data: ${JSON.stringify(item)}\n\n`).join("") + "data: [DONE]\n\n", {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  })

const call = (body: Record<string, unknown>): SessionHttpCall => ({
  sessionID,
  agent: Agent.ID.make("build"),
  model: { providerID: Provider.ID.make("test"), id: Model.ID.make("m") },
  kind: "primary",
  protocol: "openai-chat",
  request: new Request("https://provider.test/v1/chat/completions", {
    method: "POST",
    headers: { authorization: "Bearer secret", "content-type": "application/json" },
    body: JSON.stringify({ model: "m", messages: [{ role: "user", content: "hi" }], ...body }),
  }),
})

const runModel = (input: SessionHttpCall, next: (request: Request) => Effect.Effect<Response, Error>) =>
  Effect.runPromise(RelayModel.middleware(runtime, noParent)(input, next))

const events = (text: string) =>
  text
    .split("\n\n")
    .flatMap((block) => block.split("\n").filter((line) => line.startsWith("data: ")))
    .map((line) => line.slice("data: ".length))

describe("managed tool execution", () => {
  test("runs the tool through Relay and returns its result", async () => {
    const exit = await runTool("read", { path: "a.txt" }, (input) =>
      Effect.succeed({ content: `read ${JSON.stringify(input)}` }),
    )

    expect(exit).toEqual(Exit.succeed({ content: 'read {"path":"a.txt"}' }))
  })

  test("blocks a call a conditional-execution guardrail rejects", async () => {
    relay.registerToolConditionalExecutionGuardrail("test-block-shell", 10, (tool) =>
      tool === "shell" ? "shell commands are disabled" : null,
    )
    const ran: unknown[] = []
    const exit = await runTool("shell", { command: "rm -rf /" }, (input) =>
      Effect.sync(() => {
        ran.push(input)
        return { content: "ran" }
      }),
    ).finally(() => relay.deregisterToolConditionalExecutionGuardrail("test-block-shell"))

    expect(ran).toEqual([])
    expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toEqual(
      new Tool.Error({ message: "NeMo Relay blocked this tool call: shell commands are disabled" }),
    )
  })

  test("hands rewritten input to the tool", async () => {
    relay.registerToolRequestIntercept("test-rewrite-edit", 10, false, (tool, args) =>
      tool === "edit" ? { ...(args as Record<string, unknown>), path: "safe.txt" } : args,
    )
    const exit = await runTool("edit", { path: "unsafe.txt" }, (input) =>
      Effect.succeed({ content: JSON.stringify(input) }),
    ).finally(() => relay.deregisterToolRequestIntercept("test-rewrite-edit"))

    expect(exit).toEqual(Exit.succeed({ content: '{"path":"safe.txt"}' }))
  })

  test("re-raises the tool's own failures and defects unchanged", async () => {
    const failure = new Tool.Error({ message: "file not found" })
    const defect = new Error("declined")

    const failed = await runTool("read", {}, () => Effect.fail(failure))
    const died = await runTool("read", {}, () => Effect.die(defect))

    expect(Exit.isFailure(failed) && Cause.squash(failed.cause)).toBe(failure)
    expect(Exit.isFailure(died) && Cause.squash(died.cause)).toBe(defect)
  })
})

describe("managed model execution", () => {
  test("streams the provider response through Relay without exposing credentials", async () => {
    const seen: Array<Record<string, string>> = []
    relay.registerLlmRequestIntercept("test-observe-headers", 10, false, (args) => {
      seen.push((args.request as { headers: Record<string, string> }).headers)
      return { request: args.request, annotated: args.annotated }
    })
    const sent: Request[] = []
    const response = await runModel(call({ stream: true }), (request) =>
      Effect.sync(() => {
        sent.push(request)
        return sse(chunks)
      }),
    ).finally(() => relay.deregisterLlmRequestIntercept("test-observe-headers"))

    const data = events(await response.text())
    expect(response.status).toBe(200)
    expect(data.slice(0, -1).map((item) => JSON.parse(item))).toEqual(chunks)
    expect(data.at(-1)).toBe("[DONE]")
    expect(seen.length).toBe(1)
    expect(seen[0]?.authorization).toBeUndefined()
    expect(sent[0]?.headers.get("authorization")).toBe("Bearer secret")
    expect(sent[0]?.headers.get("traceparent")).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/)
  })

  test("answers a guardrail rejection locally with a final content-policy failure", async () => {
    relay.registerLlmConditionalExecutionGuardrail("test-block-prompt", 10, (request) =>
      JSON.stringify(request).includes("forbidden") ? "prompt violates policy" : null,
    )
    const sent: Request[] = []
    const response = await runModel(
      call({ stream: true, messages: [{ role: "user", content: "forbidden" }] }),
      (request) => Effect.sync(() => (sent.push(request), sse(chunks))),
    ).finally(() => relay.deregisterLlmConditionalExecutionGuardrail("test-block-prompt"))

    expect(sent).toEqual([])
    expect(response.status).toBe(400)
    expect(response.headers.get("x-should-retry")).toBe("false")
    expect(await response.json()).toEqual({
      error: {
        type: "nemo_relay_guardrail_rejected",
        code: "content_policy_violation",
        message: "NeMo Relay blocked this request: prompt violates policy",
      },
    })
  })

  test("sends the request a codec-aware request intercept rewrote", async () => {
    relay.registerLlmRequestIntercept("test-rewrite-request", 10, false, (args) => {
      const request = args.request as { headers: Record<string, string>; content: unknown }
      const annotated = args.annotated as { params?: Record<string, unknown> }
      return {
        request: { ...request, headers: { ...request.headers, "x-route": "relay" } },
        annotated: { ...annotated, params: { ...annotated.params, temperature: 0 } },
      }
    })
    const sent: Request[] = []
    await runModel(call({ stream: true }), (request) => Effect.sync(() => (sent.push(request), sse(chunks))))
      .then((response) => response.text())
      .finally(() => relay.deregisterLlmRequestIntercept("test-rewrite-request"))

    expect(sent[0]?.headers.get("x-route")).toBe("relay")
    expect(sent[0]?.headers.get("authorization")).toBe("Bearer secret")
    expect(await sent[0]?.json()).toMatchObject({ model: "m", temperature: 0 })
  })

  test("lets intercepts rewrite the raw body of protocols without a codec", async () => {
    relay.registerLlmRequestIntercept("test-rewrite-opaque", 10, false, (args) => {
      const request = args.request as { headers: Record<string, string>; content: Record<string, unknown> }
      return { request: { ...request, content: { ...request.content, safe_prompt: true } } }
    })
    const sent: Request[] = []
    await runModel({ ...call({ stream: true }), protocol: "mistral-chat" }, (request) =>
      Effect.sync(() => (sent.push(request), sse(chunks))),
    )
      .then((response) => response.text())
      .finally(() => relay.deregisterLlmRequestIntercept("test-rewrite-opaque"))

    expect(await sent[0]?.json()).toMatchObject({ model: "m", safe_prompt: true })
  })

  test("returns provider error responses unchanged", async () => {
    const response = await runModel(call({ stream: true }), () =>
      Effect.succeed(
        new Response('{"error":{"message":"slow down"}}', { status: 429, headers: { "retry-after": "1" } }),
      ),
    )

    expect(response.status).toBe(429)
    expect(response.headers.get("retry-after")).toBe("1")
    expect(await response.text()).toBe('{"error":{"message":"slow down"}}')
  })

  test("errors the response body with the provider's own mid-stream failure", async () => {
    const reset = new Error("stream reset by peer")
    const encoder = new TextEncoder()
    const state = { sent: false }
    const flaky = {
      ...call({ stream: true }),
      model: { providerID: Provider.ID.make("flaky"), id: Model.ID.make("m") },
    }
    const response = await runModel(flaky, () =>
      Effect.succeed(
        new Response(
          new ReadableStream<Uint8Array>({
            pull(controller) {
              if (state.sent) return controller.error(reset)
              state.sent = true
              controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunks[0])}\n\n`))
            },
          }),
          { status: 200, headers: { "content-type": "text/event-stream" } },
        ),
      ),
    )

    expect(response.status).toBe(200)
    await expect(response.text()).rejects.toThrow("stream reset by peer")
  })

  test("re-raises transport failures from the provider call", async () => {
    const failure = new Error("connection reset")
    const exit = await Effect.runPromiseExit(
      RelayModel.middleware(runtime, noParent)(call({ stream: true }), () => Effect.fail(failure)),
    )

    expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBe(failure)
  })

  test("manages non-streaming requests", async () => {
    const body =
      '{"id":"c2","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}'
    const response = await runModel(call({}), () =>
      Effect.succeed(new Response(body, { status: 200, headers: { "content-type": "application/json" } })),
    )

    expect(response.status).toBe(200)
    expect(await response.text()).toBe(body)
  })
})

const recorded = async () =>
  (await readFile(path.join(directory, "events.jsonl"), "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line))

describe("recorded telemetry", () => {
  test("parents managed calls under the session scope and closes it with its outcome", async () => {
    runtime.close(sessionID, "success")
    await Effect.runPromise(Scope.close(scope, Exit.void))
    const records = await recorded()
    const session = records.find((event) => event.kind === "scope" && event.name === "opencode.session")
    const ends = records.filter((event) => event.kind === "scope" && event.scope_category === "end")

    expect(session?.metadata?.["opencode.session_id"]).toBe(sessionID)
    expect(ends.find((event) => event.name === "opencode.session")?.metadata?.["otel.status_code"]).toBe("OK")
    expect(
      records
        .filter((event) => event.kind === "scope" && ["read", "edit", "test"].includes(event.name))
        .every((event) => event.parent_uuid === session?.uuid),
    ).toBe(true)
    expect(ends.some((event) => event.name === "test" && event.metadata?.["opencode.request_kind"] === "primary")).toBe(
      true,
    )
  })

  // Bindings without failStream can only end a pushed stream cleanly.
  test.if("failStream" in relay)("records a provider stream that failed midway as a failed LLM call", async () => {
    const end = (await recorded()).find(
      (event) => event.kind === "scope" && event.scope_category === "end" && event.name === "flaky",
    )

    expect(end?.metadata?.["otel.status_code"]).toBe("ERROR")
    expect(end?.metadata?.["otel.status_description"]).toContain("stream reset by peer")
  })
})
