import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import type { SessionHttpCall, SessionMiddlewares } from "@opencode/plugin/effect/session"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Session } from "@opencode/schema/session"
import { Cause, Effect, Exit } from "effect"
import type { ScopeHandle } from "nemo-relay-node"
import relay from "nemo-relay-node"
import plugin from "nemo-relay-node/plugin"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { RelayHost } from "../src/host"
import { RelayModel } from "../src/model"

let directory: string
let activation: Awaited<ReturnType<typeof plugin.initialize>>
beforeAll(async () => {
  directory = await mkdtemp(path.join(os.tmpdir(), "opencode-relay-model-"))
  const pluginsToml = path.join(directory, "plugins.toml")
  await writeFile(pluginsToml, "version = 1\n")
  activation = await plugin.initialize(plugin.defaultConfig(), pluginsToml)
})
afterAll(async () => {
  await activation?.close()
  if (directory) await rm(directory, { recursive: true, force: true })
})

interface RequestProjection {
  readonly headers: Record<string, string>
  readonly content: unknown
}

type Bridge = (
  input: RequestProjection,
  next: (input: RequestProjection, signal: AbortSignal) => Promise<unknown>,
) => Promise<unknown>

const observations = () => ({
  marks: [] as { name: string; data: Record<string, unknown> }[],
  metadata: [] as unknown[],
  names: [] as string[],
  releases: 0,
  cancel: undefined as (() => void) | undefined,
})

const providerRequest = (signal?: AbortSignal) =>
  new Request("https://provider.test/model", { method: "POST", body: '{"prompt":"hi"}', signal })

const call = (request = providerRequest()): SessionHttpCall => ({
  sessionID: Session.ID.make("ses_model_unit"),
  agent: Agent.ID.make("build"),
  model: { providerID: Provider.ID.make("test"), id: Model.ID.make("model") },
  kind: "primary",
  protocol: "custom-json",
  request,
})

const host = (
  observed: ReturnType<typeof observations>,
  api: RelayHost.Relay,
  parent: ScopeHandle,
  execute: RelayHost.Operation["run"] = (callback) => callback(),
): RelayHost.Runtime => ({
  relay: api,
  scope: () => Effect.succeed(parent),
  open: () => parent,
  admit: () => true,
  mark: (_session, name, data) => observed.marks.push({ name, data: data as Record<string, unknown> }),
  close: () => {},
  lease: () =>
    Effect.succeed({
      parent,
      run: execute,
      mark: (name, data) => observed.marks.push({ name, data: data as Record<string, unknown> }),
      onCancel: (cancel) => {
        observed.cancel = cancel
      },
      release: () => {
        observed.releases++
      },
    }),
})

const run = (
  input: SessionHttpCall,
  bridge: Bridge,
  next: Parameters<SessionMiddlewares["http"]>[1],
  signal?: AbortSignal,
  observed = observations(),
) => {
  const runtime = host(
    observed,
    {
      OpenAIChatCodec: relay.OpenAIChatCodec,
      OpenAIResponsesCodec: relay.OpenAIResponsesCodec,
      AnthropicMessagesCodec: relay.AnthropicMessagesCodec,
      GeminiGenerateContentCodec: relay.GeminiGenerateContentCodec,
      llmCallExecuteAsync: (
        name: string,
        input: RequestProjection,
        callback: Parameters<Bridge>[1],
        _parent: unknown,
        _attributes: unknown,
        _data: unknown,
        metadata: unknown,
      ) => {
        observed.names.push(name)
        observed.metadata.push(metadata)
        return bridge(input, callback)
      },
    } as unknown as RelayHost.Relay,
    { uuid: "scope_model_unit" } as ScopeHandle,
  )
  return Effect.runPromiseExit(RelayModel.middleware(runtime, () => Effect.succeed(undefined))(input, next), { signal })
}

const forward: Bridge = (input, next) => next(input, new AbortController().signal)

const native = (observed: ReturnType<typeof observations>) => {
  const stack = relay.createScopeStack()
  const parent = relay.withScopeStack(stack, () => relay.pushScope("physical-attempt-test", 0)) as ScopeHandle
  const runtime = host(
    observed,
    {
      ...relay,
      llmCallExecuteAsync: (...args: Parameters<typeof relay.llmCallExecuteAsync>) => {
        const result = relay.llmCallExecuteAsync(...args)
        observed.names.push(args[0])
        return result
      },
      llmStreamCallExecute: (...args: Parameters<typeof relay.llmStreamCallExecute>) => {
        // Unsupported codec-instance probes throw before dispatch and are not accepted managed calls.
        const result = relay.llmStreamCallExecute(...args)
        observed.names.push(args[0])
        return result
      },
    } as unknown as RelayHost.Relay,
    parent,
    <A>(callback: () => A) => relay.withScopeStack(stack, callback) as A,
  )
  return { runtime, parent, close: () => relay.withScopeStack(stack, () => relay.popScope(parent)) }
}

describe("Relay model middleware contracts", () => {
  test.each([
    ["openai-chat", "openai.chat_completions"],
    ["openai-compatible-chat", "openai.chat_completions"],
    ["openai-responses", "openai.responses"],
    ["open-responses", "openai.responses"],
    ["openai-compatible-responses", "openai.responses"],
    ["xai-responses", "openai.responses"],
    ["anthropic-messages", "anthropic.messages"],
    ["gemini", "gemini"],
    ["ai-sdk", "ai-sdk"],
    ["custom-json", "custom-json"],
  ])("names managed %s calls by protocol while retaining configured provider attribution", async (protocol, name) => {
    const observed = observations()
    const exit = await run(
      { ...call(), protocol },
      forward,
      () => Effect.succeed(new Response("{}")),
      undefined,
      observed,
    )
    expect(Exit.isSuccess(exit)).toBe(true)
    expect(observed.names).toEqual([name])
    expect(observed.metadata[0]).toMatchObject({ provider_name: "test", "opencode.protocol": protocol })
  })

  test("keeps Request options, credentials, and its original abort signal", async () => {
    const abort = new AbortController()
    const request = new Request("https://provider.test/model?key=private-query-key", {
      method: "POST",
      body: '{"prompt":"hi"}',
      signal: abort.signal,
      headers: { authorization: "Bearer private-header-key", "x-route": "original" },
      credentials: "include",
      cache: "no-store",
      redirect: "manual",
      referrerPolicy: "no-referrer",
      keepalive: true,
    })
    const sent: Request[] = []
    const projected: RequestProjection[] = []
    const exit = await run(
      call(request),
      async (input, next) => {
        projected.push(input)
        return next(
          { ...input, headers: { ...input.headers, authorization: "plugin-cannot-replace-credentials" } },
          new AbortController().signal,
        )
      },
      (input) =>
        Effect.sync(() => {
          sent.push(input)
          return new Response("{}")
        }),
    )
    expect(Exit.isSuccess(exit)).toBe(true)
    const options = ["credentials", "cache", "redirect", "referrerPolicy", "keepalive"] as const
    expect(Object.fromEntries(options.map((key) => [key, sent[0][key]]))).toEqual(
      Object.fromEntries(options.map((key) => [key, request[key]])),
    )
    expect(sent[0].headers.get("authorization")).toBe("Bearer private-header-key")
    expect(sent[0].url).toBe(request.url)
    expect(JSON.stringify(projected)).not.toContain("private-")
    abort.abort()
    expect(sent[0].signal.aborted).toBe(true)
  })

  test("does not replay an inference after the provider response body fails", async () => {
    const reset = new Error("body reset")
    const state = { calls: 0 }
    const observed = observations()
    const exit = await run(
      call(),
      forward,
      () =>
        Effect.sync(() => {
          state.calls++
          return new Response(new ReadableStream({ start: (controller) => controller.error(reset) }))
        }),
      undefined,
      observed,
    )
    expect(state.calls).toBe(1)
    expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBe(reset)
    expect(observed.marks.at(-1)?.data).toMatchObject({ outcome: "failed", error_category: "body" })
    expect(observed.marks.some((mark) => mark.name === "opencode.llm.first_output")).toBe(false)
  })

  test.each([204, 205, 304])("preserves the null body required for HTTP %s", async (status) => {
    const exit = await run(call(), forward, () => Effect.succeed(new Response(null, { status })))
    expect(Exit.isSuccess(exit)).toBe(true)
    if (!Exit.isSuccess(exit)) throw new Error("Expected provider response")
    expect(exit.value.status).toBe(status)
    expect(exit.value.body).toBe(null)
  })

  test.each(["serial", "concurrent"])("rejects %s repeated inference dispatch", async (order) => {
    const state = { calls: 0, error: "" }
    const bridge: Bridge = async (input, next) => {
      const signal = new AbortController().signal
      const first = next(input, signal)
      if (order === "serial") await first
      await next(input, signal).catch((error: Error) => {
        state.error = error.message
      })
      return first
    }
    const exit = await run(call(), bridge, () =>
      Effect.sync(() => {
        state.calls++
        return new Response("{}")
      }),
    )
    expect(Exit.isSuccess(exit)).toBe(true)
    expect(state.calls).toBe(1)
    expect(state.error).toContain("at most once")
  })

  test("preserves transport defects and does not bypass selected plugin failures", async () => {
    const defect = new Error("transport defect")
    const died = await run(call(), forward, () => Effect.die(defect))
    const state = { calls: 0 }
    const failed = await run(
      call(),
      async () => {
        throw new Error("private-plugin-details")
      },
      () =>
        Effect.sync(() => {
          state.calls++
          return new Response("{}")
        }),
    )
    expect(Exit.isFailure(died) && Cause.squash(died.cause)).toBe(defect)
    expect(Exit.isFailure(failed)).toBe(true)
    expect(state.calls).toBe(0)
    expect(Exit.isFailure(failed) && String(Cause.squash(failed.cause))).not.toContain("private-plugin-details")
  })

  test("signed Bedrock requests bypass mutation and preserve the original request", async () => {
    const input = { ...call(), protocol: "bedrock-converse" }
    const sent: Request[] = []
    const exit = await run(
      input,
      async () => {
        throw new Error("must not manage signed wire data")
      },
      (request) =>
        Effect.sync(() => {
          sent.push(request)
          return new Response("{}")
        }),
    )
    expect(Exit.isSuccess(exit)).toBe(true)
    expect(sent).toEqual([input.request])
    expect(sent[0]).toBe(input.request)
  })

  test.each([200, 429, 529])(
    "observes one physical unary attempt and HTTP %s headers without deciding retry policy",
    async (status) => {
      const observed = observations()
      const input = { ...call(), protocol: "openai-chat" }
      await run(
        input,
        forward,
        () => Effect.succeed(new Response('{"choices":[{"message":{"content":"Hi"}}]}', { status })),
        undefined,
        observed,
      )
      expect(observed.marks.map((mark) => mark.name)).toEqual([
        "opencode.llm.provider_attempt.started",
        "opencode.llm.provider_attempt.headers",
        ...(status === 200 ? ["opencode.llm.first_output"] : []),
        "opencode.llm.provider_attempt.completed",
      ])
      expect(observed.marks[1].data).toMatchObject({ count: 1, http_status_class: `${Math.floor(status / 100)}xx` })
      expect(observed.marks.at(-1)?.data).toMatchObject({
        count: 1,
        streaming: false,
        outcome: status === 200 ? "success" : "failed",
        ...(status === 200 ? {} : { error_category: "http" }),
      })
      expect(observed.marks[1].data.duration_ms).toBeGreaterThanOrEqual(0)
      expect(observed.marks.at(-1)?.data.duration_ms).toBeGreaterThanOrEqual(
        observed.marks[1].data.duration_ms as number,
      )
      expect(observed.metadata[0]).toMatchObject({ provider_name: "test" })
      expect(observed.marks.every((mark) => !("retryable" in mark.data))).toBe(true)
    },
  )

  test("local plugin failures never claim a physical provider attempt", async () => {
    const observed = observations()
    await run(
      call(),
      async () => {
        throw new Error("selected plugin failed")
      },
      () => Effect.die("must not dispatch"),
      undefined,
      observed,
    )
    expect(observed.marks).toEqual([])
  })

  test("observes interrupted physical dispatch once without inventing headers or output", async () => {
    const observed = observations()
    const entered = Promise.withResolvers<void>()
    const abort = new AbortController()
    const pending = run(
      call(),
      forward,
      () => Effect.sync(() => entered.resolve()).pipe(Effect.andThen(Effect.never)),
      abort.signal,
      observed,
    )
    await entered.promise
    abort.abort()
    await pending
    expect(observed.marks.map((mark) => mark.name)).toEqual([
      "opencode.llm.provider_attempt.started",
      "opencode.llm.provider_attempt.completed",
    ])
    expect(observed.marks[1].data).toMatchObject({ outcome: "cancelled", error_category: "cancelled", count: 1 })
  })

  test("does not dispatch when Relay supplies an already aborted unary signal", async () => {
    const abort = new AbortController()
    abort.abort()
    let calls = 0
    const observed = observations()
    const exit = await run(
      call(),
      (input, next) => next(input, abort.signal),
      () =>
        Effect.sync(() => {
          calls++
          return new Response("{}")
        }),
      undefined,
      observed,
    )
    expect(calls).toBe(0)
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
    expect(observed.marks).toEqual([])
    expect(observed.releases).toBe(1)
  })

  test.each([false, true])(
    "drains a revoked provider without confusing replacement with caller cancellation (%s)",
    async (caller) => {
      const entered = Promise.withResolvers<void>()
      const cleaning = Promise.withResolvers<void>()
      const cleanup = Promise.withResolvers<void>()
      const requestAbort = new AbortController()
      const observed = observations()
      const pending = run(
        call(providerRequest(requestAbort.signal)),
        async (input, next) => {
          const revoke = new AbortController()
          void next(input, revoke.signal).catch(() => undefined)
          await entered.promise
          if (caller) requestAbort.abort()
          revoke.abort()
          return { choices: [{ message: { content: "replacement" } }] }
        },
        () =>
          Effect.sync(() => entered.resolve()).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Effect.promise(() => {
                cleaning.resolve()
                return cleanup.promise
              }),
            ),
          ),
        undefined,
        observed,
      )
      let settled = false
      void pending.then(() => {
        settled = true
      })
      await cleaning.promise
      try {
        await new Promise((resolve) => setTimeout(resolve, 0))
        expect(settled).toBe(false)
        expect(observed.releases).toBe(0)
      } finally {
        cleanup.resolve()
      }
      const exit = await pending
      if (caller) expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
      if (!caller) {
        expect(Exit.isSuccess(exit) && (await exit.value.json())).toEqual({
          choices: [{ message: { content: "replacement" } }],
        })
      }
      expect(observed.releases).toBe(1)
      expect(observed.marks.filter((mark) => mark.name === "opencode.llm.provider_attempt.completed")).toHaveLength(1)
    },
  )

  test("accepts native execution replacements after Relay revokes their unused continuation", async () => {
    for (let index = 0; index < 3; index++) {
      const observed = observations()
      const provider = native(observed)
      const entered = Promise.withResolvers<void>()
      let interrupted = false
      relay.registerLlmExecutionIntercept("model-contract-replacement", 100, async (input, _context, next) => {
        void Promise.resolve(next(input)).catch(() => undefined)
        await entered.promise
        return { choices: [{ message: { content: "replacement" } }] }
      })
      try {
        const exit = await Effect.runPromiseExit(
          RelayModel.middleware(provider.runtime, () => Effect.succeed(undefined))(call(), () =>
            Effect.sync(() => entered.resolve()).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  interrupted = true
                }),
              ),
            ),
          ),
        )
        expect(Exit.isSuccess(exit) && (await exit.value.json())).toEqual({
          choices: [{ message: { content: "replacement" } }],
        })
        expect(interrupted).toBe(true)
        expect(observed.releases).toBe(1)
      } finally {
        relay.deregisterLlmExecutionIntercept("model-contract-replacement")
        provider.close()
      }
    }
  })

  test.each(["request", "operation"] as const)(
    "%s cancellation wins over a replacement tail, even without a pending provider",
    async (source) => {
      for (const dispatched of [false, true]) {
        const observed = observations()
        const entered = Promise.withResolvers<void>()
        const tail = Promise.withResolvers<void>()
        const abort = new AbortController()
        const pending = run(
          call(providerRequest(abort.signal)),
          async (input, next) => {
            if (dispatched) await next(input, new AbortController().signal)
            entered.resolve()
            await tail.promise
            return { choices: [{ message: { content: "replacement" } }] }
          },
          () => Effect.succeed(Response.json({ choices: [] })),
          undefined,
          observed,
        )
        await entered.promise
        if (source === "request") abort.abort()
        if (source === "operation") observed.cancel?.()
        tail.resolve()
        const exit = await pending
        expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
        expect(observed.releases).toBe(1)
      }
    },
  )

  test.each(["relay", "request", "effect", "operation"] as const)(
    "combines %s cancellation with the actual unary Request and interrupts dispatch once",
    async (source) => {
      const observed = observations()
      const entered = Promise.withResolvers<void>()
      const relayAbort = new AbortController()
      const requestAbort = new AbortController()
      const effectAbort = new AbortController()
      const input = call(providerRequest(requestAbort.signal))
      const state: { request?: Request; calls: number; interrupted: boolean } = { calls: 0, interrupted: false }
      const pending = run(
        input,
        (input, next) => next(input, relayAbort.signal),
        (request) =>
          Effect.sync(() => {
            state.request = request
            state.calls++
            entered.resolve()
          }).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                state.interrupted = true
              }),
            ),
          ),
        effectAbort.signal,
        observed,
      )
      await entered.promise
      if (source === "relay") relayAbort.abort()
      if (source === "request") requestAbort.abort()
      if (source === "effect") effectAbort.abort()
      if (source === "operation") observed.cancel?.()
      // A bounded cleanup also lets the pre-fix regression fail without leaving a pending provider callback.
      const propagated = state.request?.signal.aborted
      if (!propagated) effectAbort.abort()
      const exit = await pending
      expect(propagated).toBe(true)
      expect(state.interrupted).toBe(true)
      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
      expect(state.calls).toBe(1)
      expect(observed.releases).toBe(1)
      expect(observed.marks.filter((mark) => mark.name === "opencode.llm.provider_attempt.completed")).toHaveLength(1)
      expect(observed.marks.at(-1)?.data).toMatchObject({ outcome: "cancelled", error_category: "cancelled" })
      expect(requestAbort.signal.aborted).toBe(source === "request")
    },
  )

  test("Relay cancellation interrupts unary body consumption without replaying dispatch", async () => {
    const observed = observations()
    const entered = Promise.withResolvers<void>()
    const parked = Promise.withResolvers<void>()
    const relayAbort = new AbortController()
    const effectAbort = new AbortController()
    const state: { request?: Request; controller?: ReadableStreamDefaultController<Uint8Array>; calls: number } = {
      calls: 0,
    }
    const pending = run(
      call(),
      (input, next) => next(input, relayAbort.signal),
      (request) =>
        Effect.sync(() => {
          state.request = request
          state.calls++
          const body = new ReadableStream<Uint8Array>(
            {
              start(controller) {
                state.controller = controller
                request.signal.addEventListener(
                  "abort",
                  () => controller.error(new DOMException("Aborted", "AbortError")),
                  { once: true },
                )
              },
              pull() {
                entered.resolve()
                return parked.promise
              },
            },
            { highWaterMark: 0 },
          )
          return new Response(body)
        }),
      effectAbort.signal,
      observed,
    )
    await entered.promise
    relayAbort.abort()
    const propagated = state.request?.signal.aborted
    if (!propagated) effectAbort.abort()
    state.controller?.error(new DOMException("Cleanup", "AbortError"))
    parked.resolve()
    const exit = await pending
    expect(propagated).toBe(true)
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
    expect(state.calls).toBe(1)
    expect(observed.releases).toBe(1)
    expect(observed.marks.at(-1)?.data).toMatchObject({ outcome: "cancelled", error_category: "cancelled" })
  })

  test("interruption retains unary ownership until the managed native promise tail has drained", async () => {
    const observed = observations()
    const entered = Promise.withResolvers<void>()
    const tail = Promise.withResolvers<void>()
    const abort = new AbortController()
    const state = { settled: false }
    const actual = native(observed)
    relay.registerLlmExecutionIntercept("held-unary-tail", 0, async (request, _context, next) => {
      const response = await next(request)
      entered.resolve()
      await tail.promise
      return response
    })
    const pending = Effect.runPromiseExit(
      RelayModel.middleware(actual.runtime, () => Effect.succeed(undefined))(call(), () =>
        Effect.succeed(new Response("{}")),
      ),
      { signal: abort.signal },
    ).then((exit) => {
      state.settled = true
      return exit
    })
    await entered.promise
    abort.abort()
    await Bun.sleep(0)
    try {
      expect(state.settled).toBe(false)
      expect(observed.releases).toBe(0)
    } finally {
      tail.resolve()
      await pending
      relay.deregisterLlmExecutionIntercept("held-unary-tail")
      actual.close()
    }
    expect(observed.releases).toBe(1)
    const exit = await pending
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
    expect(observed.marks.filter((mark) => mark.name === "opencode.llm.provider_attempt.completed")).toHaveLength(1)
  })

  test.each(["success", "cancelled", "malformed"])(
    "native streaming bridge observes %s without counting empty first frames",
    async (outcome) => {
      const observed = observations()
      const actual = native(observed)
      relay.clearLastCallbackError()
      const input = {
        ...call(
          new Request("https://provider.test/chat", {
            method: "POST",
            body: '{"model":"model","messages":[{"role":"user","content":"hi"}],"stream":true}',
          }),
        ),
        protocol: "openai-chat",
      }
      const encode = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`
      const frames =
        encode({ choices: [{ delta: { role: "assistant", content: "" } }] }) +
        encode({ choices: [], usage: { total_tokens: 2 } }) +
        encode({ choices: [{ delta: { reasoning_content: "Think" } }] }) +
        encode({ choices: [{ delta: { content: "Hi" } }] })
      try {
        const response = await Effect.runPromise(
          RelayModel.middleware(actual.runtime, () => Effect.succeed(undefined))(input, () =>
            Effect.succeed(
              new Response(
                new ReadableStream({
                  start(controller) {
                    controller.enqueue(
                      new TextEncoder().encode(
                        frames +
                          (outcome === "malformed"
                            ? "data: invalid\n\n"
                            : outcome === "success"
                              ? "data: [DONE]\n\n"
                              : ""),
                      ),
                    )
                    if (outcome !== "cancelled") controller.close()
                  },
                }),
                { headers: { "content-type": "text/event-stream" } },
              ),
            ),
          ),
        )
        if (outcome === "cancelled") {
          const reader = response.body!.getReader()
          while (!new TextDecoder().decode((await reader.read()).value).includes("Think")) {}
          await reader.cancel()
          reader.releaseLock()
        }
        if (outcome !== "cancelled") await response.text()
        expect(observed.names).toEqual(["openai.chat_completions"])
        expect(observed.marks.filter((mark) => mark.name === "opencode.llm.provider_attempt.started")).toHaveLength(1)
        expect(observed.marks.filter((mark) => mark.name === "opencode.llm.provider_attempt.headers")).toHaveLength(1)
        expect(observed.marks.filter((mark) => mark.name === "opencode.llm.first_output")).toEqual([
          expect.objectContaining({
            data: expect.objectContaining({ output_kind: "reasoning", streaming: true, count: 1 }),
          }),
        ])
        expect(observed.marks.filter((mark) => mark.name === "opencode.llm.provider_attempt.completed")).toEqual([
          expect.objectContaining({
            data: expect.objectContaining({ outcome: outcome === "malformed" ? "failed" : outcome }),
          }),
        ])
        expect(relay.getLastCallbackError()).toBeNull()
      } finally {
        actual.close()
      }
    },
  )
})
