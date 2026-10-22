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
  releases: 0,
})

const call = (
  request = new Request("https://provider.test/model", { method: "POST", body: '{"prompt":"hi"}' }),
): SessionHttpCall => ({
  sessionID: Session.ID.make("ses_model_unit"),
  agent: Agent.ID.make("build"),
  model: { providerID: Provider.ID.make("test"), id: Model.ID.make("model") },
  kind: "primary",
  protocol: "custom-json",
  request,
})

const run = (
  input: SessionHttpCall,
  bridge: Bridge,
  next: Parameters<SessionMiddlewares["http"]>[1],
  signal?: AbortSignal,
  observed = observations(),
) => {
  const parent = { uuid: "scope_model_unit" } as ScopeHandle
  const runtime: RelayHost.Runtime = {
    relay: {
      OpenAIChatCodec: relay.OpenAIChatCodec,
      llmCallExecuteAsync: (
        _name: string,
        input: RequestProjection,
        callback: Parameters<Bridge>[1],
        _parent: unknown,
        _attributes: unknown,
        _data: unknown,
        metadata: unknown,
      ) => {
        observed.metadata.push(metadata)
        return bridge(input, callback)
      },
    } as unknown as RelayHost.Relay,
    scope: () => Effect.succeed(parent),
    open: () => parent,
    admit: () => true,
    mark: (_session, name, data) => observed.marks.push({ name, data: data as Record<string, unknown> }),
    close: () => {},
    lease: () =>
      Effect.succeed({
        parent,
        run: (callback) => callback(),
        onCancel: () => {},
        release: () => {
          observed.releases++
        },
      }),
  }
  return Effect.runPromiseExit(RelayModel.middleware(runtime, () => Effect.succeed(undefined))(input, next), { signal })
}

const forward: Bridge = (input, next) => next(input, new AbortController().signal)

const native = (observed: ReturnType<typeof observations>) => {
  const stack = relay.createScopeStack()
  const parent = relay.withScopeStack(stack, () => relay.pushScope("physical-attempt-test", 0)) as ScopeHandle
  const runtime: RelayHost.Runtime = {
    relay: relay as unknown as RelayHost.Relay,
    scope: () => Effect.succeed(parent),
    open: () => parent,
    admit: () => true,
    close: () => {},
    mark: (_session, name, data) => observed.marks.push({ name, data: data as Record<string, unknown> }),
    lease: () =>
      Effect.succeed({
        parent,
        run: <A>(callback: () => A) => relay.withScopeStack(stack, callback) as A,
        onCancel: () => {},
        release: () => {
          observed.releases++
        },
      }),
  }
  return { runtime, parent, close: () => relay.withScopeStack(stack, () => relay.popScope(parent)) }
}

describe("Relay model middleware contracts", () => {
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
      } finally {
        actual.close()
      }
    },
  )
})
