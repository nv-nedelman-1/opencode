import { describe, expect, test } from "bun:test"
import type { SessionHttpCall, SessionMiddlewares } from "@opencode/plugin/effect/session"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Session } from "@opencode/schema/session"
import { Cause, Effect, Exit } from "effect"
import type { ScopeHandle } from "nemo-relay-node"
import { RelayHost } from "../src/host"
import { RelayModel } from "../src/model"

interface RequestProjection {
  readonly headers: Record<string, string>
  readonly content: unknown
}

type Bridge = (
  input: RequestProjection,
  next: (input: RequestProjection, signal: AbortSignal) => Promise<unknown>,
) => Promise<unknown>

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
) => {
  const parent = { uuid: "scope_model_unit" } as ScopeHandle
  const runtime: RelayHost.Runtime = {
    relay: {
      llmCallExecuteAsync: (_name: string, input: RequestProjection, callback: Parameters<Bridge>[1]) =>
        bridge(input, callback),
    } as unknown as RelayHost.Relay,
    scope: () => Effect.succeed(parent),
    open: () => parent,
    admit: () => true,
    mark: () => {},
    close: () => {},
    lease: () =>
      Effect.succeed({
        parent,
        run: (callback) => callback(),
        onCancel: () => {},
        release: () => {},
      }),
  }
  return Effect.runPromiseExit(RelayModel.middleware(runtime, () => Effect.succeed(undefined))(input, next), { signal })
}

const forward: Bridge = (input, next) => next(input, new AbortController().signal)

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
    const exit = await run(call(), forward, () =>
      Effect.sync(() => {
        state.calls++
        return new Response(new ReadableStream({ start: (controller) => controller.error(reset) }))
      }),
    )
    expect(state.calls).toBe(1)
    expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBe(reset)
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
})
