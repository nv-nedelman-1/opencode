import { describe, expect } from "bun:test"
import { OpenAIChat } from "@opencode/ai/protocols"
import { Agent } from "@opencode/schema/agent"
import { Money } from "@opencode/schema/money"
import { Session } from "@opencode/schema/session"
import { Location } from "@opencode/core/location"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Project } from "@opencode/core/project"
import { AbsolutePath } from "@opencode/core/schema"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { DateTime, Effect } from "effect"
import { HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { testEffect } from "./lib/effect"
import { PluginTestLayer } from "./plugin/fixture"

const it = testEffect(PluginTestLayer)

const session = Session.Info.make({
  id: Session.ID.make("ses_middleware"),
  projectID: Project.ID.global,
  cost: Money.USD.zero,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
  location: Location.Ref.make({ directory: AbsolutePath.make("/project") }),
})
const model = SessionRunnerModel.resolved(OpenAIChat.route.model({ id: "gpt-5.5", provider: "test" }), {
  capabilities: { tools: true, input: ["text"], output: ["text"] },
  cost: [],
  limit: { context: 200_000, output: 32_000 },
})
const transport = SessionModelTransport.Service.of({
  bind: () => ({ execute: () => Effect.die("unused WebSocket execution") }),
  close: () => Effect.void,
  closeAll: Effect.void,
})

const prepare = Effect.gen(function* () {
  const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))
  return yield* requests.primary({ session, agent: Agent.ID.make("build"), model, system: [], messages: [] })
})

const request = () =>
  HttpClientRequest.post("https://example.test/v1/chat/completions").pipe(
    HttpClientRequest.bodyText(JSON.stringify({ model: "gpt-5.5" }), "application/json"),
  )

describe("SessionModelRequest HTTP middleware", () => {
  it.effect("wraps the physical request between the http.request and http.response hooks", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      const seen: string[] = []
      yield* hooks.register("session", "http.request", (event) =>
        Effect.sync(() => {
          seen.push("http.request")
          event.request.headers.set("x-hook", "request")
        }),
      )
      yield* hooks.use("session", "http", (call, next) =>
        Effect.gen(function* () {
          seen.push(`middleware:${call.kind}:${call.protocol}:${call.request.headers.get("x-hook")}`)
          const body: Record<string, unknown> = JSON.parse(yield* Effect.promise(() => call.request.clone().text()))
          const response = yield* next(
            new Request(call.request, { body: JSON.stringify({ ...body, rewritten: true }) }),
          )
          return new Response(`wrapped:${yield* Effect.promise(() => response.text())}`, response)
        }),
      )
      yield* hooks.register("session", "http.response", (event) =>
        Effect.promise(async () => {
          seen.push(`http.response:${await event.response.clone().text()}`)
        }),
      )
      const http = (yield* prepare).options.http
      if (!http) throw new Error("Expected HTTP middleware")
      const response = yield* http(request(), (sent) =>
        Effect.gen(function* () {
          const web = yield* HttpClientRequest.toWeb(sent)
          seen.push(`network:${sent.headers["x-hook"]}:${yield* Effect.promise(() => web.text())}`)
          return HttpClientResponse.fromWeb(sent, new Response("provider", { status: 200 }))
        }),
      )

      expect(yield* response.text).toBe("wrapped:provider")
      expect(seen).toEqual([
        "http.request",
        "middleware:primary:openai-chat:request",
        'network:request:{"model":"gpt-5.5","rewritten":true}',
        "http.response:wrapped:provider",
      ])
    }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )

  it.effect("nests middlewares in registration order", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      const seen: string[] = []
      const trace = (name: string) =>
        hooks.use("session", "http", (call, next) =>
          Effect.gen(function* () {
            seen.push(`${name}:before`)
            const response = yield* next(call.request)
            seen.push(`${name}:after`)
            return response
          }),
        )
      yield* trace("outer")
      yield* trace("inner")
      const http = (yield* prepare).options.http
      if (!http) throw new Error("Expected HTTP middleware")
      yield* http(request(), (sent) =>
        Effect.sync(() => {
          seen.push("network")
          return HttpClientResponse.fromWeb(sent, new Response("{}", { status: 200 }))
        }),
      )

      expect(seen).toEqual(["outer:before", "inner:before", "network", "inner:after", "outer:after"])
    }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )

  it.effect("answers locally when a middleware does not call next", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      yield* hooks.use("session", "http", () =>
        Effect.succeed(new Response(JSON.stringify({ error: "blocked" }), { status: 400 })),
      )
      const http = (yield* prepare).options.http
      if (!http) throw new Error("Expected HTTP middleware")
      const response = yield* http(request(), () => Effect.die("the provider must not be called"))

      expect(response.status).toBe(400)
      expect(yield* response.text).toBe('{"error":"blocked"}')
    }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )

  it.effect("skips middlewares scoped to another provider", () =>
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      yield* hooks.use("session", "http", () => Effect.die("unused middleware"), { providerID: "other" })

      expect((yield* prepare).options.http).toBeUndefined()
    }).pipe(Effect.provideService(SessionModelTransport.Service, transport)),
  )
})
