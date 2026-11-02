import { describe, expect } from "bun:test"
import { Agent } from "@opencode/core/agent"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Image } from "@opencode/core/image"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Session } from "@opencode/core/session"
import { SessionMessage } from "@opencode/core/session/message"
import { Tool } from "@opencode/core/tool"
import type { Info } from "@opencode/schema/tool"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Cause, Effect, Exit, Layer, Schema } from "effect"
import { testEffect } from "./lib/effect"

const registryLayer = AppNodeBuilder.build(LayerNode.group([Tool.node, PluginHooks.node]), [
  Image.node.replace(Layer.mock(Image.Service, { normalize: (_resource, content) => Effect.succeed(content) })),
])
const it = testEffect(registryLayer)

const call = (input: unknown): Parameters<Tool.Snapshot["execute"]>[0] => ({
  sessionID: Session.ID.make("ses_tool_middleware"),
  agent: Agent.ID.make("build"),
  messageID: SessionMessage.ID.make("msg_tool_middleware"),
  call: { type: "tool-call", id: "call-echo", name: "echo", input },
})

const echo = (input: { readonly text: string }) => Effect.succeed({ content: input.text })

const register = (execute: Info["execute"] = echo) =>
  Effect.gen(function* () {
    const service = yield* Tool.Service
    yield* service.transform((editor) =>
      editor.add({
        name: "echo",
        description: "Echo text",
        input: Schema.Struct({ text: Schema.String }),
        execute,
        options: { codemode: false },
      }),
    )
    return service
  })

describe("Tool execute middleware", () => {
  it.effect("wraps execution after execute.before hooks and validates rewritten input", () =>
    Effect.gen(function* () {
      const service = yield* register()
      const hooks = yield* PluginHooks.Service
      const seen: unknown[] = []
      yield* hooks.register("tool", "execute.before", (event) =>
        Effect.sync(() => {
          event.input = { text: "from hook" }
        }),
      )
      yield* hooks.use("tool", "execute", (execution, next) =>
        Effect.gen(function* () {
          seen.push({ tool: execution.tool, id: execution.id, input: execution.input })
          return yield* next({ text: "from middleware" })
        }),
      )
      const snapshot = yield* service.snapshot()

      expect(yield* snapshot.execute(call({ text: "from model" }))).toEqual({
        content: [{ type: "text", text: "from middleware" }],
      })
      expect(seen).toEqual([{ tool: "echo", id: "call-echo", input: { text: "from hook" } }])
    }),
  )

  it.effect("rejects rewritten input that fails the tool schema", () =>
    Effect.gen(function* () {
      const service = yield* register()
      const hooks = yield* PluginHooks.Service
      yield* hooks.use("tool", "execute", (_execution, next) => next({ text: 1 }))
      const snapshot = yield* service.snapshot()
      const failure = yield* snapshot.execute(call({ text: "ok" })).pipe(Effect.flip)

      expect(failure).toBeInstanceOf(Tool.Error)
      expect(failure.message).toContain('Invalid arguments for tool "echo"')
    }),
  )

  it.effect("nests middlewares in registration order and can answer without running the tool", () =>
    Effect.gen(function* () {
      const service = yield* register(() => Effect.die("the tool must not run"))
      const hooks = yield* PluginHooks.Service
      const seen: string[] = []
      yield* hooks.use("tool", "execute", (_execution, next) =>
        Effect.gen(function* () {
          seen.push("outer:before")
          const result = yield* next({ text: "unused" })
          seen.push("outer:after")
          return result
        }),
      )
      yield* hooks.use("tool", "execute", () =>
        Effect.sync(() => {
          seen.push("inner")
          return { content: "cached" }
        }),
      )
      const snapshot = yield* service.snapshot()

      expect(yield* snapshot.execute(call({ text: "ok" }))).toEqual({ content: [{ type: "text", text: "cached" }] })
      expect(seen).toEqual(["outer:before", "inner", "outer:after"])
    }),
  )

  it.effect("propagates defects from the tool through a middleware unchanged", () =>
    Effect.gen(function* () {
      const defect = new Error("declined")
      const service = yield* register(() => Effect.die(defect))
      const hooks = yield* PluginHooks.Service
      yield* hooks.use("tool", "execute", (execution, next) => next(execution.input))
      const snapshot = yield* service.snapshot()
      const exit = yield* snapshot.execute(call({ text: "ok" })).pipe(Effect.exit)

      expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBe(defect)
    }),
  )
})
