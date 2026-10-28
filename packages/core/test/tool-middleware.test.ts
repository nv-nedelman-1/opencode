import { describe, expect } from "bun:test"
import { Agent } from "@opencode/core/agent"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Image } from "@opencode/core/image"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Session } from "@opencode/core/session"
import { SessionMessage } from "@opencode/core/session/message"
import { Tool } from "@opencode/core/tool"
import type { Info } from "@opencode/schema/tool"
import type { ToolProvenance } from "@opencode/plugin/effect/tool"
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
  it.effect("captures registration provenance across overrides, updates, disposal and request aliases", () =>
    Effect.gen(function* () {
      const service = yield* Tool.Service
      const hooks = yield* PluginHooks.Service
      const seen: ToolProvenance[] = []
      const terminal: ToolProvenance[] = []
      yield* hooks.use("tool", "execute", (event, next) => {
        seen.push(event.provenance!)
        return next(event.input)
      })
      yield* hooks.register("tool", "execute.after", (event) =>
        Effect.sync(() => {
          terminal.push(event.provenance!)
        }),
      )
      const definition = {
        name: "read",
        description: "A synthetic MCP tool whose name resembles a built-in",
        input: Schema.Struct({ text: Schema.String }),
        execute: echo,
        options: { codemode: false },
      } satisfies Info
      const mcp = { origin: "mcp", execution: "mcp", category: "extension" } as const
      yield* service.transform((editor) => editor.add(definition), mcp)
      const original = yield* service.snapshot()
      const replacement = yield* service.transform(
        (editor) => editor.add(definition),
        Tool.provenance({ type: "sdk" }, "opencode.tool.read"),
      )
      const selected = yield* service.snapshot()
      const input = {
        ...call({ text: "unchanged" }),
        call: { ...call({ text: "unchanged" }).call, name: "read" },
      }
      yield* original.execute(input)
      yield* selected.execute(input)
      yield* replacement.dispose
      yield* service.transform((editor) =>
        editor.update("read", (tool) => {
          tool.description = "Updated"
        }),
      )
      const updated = yield* service.snapshot()
      yield* updated.execute(input)
      yield* service.transform(
        (editor) =>
          editor.update("read", (tool) => {
            tool.execute = () => Effect.succeed({ content: "replaced" })
          }),
        Tool.provenance({ type: "sdk" }, "opencode.tool.read"),
      )
      const changed = yield* service.snapshot()
      yield* changed.execute({
        ...input,
        call: { ...input.call, name: "alias" },
        definitions: new Map([
          ["alias", { type: "tool", name: "read", description: "alias", inputSchema: { type: "object" } }],
        ]),
      })
      yield* service.transform(
        (editor) => editor.add({ ...definition, name: "target" }),
        Tool.provenance({ type: "builtin" }, "opencode.tool.read"),
      )
      yield* hooks.register("tool", "execute.before", (event) =>
        Effect.sync(() => {
          event.tool = "target"
        }),
      )
      const retargeted = yield* service.snapshot()
      yield* retargeted.execute(input)
      expect(seen).toEqual([
        mcp,
        { origin: "plugin", execution: "local", category: "extension" },
        mcp,
        { origin: "plugin", execution: "local", category: "extension" },
        { origin: "builtin", execution: "local", category: "file_read" },
      ])
      expect(terminal).toEqual(seen)
      expect(seen.every(Object.isFrozen)).toBe(true)
    }),
  )

  it.effect("classifies only actual built-in registrations and keeps untagged tools unknown", () =>
    Effect.gen(function* () {
      expect(Tool.provenance({ type: "builtin" }, "opencode.tool.read")).toEqual({
        origin: "builtin",
        execution: "local",
        category: "file_read",
      })
      expect(Tool.provenance({ type: "sdk" }, "opencode.tool.read")).toEqual({
        origin: "plugin",
        execution: "local",
        category: "extension",
      })
      expect(Tool.provenance(undefined, "opencode.tool.read")).toEqual({
        origin: "unknown",
        execution: "local",
        category: "unknown",
      })
      const service = yield* register()
      const hooks = yield* PluginHooks.Service
      const seen: unknown[] = []
      yield* hooks.use("tool", "execute", (event, next) => {
        seen.push(event.provenance)
        return next(event.input)
      })
      const snapshot = yield* service.snapshot()
      yield* snapshot.execute(call({ text: "unchanged" }))
      expect(seen).toEqual([{ origin: "unknown", execution: "local", category: "unknown" }])
    }),
  )

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
