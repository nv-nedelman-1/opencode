import { expect } from "bun:test"
import { Agent } from "@opencode/core/agent"
import { Plugin } from "@opencode/core/plugin"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Tool } from "@opencode/core/tool"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import type { ToolProvenance } from "@opencode/plugin/effect/tool"
import { Effect, Schema } from "effect"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)

it.effect("uses actual activation origin even when a plugin adopts a built-in registration ID", () =>
  Effect.gen(function* () {
    const plugins = yield* Plugin.Service
    const tools = yield* Tool.Service
    const hooks = yield* PluginHooks.Service
    const seen: ToolProvenance[] = []
    yield* hooks.use("tool", "execute", (event, next) => {
      seen.push(event.provenance!)
      return next(event.input)
    })
    const input = {
      sessionID: Session.ID.make("ses_provenance"),
      agent: Agent.ID.make("build"),
      messageID: SessionMessage.ID.make("msg_provenance"),
      call: { type: "tool-call" as const, id: "call-read", name: "read", input: {} },
    }
    for (const source of [{ type: "builtin" }, { type: "sdk" }, undefined] as const) {
      yield* plugins.activate([
        {
          id: "opencode.tool.read",
          revision: source?.type ?? "unknown",
          source,
          effect: (ctx) =>
            ctx.tool
              .transform((editor) =>
                editor.add({
                  name: "read",
                  description: "Synthetic same-name registration",
                  input: Schema.Struct({}),
                  options: { codemode: false },
                  execute: () => Effect.succeed({ content: "unchanged" }),
                }),
              )
              .pipe(Effect.asVoid),
        },
      ])
      const snapshot = yield* tools.snapshot()
      expect((yield* snapshot.execute(input)).content).toEqual([{ type: "text", text: "unchanged" }])
    }
    expect(seen).toEqual([
      { origin: "builtin", execution: "local", category: "file_read" },
      { origin: "plugin", execution: "local", category: "extension" },
      { origin: "unknown", execution: "local", category: "unknown" },
    ])
    expect(seen.every(Object.isFrozen)).toBe(true)
  }),
)
