import { afterAll, beforeAll, expect, test } from "bun:test"
import type { Plugin } from "@opencode/plugin/effect"
import type { SessionHttpCall, SessionMiddlewares } from "@opencode/plugin/effect/session"
import type { ToolExecution, ToolMiddlewares } from "@opencode/plugin/effect/tool"
import { Cause, Effect, Exit } from "effect"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import NemoRelayPlugin from "../src/index"
import { RelayBinding } from "../src/binding"

let directory: string
let pluginsToml: string

beforeAll(async () => {
  directory = await mkdtemp(path.join(os.tmpdir(), "opencode-relay-startup-"))
  pluginsToml = path.join(directory, "plugins.toml")
  await writeFile(pluginsToml, "version = 1\n")
})

afterAll(async () => {
  if (directory) await rm(directory, { recursive: true, force: true })
})

test.each([
  { name: "explicit missing file", content: undefined },
  { name: "malformed file", content: 'version = "private-config-detail' },
  {
    name: "invalid selected component",
    content:
      'version = 1\n[policy]\nunknown_component = "error"\n[[components]]\nkind = "private-config-detail"\nenabled = true\n',
  },
])("$name blocks tool and HTTP dispatch without leaking activation or configuration", async ({ name, content }) => {
  const file = path.join(directory, `${name}.toml`)
  if (content !== undefined) await writeFile(file, content)
  const previous = {
    enable: process.env.OPENCODE_NEMO_RELAY,
    file: process.env.OPENCODE_NEMO_RELAY_PLUGINS_TOML,
  }
  process.env.OPENCODE_NEMO_RELAY = "1"
  process.env.OPENCODE_NEMO_RELAY_PLUGINS_TOML = file
  const registered: {
    tool?: ToolMiddlewares["execute"]
    http?: SessionMiddlewares["http"]
    handshake?: () => Effect.Effect<void>
  } = {}
  const context = {
    tool: {
      middleware: (_: "execute", middleware: ToolMiddlewares["execute"]) => {
        registered.tool = middleware
        return Effect.void
      },
    },
    session: {
      hook: (_: "experimental.ws.handshake", callback: () => Effect.Effect<void>) => {
        registered.handshake = callback
        return Effect.void
      },
      middleware: (_: "http", middleware: SessionMiddlewares["http"]) => {
        registered.http = middleware
        return Effect.void
      },
    },
  } as unknown as Plugin.Context
  try {
    await Effect.runPromise(Effect.scoped(NemoRelayPlugin.effect(context)))
    if (!registered.tool || !registered.http || !registered.handshake) throw new Error("Expected blocking middleware")
    const dispatch = { tools: 0, requests: 0 }
    const tool = await Effect.runPromiseExit(
      registered.tool({} as ToolExecution, () => Effect.sync(() => ({ output: ++dispatch.tools }))),
    )
    const http = await Effect.runPromiseExit(
      registered.http({} as SessionHttpCall, () =>
        Effect.sync(() => {
          dispatch.requests++
          return new Response("must not dispatch")
        }),
      ),
    )
    expect(dispatch).toEqual({ tools: 0, requests: 0 })
    const websocket = await Effect.runPromiseExit(registered.handshake())
    expect(Exit.isFailure(websocket) && Cause.hasDies(websocket.cause)).toBe(true)
    for (const exit of [tool, http] as readonly Exit.Exit<unknown, Error>[]) {
      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) throw new Error("Expected configuration failure")
      expect(String(Cause.squash(exit.cause))).toContain("NeMo Relay plugin configuration failed")
      expect(String(Cause.squash(exit.cause))).not.toContain("private-config-detail")
    }
    const [, plugin] = await RelayBinding.load()
    const activation = await plugin.initialize(plugin.defaultConfig(), pluginsToml)
    expect(activation.isActive).toBe(true)
    await activation.close()
  } finally {
    if (previous.enable === undefined) delete process.env.OPENCODE_NEMO_RELAY
    else process.env.OPENCODE_NEMO_RELAY = previous.enable
    if (previous.file === undefined) delete process.env.OPENCODE_NEMO_RELAY_PLUGINS_TOML
    else process.env.OPENCODE_NEMO_RELAY_PLUGINS_TOML = previous.file
  }
})

test.each(["failStream", "pushStreamChunkAsync"])("a loaded binding without %s is a blocking startup failure", async (api) => {
  // Isolate module substitution so other native tests keep their real binding.
  const child = Bun.spawn(
    [
      process.execPath,
      "-e",
      `import { mock } from "bun:test";
       import { Effect } from "effect";
       import { RelayBinding } from "./src/binding.ts";
       const [relay, plugin] = await RelayBinding.load();
       mock.module("./src/binding.ts", () => ({ RelayBinding: { load: async () => [{...relay, ${api}: undefined}, plugin] } }));
       const { RelayHost } = await import("./src/host.ts");
       const runtime = await Effect.runPromise(Effect.scoped(RelayHost.acquire({config: plugin.defaultConfig()})));
       if (!(runtime instanceof RelayHost.StartupFailure) || !runtime.error.message.includes("${api}")) process.exit(1);`,
    ],
    { cwd: path.resolve(import.meta.dir, ".."), stdout: "pipe", stderr: "pipe" },
  )
  expect(await child.exited).toBe(0)
})

test("failed native teardown blocks a new Location instead of starting a competing host", async () => {
  // Retirement is deliberately sticky until process restart; isolate that process-wide state.
  const child = Bun.spawn(
    [
      process.execPath,
      "-e",
      `import { mock } from "bun:test";
       import { Effect, Exit, Scope } from "effect";
       import { RelayBinding } from "./src/binding.ts";
       const [relay, plugin] = await RelayBinding.load();
       let starts = 0;
       let activation;
       mock.module("./src/binding.ts", () => ({ RelayBinding: { load: async () => [relay, {
         ...plugin,
         initialize: async (...args) => {
           starts++;
           activation = await plugin.initialize(...args);
           return { report: activation.report, isActive: activation.isActive, close: async () => { throw new Error("private-teardown-detail"); } };
         }
       }] } }));
       const { RelayHost } = await import("./src/host.ts");
       const options = {config: plugin.defaultConfig(), pluginsToml: ${JSON.stringify(pluginsToml)}};
       const scope = Effect.runSync(Scope.make());
       const runtime = await Effect.runPromise(RelayHost.acquire(options).pipe(Scope.provide(scope)));
       await Effect.runPromise(Scope.close(scope, Exit.void));
       const failure = await runtime.retired;
       const next = await Effect.runPromise(Effect.scoped(RelayHost.acquire(options)));
       const valid = failure instanceof RelayHost.StartupFailure && next === failure && failure.retained &&
         activation.isActive && starts === 1 && !failure.error.message.includes("private-teardown-detail");
       await activation.close();
       if (!valid) process.exit(1);`,
    ],
    { cwd: path.resolve(import.meta.dir, ".."), stdout: "pipe", stderr: "pipe" },
  )
  expect(await child.exited).toBe(0)
})
