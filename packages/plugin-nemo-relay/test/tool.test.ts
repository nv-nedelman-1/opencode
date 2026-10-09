import { describe, expect, test } from "bun:test"
import type { ToolExecution, ToolMiddlewares } from "@opencode/plugin/effect/tool"
import { Agent } from "@opencode/schema/agent"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { Tool } from "@opencode/schema/tool"
import { Cause, Effect, Exit } from "effect"
import type { ScopeHandle } from "nemo-relay-node"
import { RelayHost } from "../src/host"
import { RelayTool } from "../src/tool"

type Bridge = (
  input: unknown,
  next: (input: unknown, signal: AbortSignal) => Promise<{ result: unknown }>,
) => Promise<{ result: unknown }>

const call: ToolExecution = {
  tool: "read",
  sessionID: Session.ID.make("ses_tool_unit"),
  agent: Agent.ID.make("build"),
  messageID: SessionMessage.ID.make("msg_tool_unit"),
  id: Tool.CallID.make("call_tool_unit"),
  input: { path: "a.txt" },
}

const run = (bridge: Bridge, next: Parameters<ToolMiddlewares["execute"]>[1], signal?: AbortSignal) => {
  const parent = { uuid: "scope_tool_unit" } as ScopeHandle
  const runtime: RelayHost.Runtime = {
    relay: {
      toolCallExecuteAsync: (_name: string, input: unknown, callback: Parameters<Bridge>[1]) => bridge(input, callback),
    } as unknown as RelayHost.Relay,
    scope: () => Effect.succeed(parent),
    open: () => parent,
    admit: () => true,
    mark: () => {},
    lease: () =>
      Effect.succeed({
        parent,
        run: (callback) => callback(),
        onCancel: () => {},
        release: () => {},
      }),
    close: () => {},
  }
  return Effect.runPromiseExit(RelayTool.middleware(runtime, () => Effect.succeed(undefined))(call, next), { signal })
}

const forward: Bridge = (input, next) => next(input, new AbortController().signal)

describe("Relay tool middleware boundaries", () => {
  test("keeps result identity and projects stateful results only once", async () => {
    const state = { reads: 0 }
    const result = {
      content: "read",
      get metadata() {
        if (++state.reads > 1) throw new Error("projected twice")
        return { source: "tool" }
      },
    }
    const exit = await run(forward, () => Effect.succeed(result))
    expect(Exit.isSuccess(exit) && exit.value).toBe(result)
    expect(state.reads).toBe(1)
  })

  test("keeps non-JSON results and never replays a completed tool after Relay failure", async () => {
    const state = { calls: 0 }
    const result = { output: 1n, content: "read" }
    const exit = await run(forward, () =>
      Effect.sync(() => {
        state.calls++
        return result
      }),
    )
    expect(Exit.isSuccess(exit) && exit.value).toBe(result)
    expect(state.calls).toBe(1)
  })

  test("preserves tool failures, permission defects, and interruption unchanged", async () => {
    const failure = new Tool.Error({ message: "missing file" })
    const defect = new Error("declined")
    const failed = await run(forward, () => Effect.fail(failure))
    const died = await run(forward, () => Effect.die(defect))
    const interrupted = await run(forward, () => Effect.interrupt)
    expect(Exit.isFailure(failed) && Cause.squash(failed.cause)).toBe(failure)
    expect(Exit.isFailure(died) && Cause.squash(died.cause)).toBe(defect)
    expect(Exit.isFailure(interrupted) && Cause.hasInterruptsOnly(interrupted.cause)).toBe(true)
  })

  test("accepts intentional replacements and typed failure recovery, but not recovered defects", async () => {
    const recover: Bridge = async (input, next) => {
      await next(input, new AbortController().signal).catch(() => undefined)
      return { result: { content: "recovered" } }
    }
    expect(await run(recover, () => Effect.fail(new Tool.Error({ message: "retryable" })))).toEqual(
      Exit.succeed({ content: "recovered" }),
    )
    const defect = new Error("permission declined")
    const died = await run(recover, () => Effect.die(defect))
    expect(Exit.isFailure(died) && Cause.squash(died.cause)).toBe(defect)
    expect(
      await run(
        async () => ({ result: { content: "cached" } }),
        () => Effect.die("must not run"),
      ),
    ).toEqual(Exit.succeed({ content: "cached" }))
  })

  test.each(["serial", "concurrent"])("rejects %s repeated dispatch without replaying side effects", async (order) => {
    const state = { calls: 0, error: "" }
    const result = { content: "first" }
    const bridge: Bridge = async (input, next) => {
      const signal = new AbortController().signal
      const first = next(input, signal)
      if (order === "serial") await first
      await next({ path: "second.txt" }, signal).catch((error: Error) => {
        state.error = error.message
      })
      return first
    }
    const exit = await run(bridge, () =>
      Effect.sync(() => {
        state.calls++
        return result
      }),
    )
    expect(Exit.isSuccess(exit) && exit.value).toBe(result)
    expect(state.calls).toBe(1)
    expect(state.error).toBe("OpenCode tool middleware may call next at most once")
  })

  test("awaits a revoked in-flight continuation before accepting its replacement", async () => {
    const started = Promise.withResolvers<void>()
    const state = { finalized: false }
    const bridge: Bridge = async (input, next) => {
      const aborted = new AbortController()
      const pending = next(input, aborted.signal).catch(() => undefined)
      await started.promise
      aborted.abort()
      void pending
      return { result: { content: "replacement" } }
    }
    const exit = await run(bridge, () =>
      Effect.sync(() => started.resolve()).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Effect.sync(() => (state.finalized = true))),
      ),
    )
    expect(exit).toEqual(Exit.succeed({ content: "replacement" }))
    expect(state.finalized).toBe(true)
  })

  test("caller cancellation drains the running tool and cannot trigger unmanaged replay", async () => {
    const started = Promise.withResolvers<void>()
    const aborted = new AbortController()
    const state = { calls: 0, finalized: false }
    const pending = run(
      forward,
      () =>
        Effect.sync(() => {
          state.calls++
          started.resolve()
        }).pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sync(() => (state.finalized = true)))),
      aborted.signal,
    )
    await started.promise
    aborted.abort()
    const exit = await pending
    expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
    expect(state).toEqual({ calls: 1, finalized: true })
  })

  test("never bypasses selected plugins on managed failure or guardrail rejection", async () => {
    const state = { calls: 0 }
    const next = () =>
      Effect.sync(() => {
        state.calls++
        return { content: "unmanaged" }
      })
    const failed = await run(async () => {
      throw new Error("binding unavailable")
    }, next)
    const rejected = await run(async () => {
      throw new Error("guardrail rejected: forbidden")
    }, next)
    expect(Exit.isFailure(failed) && Cause.squash(failed.cause)).toEqual(
      new Tool.Error({
        message: "NeMo Relay could not manage this tool call",
        metadata: { "nemo_relay.failure_stage": "before_dispatch" },
      }),
    )
    expect(state.calls).toBe(0)
    expect(Exit.isFailure(rejected) && Cause.squash(rejected.cause)).toEqual(
      new Tool.Error({ message: "NeMo Relay blocked this tool call: forbidden" }),
    )
  })
})
