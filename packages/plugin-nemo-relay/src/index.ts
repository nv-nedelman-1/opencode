import { Plugin } from "@opencode/plugin/effect"
import { EventManifest } from "@opencode/schema/event-manifest"
import type { Session } from "@opencode/schema/session"
import { Cause, Effect, Stream } from "effect"
import { RelayHost } from "./host.js"
import { RelayModel } from "./model.js"
import { RelayTool } from "./tool.js"
import { RelayObserve } from "./observe.js"

const observe = (effect: Effect.Effect<void>) =>
  effect.pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterrupts(cause)
        ? Effect.failCause(cause).pipe(Effect.orDie)
        : Effect.logWarning("NeMo Relay host observation failed"),
    ),
  )

/**
 * Built-in NeMo Relay integration. It activates when Relay is configured through its user or system
 * `plugins.toml` (or `OPENCODE_NEMO_RELAY_PLUGINS_TOML`), and runs every model request and tool call
 * through Relay managed execution beneath one Relay scope per session execution.
 */
export default Plugin.define({
  id: "opencode.nemo-relay",
  effect: (ctx) =>
    Effect.gen(function* () {
      const runtime = yield* RelayHost.acquire()
      if (!runtime) return
      const parentID = (sessionID: Session.ID) =>
        ctx.session.get({ sessionID }).pipe(
          Effect.map((session) => session.parentID),
          Effect.orElseSucceed(() => undefined),
        )
      yield* ctx.tool.middleware("execute", RelayTool.middleware(runtime, parentID))
      yield* ctx.session.middleware("http", RelayModel.middleware(runtime, parentID))
      const observed = RelayObserve.make(runtime)
      yield* ctx.session.hook("context", (event) => observe(observed.context(event)))
      yield* ctx.session.hook("compaction", (event) => observe(observed.context(event, "compaction")))
      yield* ctx.session.hook("generate", (event) => observe(observed.context(event, "generate")))
      yield* ctx.session.hook("title", (event) => observe(observed.context(event, "title")))
      yield* ctx.event.subscribe().pipe(
        Stream.filter(EventManifest.isServer),
        Stream.runForEach((event) => observe(observed.event(event))),
        Effect.forkScoped({ startImmediately: true }),
      )
    }),
})
