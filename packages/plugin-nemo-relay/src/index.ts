import { Plugin } from "@opencode/plugin/effect"
import type { Session } from "@opencode/schema/session"
import type { SessionEvent } from "@opencode/schema/session-event"
import { Effect, Stream } from "effect"
import { RelayHost } from "./host.js"
import { RelayModel } from "./model.js"
import { RelayTool } from "./tool.js"

type Terminal =
  | SessionEvent.Execution.Succeeded
  | SessionEvent.Execution.Failed
  | SessionEvent.Execution.Interrupted
  | SessionEvent.Deleted

const OUTCOMES = {
  "session.execution.succeeded": "success",
  "session.execution.failed": "failed",
  "session.execution.interrupted": "cancelled",
  "session.deleted": "cancelled",
} as const satisfies Record<Terminal["type"], RelayHost.Outcome>

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
      yield* ctx.event.subscribe().pipe(
        Stream.filter((event): event is Terminal => event.type in OUTCOMES),
        Stream.runForEach((event) => Effect.sync(() => runtime.close(event.data.sessionID, OUTCOMES[event.type]))),
        Effect.forkScoped({ startImmediately: true }),
      )
    }),
})
