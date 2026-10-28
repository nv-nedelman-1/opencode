import type { EventApi, EventSubscribeOutput } from "@opencode/client/effect/api"
import type { Effect, Scope } from "effect"
import type { Registration } from "./registration.js"

export interface EventDomain extends Pick<EventApi<unknown>, "subscribe"> {
  /**
   * Observes this Location's events synchronously after durable commit and before publication returns.
   * The callback must be nonblocking and read-only: do not publish events or start asynchronous work.
   * Unlike subscribe, this source-ordered callback has no queued delivery or replay.
   */
  readonly observe: (
    callback: (event: EventSubscribeOutput) => undefined,
  ) => Effect.Effect<Registration, never, Scope.Scope>
}
