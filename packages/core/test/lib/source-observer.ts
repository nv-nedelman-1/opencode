import { expect } from "bun:test"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Deferred, Effect, Fiber } from "effect"

export const sourceObserver = <
  Domain extends keyof PluginHooks.Observations,
  Name extends keyof PluginHooks.Observations[Domain],
>(
  hooks: PluginHooks.Interface,
  domain: Domain,
  name: Name,
) =>
  Effect.gen(function* () {
    type Event = PluginHooks.Observations[Domain][Name]
    const pending = Promise.withResolvers<undefined>()
    const release = yield* Deferred.make<void>()
    const received: Event[] = []
    let first: Event | undefined
    let foreignRan = false
    yield* hooks.observe(domain, name, (event) => {
      first = event
      throw new Error("private observer defect")
    })
    // @ts-expect-error Untyped plugins must not hold settlement with a pending promise.
    yield* hooks.observe(domain, name, () => pending.promise)
    // @ts-expect-error Rejected async work must be consumed without awaiting it.
    yield* hooks.observe(domain, name, async () => {
      throw new Error("private async rejection")
    })
    const foreign = Effect.sync(() => {
      foreignRan = true
    }).pipe(Effect.andThen(Deferred.await(release)))
    // @ts-expect-error Returned Effects must neither run nor hold finalizer cleanup.
    yield* hooks.observe(domain, name, () => foreign)
    yield* hooks.observe(domain, name, (event) => void received.push(event))

    return {
      received,
      settle: <A, E, R>(work: Effect.Effect<A, E, R>) =>
        Effect.gen(function* () {
          let settled = false
          const call = yield* work.pipe(
            Effect.onExit(() => Effect.sync(() => void (settled = true))),
            Effect.forkScoped({ startImmediately: true }),
          )
          yield* Effect.yieldNow
          const settledBeforeRelease = settled
          pending.resolve(undefined)
          yield* Deferred.succeed(release, undefined)
          const exit = yield* Fiber.await(call)
          yield* Effect.promise(() => Promise.resolve())
          expect(settledBeforeRelease).toBe(true)
          expect(foreignRan).toBe(false)
          expect(received).toHaveLength(1)
          expect(received[0]).toBe(first!)
          expect(Object.isFrozen(received[0])).toBe(true)
          return exit
        }),
    }
  })
