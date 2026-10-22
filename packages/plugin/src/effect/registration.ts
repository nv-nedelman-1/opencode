import type { Effect, Scope } from "effect"

export interface Registration {
  readonly dispose: Effect.Effect<void>
}

export interface ModelHookOptions {
  /** Limits the hook to one provider. Unscoped hooks apply to every provider. */
  readonly providerID?: string
}

export type Hooks<Spec, Failures extends Record<keyof Spec, unknown> = Record<keyof Spec, never>> = <
  Name extends keyof Spec,
>(
  name: Name,
  callback: (input: Spec[Name]) => Effect.Effect<void, Failures[Name]>,
) => Effect.Effect<Registration, never, Scope.Scope>

export type ModelHooks<Spec, Failures extends Record<keyof Spec, unknown> = Record<keyof Spec, never>> = <
  Name extends keyof Spec,
>(
  name: Name,
  callback: (input: Spec[Name]) => Effect.Effect<void, Failures[Name]>,
  options?: Spec[Name] extends { readonly model: unknown } ? ModelHookOptions : never,
) => Effect.Effect<Registration, never, Scope.Scope>

export type Transform<Input> = (callback: (input: Input) => void) => Effect.Effect<Registration, never, Scope.Scope>

/**
 * Registers an around-middleware. Unlike hooks, a middleware receives a `next` continuation for the
 * operation it wraps. Middlewares nest in registration order: the first registered is outermost.
 */
export type Middleware<Spec> = <Name extends keyof Spec>(
  name: Name,
  middleware: Spec[Name],
) => Effect.Effect<Registration, never, Scope.Scope>

export type ModelMiddleware<Spec> = <Name extends keyof Spec>(
  name: Name,
  middleware: Spec[Name],
  options?: ModelHookOptions,
) => Effect.Effect<Registration, never, Scope.Scope>
