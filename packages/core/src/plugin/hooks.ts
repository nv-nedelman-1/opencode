export * as PluginHooks from "./hooks.js"

import type { AISDKHooks } from "@opencode/plugin/effect/aisdk"
import type { SessionHooks, SessionMiddlewares } from "@opencode/plugin/effect/session"
import type { ShellHooks } from "@opencode/plugin/effect/shell"
import type { ToolFailures, ToolHooks, ToolMiddlewares } from "@opencode/plugin/effect/tool"
import type { ModelHookOptions } from "@opencode/plugin/effect/registration"
import type { PermissionHooks } from "@opencode/plugin/effect/permission"
import { Context, Effect, Layer, Scope } from "effect"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { State } from "../state.js"

export interface Domains {
  readonly aisdk: AISDKHooks
  readonly session: SessionHooks
  readonly permission: PermissionHooks
  readonly shell: ShellHooks
  readonly tool: ToolHooks
}

export interface Middlewares {
  readonly session: SessionMiddlewares
  readonly tool: ToolMiddlewares
}

type NoFailures<Spec> = { readonly [Name in keyof Spec]: never }

// Failure channel for each hook event. Only tool execute.before may fail: a Tool.Error rejects the call before it runs.
interface Failures extends Record<keyof Domains, unknown> {
  readonly aisdk: NoFailures<AISDKHooks>
  readonly session: NoFailures<SessionHooks>
  readonly permission: NoFailures<PermissionHooks>
  readonly shell: NoFailures<ShellHooks>
  readonly tool: ToolFailures
}

type Callback<Event, Error> = (event: Event) => Effect.Effect<void, Error>
type Entry<Callback = Function> = { readonly callback: Callback; readonly options?: ModelHookOptions }

const eventProviderID = (event: unknown) => {
  if (typeof event !== "object" || event === null || !("model" in event)) return undefined
  const model = event.model
  if (typeof model !== "object" || model === null || !("providerID" in model)) return undefined
  return typeof model.providerID === "string" ? model.providerID : undefined
}

export interface Interface {
  readonly has: <Domain extends keyof Domains>(
    domain: Domain,
    name: keyof Domains[Domain] & keyof Failures[Domain],
    providerID?: string,
  ) => Effect.Effect<boolean>
  readonly register: <Domain extends keyof Domains, Name extends keyof Domains[Domain] & keyof Failures[Domain]>(
    domain: Domain,
    name: Name,
    callback: Callback<Domains[Domain][Name], Failures[Domain][Name]>,
    options?: ModelHookOptions,
  ) => Effect.Effect<State.Registration, never, Scope.Scope>
  readonly trigger: <Domain extends keyof Domains, Name extends keyof Domains[Domain] & keyof Failures[Domain]>(
    domain: Domain,
    name: Name,
    event: Domains[Domain][Name],
  ) => Effect.Effect<Domains[Domain][Name], Failures[Domain][Name]>
  readonly use: <Domain extends keyof Middlewares, Name extends keyof Middlewares[Domain]>(
    domain: Domain,
    name: Name,
    middleware: Middlewares[Domain][Name],
    options?: ModelHookOptions,
  ) => Effect.Effect<State.Registration, never, Scope.Scope>
  /** Middlewares in registration order, limited to those that apply to `providerID`. */
  readonly middlewares: <Domain extends keyof Middlewares, Name extends keyof Middlewares[Domain]>(
    domain: Domain,
    name: Name,
    providerID?: string,
  ) => Effect.Effect<ReadonlyArray<Middlewares[Domain][Name]>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/PluginHooks") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const callbacks = new Map<string, Entry[]>()
    const wrappers = new Map<string, Entry<unknown>[]>()
    const key = (domain: keyof Domains | keyof Middlewares, name: PropertyKey) => `${domain}.${String(name)}`

    const add = Effect.fn("PluginHooks.add")(function* <T>(entries: Map<string, T[]>, id: string, entry: T) {
      const scope = yield* Scope.Scope
      let active = true
      entries.set(id, [...(entries.get(id) ?? []), entry])
      const dispose = Effect.sync(() => {
        if (!active) return
        active = false
        const next = (entries.get(id) ?? []).filter((item) => item !== entry)
        if (next.length === 0) entries.delete(id)
        else entries.set(id, next)
      })
      yield* Scope.addFinalizer(scope, dispose)
      return { dispose }
    })

    const register: Interface["register"] = (domain, name, callback, options) =>
      add(callbacks, key(domain, name), { callback, options })

    const use: Interface["use"] = (domain, name, middleware, options) =>
      add(wrappers, key(domain, name), { callback: middleware, options })

    const middlewares: Interface["middlewares"] = <
      Domain extends keyof Middlewares,
      Name extends keyof Middlewares[Domain],
    >(
      domain: Domain,
      name: Name,
      providerID?: string,
    ) =>
      Effect.sync(() =>
        (wrappers.get(key(domain, name)) ?? []).flatMap((entry) =>
          entry.options?.providerID === undefined || entry.options.providerID === providerID
            ? [entry.callback as Middlewares[Domain][Name]]
            : [],
        ),
      )

    const trigger: Interface["trigger"] = Effect.fnUntraced(function* (domain, name, event) {
      for (const entry of callbacks.get(key(domain, name)) ?? []) {
        if (entry.options?.providerID !== undefined && entry.options.providerID !== eventProviderID(event)) continue
        const result: Effect.Effect<void, Failures[typeof domain][typeof name]> = entry.callback(event)
        yield* result
      }
      return event
    })

    const has: Interface["has"] = (domain, name, providerID) =>
      Effect.sync(() =>
        (callbacks.get(key(domain, name)) ?? []).some(
          (entry) => entry.options?.providerID === undefined || entry.options.providerID === providerID,
        ),
      )

    return Service.of({ has, register, trigger, use, middlewares })
  }),
)

export const node = makeLocationNode({ service: Service, layer, deps: [] })
