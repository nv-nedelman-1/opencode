export * as RelayModel from "./model.js"

import type { SessionHttpCall, SessionMiddlewares } from "@opencode/plugin/effect/session"
import type { Session } from "@opencode/schema/session"
import { Cause, Effect, Exit } from "effect"
import type { LlmStream, ScopeHandle } from "nemo-relay-node"
import { RelayHost } from "./host.js"
import { RelayMetrics } from "./metrics.js"
import { RelaySSE } from "./sse.js"

type Next = Parameters<SessionMiddlewares["http"]>[1]

interface LlmRequest {
  readonly headers: Record<string, string>
  readonly content: unknown
}

interface Producer {
  readonly __nemo_relay_native: LlmRequest
  readonly __nemo_relay_stream_id: number
}

interface Managed {
  readonly runtime: RelayHost.Runtime
  readonly call: SessionHttpCall
  readonly parent: ScopeHandle
  readonly operation: RelayHost.Operation
  readonly ownership: { body: boolean }
  readonly request: LlmRequest
  readonly send: (request: LlmRequest) => ReturnType<Next>
}

type Upstream =
  | { readonly type: "failed"; readonly cause: Cause.Cause<Error> }
  | { readonly type: "response"; readonly response: Response; readonly body: string }
  | { readonly type: "stream"; readonly response: Response }
  | { readonly type: "local" }

// Bedrock signs the exact URL and body before middleware runs, so a managed rewrite would fail verification.
const UNMANAGED = new Set(["bedrock-converse"])
// Credentials never reach Relay; they are restored on the request that is actually sent.
const CREDENTIALS = new Set([
  "authorization",
  "proxy-authorization",
  "cookie",
  "x-api-key",
  "api-key",
  "x-goog-api-key",
])
const STREAMING = 2

/**
 * Runs each physical HTTP model request through Relay managed execution, so conditional-execution
 * guardrails, request intercepts, and execution intercepts apply to the real provider call.
 */
export const middleware =
  (
    runtime: RelayHost.Runtime,
    parentID: (sessionID: Session.ID) => Effect.Effect<Session.ID | undefined>,
  ): SessionMiddlewares["http"] =>
  (call, next) =>
    Effect.gen(function* () {
      if (
        UNMANAGED.has(call.protocol) ||
        call.request.headers.has("x-amz-date") ||
        call.request.headers.get("authorization")?.startsWith("AWS4-HMAC-SHA256")
      ) {
        runtime.mark(call.sessionID, "opencode.llm.coverage", { managed: false, reason: "signed_transport" })
        return yield* next(call.request)
      }
      const text = yield* Effect.promise(() => call.request.clone().text())
      const content = RelayHost.parse(text)
      if (!RelayHost.isRecord(content)) return yield* next(call.request)
      const request = {
        headers: Object.fromEntries(
          Array.from(call.request.headers).filter(([key]) => !CREDENTIALS.has(key) && !key.startsWith("x-amz-")),
        ),
        content,
      }
      const operation = yield* runtime.lease(call.sessionID, parentID(call.sessionID))
      const ownership = { body: false }
      const dispatch = { sent: false }
      const managed: Managed = {
        runtime,
        call,
        parent: operation.parent,
        operation,
        ownership,
        request,
        send: (intercepted) =>
          Effect.suspend(() => {
            if (dispatch.sent) return Effect.fail(new Error("OpenCode HTTP middleware may call next at most once"))
            dispatch.sent = true
            return next(rebuild(call.request, text, request, intercepted))
          }),
      }
      return yield* (
        content.stream === true || call.request.url.includes(":streamGenerateContent")
          ? stream(managed)
          : unary(managed)
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (!ownership.body) operation.release()
          }),
        ),
      )
    })

const stream = (managed: Managed) =>
  Effect.gen(function* () {
    const relay = managed.runtime.relay
    const run = Effect.runPromiseExitWith(yield* Effect.context<never>())
    const abort = new AbortController()
    const upstream = Promise.withResolvers<Upstream>()
    const aggregate = RelaySSE.accumulator(managed.call.protocol)
    const push =
      "pushStreamChunkAsync" in relay && typeof relay.pushStreamChunkAsync === "function"
        ? relay.pushStreamChunkAsync.bind(relay)
        : undefined
    if (!push) return yield* Effect.fail(new Error("NeMo Relay native runtime lacks awaitable stream push support"))
    const source: { named: boolean; done: boolean; failure?: Error; invalid?: RelaySSE.Event } = {
      named: false,
      done: false,
    }
    const lifecycle: { llm?: LlmStream; opening?: Promise<LlmStream>; closed?: Promise<void>; producing?: boolean } = {}
    const close = () =>
      (lifecycle.closed ??= (async () => {
        abort.abort()
        await lifecycle.opening?.catch(() => undefined)
        await lifecycle.llm?.close().catch(() => undefined)
        managed.call.request.signal.removeEventListener("abort", closeOnAbort)
        managed.operation.release()
      })())
    const closeOnAbort = () => {
      source.failure ??= new DOMException("Provider request was aborted", "AbortError")
      void close()
    }
    managed.operation.onCancel(closeOnAbort)
    managed.call.request.signal.addEventListener("abort", closeOnAbort, { once: true })
    if (managed.call.request.signal.aborted) {
      yield* Effect.promise(close)
      return yield* Effect.fail(new DOMException("Provider request was aborted", "AbortError"))
    }

    const produce = async (producer: Producer) => {
      const id = producer.__nemo_relay_stream_id
      const attempt = new ProviderAttempt(managed, true)
      try {
        const exit = await run(managed.send(producer.__nemo_relay_native), { signal: abort.signal })
        if (Exit.isFailure(exit)) {
          attempt.finish(Cause.hasInterrupts(exit.cause) ? "cancelled" : "failed", "transport")
          upstream.resolve({ type: "failed", cause: exit.cause })
          return fail(relay, id, "provider request failed")
        }
        attempt.headers(exit.value.status)
        if (!exit.value.ok || !exit.value.body) {
          const text = await run(
            Effect.tryPromise({
              try: () => exit.value.text(),
              catch: (error) => (error instanceof Error ? error : new Error(String(error))),
            }),
            { signal: abort.signal },
          )
          if (Exit.isFailure(text)) {
            attempt.finish(Cause.hasInterrupts(text.cause) ? "cancelled" : "failed", "body")
            upstream.resolve({ type: "failed", cause: text.cause })
            return fail(relay, id, "provider response body failed")
          }
          attempt.finish(exit.value.ok ? "success" : "failed", exit.value.ok ? undefined : "http")
          upstream.resolve({ type: "response", response: exit.value, body: text.value })
          return fail(relay, id, `provider returned HTTP ${exit.value.status}`)
        }
        upstream.resolve({ type: "stream", response: exit.value })
        const failure = await RelaySSE.read(
          exit.value.body,
          async (event) => {
            source.named ||= event.event !== undefined
            // Match the host's SSE framing; a JSON null is also Relay's consumer EOF sentinel.
            if (event.data === "" || event.data === "null" || event.data === ": keepalive") return true
            if (event.data === "[DONE]") {
              source.done = true
              return false
            }
            const chunk = RelayHost.parse(event.data)
            if (chunk === undefined) {
              source.invalid = event
              throw new Error("Provider returned invalid JSON SSE data")
            }
            attempt.output(chunk)
            aggregate.push(chunk)
            return await push(id, chunk)
          },
          abort.signal,
        ).then(
          () => undefined,
          (error: unknown) => (error instanceof Error ? error : new Error(String(error))),
        )
        // The pushed producer must settle before native close can finish; cancellation is not a successful EOF.
        if (abort.signal.aborted) {
          attempt.finish("cancelled", "cancelled")
          return fail(relay, id, "Provider request was aborted", "AbortError")
        }
        if (failure === undefined) {
          attempt.finish("success")
          return relay.endStream(id)
        }
        attempt.finish("failed", source.invalid ? "protocol" : "body")
        source.failure = failure
        fail(relay, id, failure.message)
      } catch (error) {
        attempt.finish(abort.signal.aborted ? "cancelled" : "failed", "body")
        throw error
      }
    }

    const opened = yield* Effect.tryPromise({
      try: () =>
        (lifecycle.opening = managed.operation
          .run(() =>
            withCodec(codecFor(relay, managed.call.protocol), (codecs) =>
              relay.llmStreamCallExecute(
                managed.call.model.providerID,
                managed.request,
                (producer: Producer) => {
                  if (lifecycle.producing) throw new Error("OpenCode HTTP middleware may call next at most once")
                  lifecycle.producing = true
                  void produce(producer).catch((error: unknown) => {
                    const failure = error instanceof Error ? error : new Error(String(error))
                    source.failure = failure
                    upstream.resolve({ type: "failed", cause: Cause.fail(failure) })
                    fail(relay, producer.__nemo_relay_stream_id, failure.message)
                  })
                },
                undefined,
                aggregate.value,
                managed.parent,
                STREAMING,
                null,
                metadata(managed.call),
                managed.call.model.id,
                ...codecs,
              ),
            ),
          )
          .then((llm) => {
            lifecycle.llm = llm
            return llm
          })),
      catch: (error) => error,
    }).pipe(
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Effect.promise(close) : Effect.void)),
      Effect.exit,
    )
    if (Exit.isFailure(opened)) {
      yield* Effect.promise(close)
      return yield* fallback(opened.cause)
    }

    const llm = opened.value
    lifecycle.llm = llm
    if (abort.signal.aborted) {
      yield* Effect.promise(() => llm.close().catch(() => undefined))
      return yield* Effect.fail(new Error("Provider request was aborted"))
    }
    // Pull eagerly: Relay may defer the provider callback until the first read, and an execution
    // intercept may answer without calling the provider at all.
    const first = llm.next()
    const started = yield* Effect.promise((signal) => {
      signal.addEventListener("abort", closeOnAbort, { once: true })
      return Promise.race([upstream.promise, first.then(local, local)])
    }).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? Effect.promise(close) : Effect.void)))
    if (started.type === "failed") {
      yield* Effect.promise(close)
      return yield* Effect.failCause(started.cause)
    }
    if (started.type === "response") {
      yield* Effect.promise(close)
      return new Response(responseBody(started.response.status, started.body), {
        status: started.response.status,
        statusText: started.response.statusText,
        headers: bodyHeaders(started.response.headers),
      })
    }
    managed.ownership.body = true
    return new Response(body(llm, first, source, close), {
      status: started.type === "stream" ? started.response.status : 200,
      statusText: started.type === "stream" ? started.response.statusText : undefined,
      headers:
        started.type === "stream" ? bodyHeaders(started.response.headers) : { "content-type": "text/event-stream" },
    })
  })

const local = (): Upstream => ({ type: "local" })

const body = (
  llm: LlmStream,
  first: Promise<unknown>,
  source: {
    readonly named: boolean
    readonly done: boolean
    readonly failure?: Error
    readonly invalid?: RelaySSE.Event
  },
  close: () => Promise<void>,
) => {
  const encoder = new TextEncoder()
  const pending: { next?: Promise<unknown> } = { next: first }
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const read = await (pending.next ?? llm.next()).then(
        (chunk) => ({ chunk }),
        (error: unknown) => ({ chunk: null, error }),
      )
      pending.next = undefined
      if (read.chunk !== null) return controller.enqueue(encoder.encode(RelaySSE.encode(read.chunk, source.named)))
      await close()
      if (source.invalid) {
        controller.enqueue(encoder.encode(RelaySSE.encodeEvent(source.invalid)))
        return controller.close()
      }
      // The provider's own failure keeps its identity for retry classification; Relay reports it wrapped.
      if (source.failure) return controller.error(source.failure)
      if ("error" in read) return controller.error(read.error)
      if (source.done) controller.enqueue(encoder.encode("data: [DONE]\n\n"))
      controller.close()
    },
    async cancel() {
      await close()
    },
  })
}

const unary = (managed: Managed) =>
  Effect.gen(function* () {
    const relay = managed.runtime.relay
    const run = Effect.runPromiseExitWith(yield* Effect.context<never>())
    const abort = new AbortController()
    managed.operation.onCancel(() => abort.abort())
    const captured: {
      pending?: Promise<unknown>
      called?: boolean
      failure?: Cause.Cause<Error>
      response?: {
        readonly status: number
        readonly statusText: string
        readonly headers: Headers
        readonly text: string
      }
    } = {}
    const executed = yield* Effect.tryPromise({
      try: (signal) =>
        (captured.pending = managed.operation.run(() =>
          withCodec(codecFor(relay, managed.call.protocol), (codecs) =>
            relay.llmCallExecuteAsync(
              managed.call.model.providerID,
              managed.request,
              async (intercepted: LlmRequest) => {
                if (captured.called) throw new Error("OpenCode HTTP middleware may call next at most once")
                captured.called = true
                const requestSignal = AbortSignal.any([signal, abort.signal, managed.call.request.signal])
                const attempt = new ProviderAttempt(managed, false)
                const exit = await run(managed.send(intercepted), { signal: requestSignal })
                if (Exit.isFailure(exit)) {
                  attempt.finish(Cause.hasInterrupts(exit.cause) ? "cancelled" : "failed", "transport")
                  captured.failure = exit.cause
                  throw new Error("provider request failed")
                }
                attempt.headers(exit.value.status)
                const body = await run(
                  Effect.tryPromise({
                    try: () => exit.value.text(),
                    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
                  }),
                  { signal: requestSignal },
                )
                if (Exit.isFailure(body)) {
                  attempt.finish(Cause.hasInterrupts(body.cause) ? "cancelled" : "failed", "body")
                  captured.failure = body.cause
                  throw new Error("provider response body failed")
                }
                const text = body.value
                captured.response = {
                  status: exit.value.status,
                  statusText: exit.value.statusText,
                  headers: exit.value.headers,
                  text,
                }
                const response = RelayHost.parse(text) ?? text
                if (exit.value.ok) attempt.output(response)
                attempt.finish(exit.value.ok ? "success" : "failed", exit.value.ok ? undefined : "http")
                if (!exit.value.ok) throw new Error(`provider returned HTTP ${exit.value.status}`)
                return response
              },
              managed.parent,
              0,
              null,
              metadata(managed.call),
              managed.call.model.id,
              ...codecs,
            ),
          ),
        )),
      catch: (error) => error,
    }).pipe(
      Effect.onExit((exit) =>
        Exit.isFailure(exit)
          ? Effect.promise(async () => {
              abort.abort()
              // Interruption stops waiting for tryPromise, not the native managed call or its intercept tail.
              await captured.pending?.catch(() => undefined)
            })
          : Effect.void,
      ),
      Effect.exit,
    )
    if (
      captured.failure &&
      (Exit.isFailure(executed) || Cause.hasDies(captured.failure) || Cause.hasInterrupts(captured.failure))
    )
      return yield* Effect.failCause(captured.failure)
    const upstream = captured.response
    if (upstream && Exit.isFailure(executed))
      return new Response(responseBody(upstream.status, upstream.text), {
        status: upstream.status,
        statusText: upstream.statusText,
        headers: bodyHeaders(upstream.headers),
      })
    if (Exit.isFailure(executed)) return yield* fallback(executed.cause)
    const status = upstream?.status !== undefined && upstream.status < 400 ? upstream.status : 200
    return new Response(
      responseBody(
        status,
        upstream && RelayHost.sameJson(executed.value, RelayHost.parse(upstream.text) ?? upstream.text)
          ? upstream.text
          : JSON.stringify(executed.value),
      ),
      {
        status,
        statusText: upstream?.status !== undefined && upstream.status < 400 ? upstream.statusText : undefined,
        headers: upstream ? bodyHeaders(upstream.headers) : { "content-type": "application/json" },
      },
    )
  })

const responseBody = (status: number, text: string) => ([204, 205, 304].includes(status) ? null : text)

/** A guardrail block answers locally. Other managed failures never replay a provider request. */
const fallback = (cause: Cause.Cause<unknown>) =>
  Effect.gen(function* () {
    const reason = RelayHost.rejection(cause)
    if (reason !== undefined) return blocked(reason)
    if (Cause.hasInterrupts(cause) || Cause.hasDies(cause)) return yield* Effect.failCause(cause).pipe(Effect.orDie)
    return yield* Effect.fail(new Error("NeMo Relay could not manage this model request"))
  })

// A content-policy failure is final: the session reports it instead of retrying the request.
const blocked = (reason: string) =>
  new Response(
    JSON.stringify({
      error: {
        type: "nemo_relay_guardrail_rejected",
        code: "content_policy_violation",
        message: `NeMo Relay blocked this request: ${reason}`,
      },
    }),
    { status: 400, headers: { "content-type": "application/json", "x-should-retry": "false" } },
  )

/** Applies Relay's rewrites to the original request while keeping the credentials Relay never saw. */
const rebuild = (original: Request, text: string, request: LlmRequest, intercepted: LlmRequest) => {
  const headers = new Headers(original.headers)
  Object.keys(request.headers)
    .filter((key) => !(key in intercepted.headers))
    .forEach((key) => headers.delete(key))
  Object.entries(intercepted.headers)
    .filter(([key]) => !CREDENTIALS.has(key.toLowerCase()) && !key.toLowerCase().startsWith("x-amz-"))
    .forEach(([key, value]) => headers.set(key, value))
  headers.delete("content-length")
  return new Request(original, {
    headers,
    body: RelayHost.sameJson(intercepted.content, request.content) ? text : JSON.stringify(intercepted.content),
  })
}

// The rewritten body is decoded text, so length and encoding headers no longer describe it.
const bodyHeaders = (source: Headers) => {
  const headers = new Headers(source)
  headers.delete("content-length")
  headers.delete("content-encoding")
  return headers
}

const metadata = (call: SessionHttpCall) => ({
  provider_name: call.model.providerID,
  "opencode.session_id": call.sessionID,
  "opencode.agent": call.agent,
  "opencode.request_kind": call.kind,
  "opencode.protocol": call.protocol,
})

// These observations own only physical provider I/O; the host decides whether and when to retry it.
class ProviderAttempt {
  private readonly started = performance.now()
  private readonly attributes
  private completed = false
  private emittedOutput = false

  constructor(
    private readonly managed: Managed,
    streaming: boolean,
  ) {
    this.attributes = {
      ...RelayMetrics.route(managed.call.model, managed.call.protocol),
      call_role: managed.call.kind,
      streaming,
    }
    this.mark("started", { count: 1 })
  }

  headers(status: number) {
    this.mark("headers", {
      count: 1,
      duration_ms: performance.now() - this.started,
      http_status_class: status >= 200 && status < 600 ? `${Math.floor(status / 100)}xx` : "other",
    })
  }

  output(chunk: unknown) {
    if (this.emittedOutput || this.completed) return
    const output_kind = RelaySSE.outputKind(this.managed.call.protocol, chunk)
    if (output_kind === undefined) return
    this.emittedOutput = true
    this.managed.runtime.mark(this.managed.call.sessionID, "opencode.llm.first_output", {
      ...this.attributes,
      count: 1,
      duration_ms: performance.now() - this.started,
      output_kind,
    })
  }

  finish(
    outcome: "success" | "failed" | "cancelled",
    error_category?: "http" | "transport" | "body" | "protocol" | "cancelled",
  ) {
    if (this.completed) return
    this.completed = true
    this.mark("completed", {
      count: 1,
      duration_ms: performance.now() - this.started,
      outcome,
      ...(outcome === "success" ? {} : { error_category: outcome === "cancelled" ? "cancelled" : error_category }),
    })
  }

  private mark(name: string, data: Record<string, unknown>) {
    this.managed.runtime.mark(this.managed.call.sessionID, `opencode.llm.provider_attempt.${name}`, {
      ...this.attributes,
      ...data,
    })
  }
}

// Older bindings cannot fail a pushed stream; ending it keeps the partial output and the consumer still errors.
const fail = (relay: RelayHost.Relay, id: number, message: string, exceptionType?: string) => {
  if ("failStream" in relay && typeof relay.failStream === "function")
    return void relay.failStream(id, message, exceptionType)
  relay.endStream(id)
}

const codecFor = (relay: RelayHost.Relay, protocol: string) =>
  protocol === "openai-chat" || protocol === "openai-compatible-chat"
    ? new relay.OpenAIChatCodec()
    : RelaySSE.RESPONSES.has(protocol)
      ? new relay.OpenAIResponsesCodec()
      : protocol === "anthropic-messages"
        ? new relay.AnthropicMessagesCodec()
        : protocol === "gemini"
          ? new relay.GeminiGenerateContentCodec()
          : undefined

type Codec = NonNullable<ReturnType<typeof codecFor>>
type CodecArguments = [
  decode?: (request: unknown) => unknown,
  encode?: (payload: { readonly annotated: unknown; readonly original: unknown }) => unknown,
  decodeResponse?: (response: unknown) => unknown,
]

const callbacks = (codec: Codec): CodecArguments => [
  (request) => codec.decode(request),
  (payload) => codec.encode(payload.annotated, payload.original),
  (response) => codec.decodeResponse(response),
]

const support = { instances: true }

// A built-in codec instance keeps the built-in identity Relay's export presets need, such as the
// metadata-only trajectory preset projecting usage. Bindings that only take codec callbacks reject an
// instance before doing any work, so the call is made again with callbacks.
const withCodec = <A>(codec: Codec | undefined, call: (codecs: CodecArguments) => A): A => {
  if (!codec) return call([])
  if (support.instances) {
    try {
      return call([codec, undefined, codec] as unknown as CodecArguments)
    } catch (error) {
      if (!(error instanceof Error) || !error.message.includes("codecDecode and codecEncode must be provided together"))
        throw error
      support.instances = false
    }
  }
  return call(callbacks(codec))
}
