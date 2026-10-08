export * as RelayModel from "./model.js"

import type { SessionHttpCall, SessionMiddlewares } from "@opencode/plugin/effect/session"
import type { Session } from "@opencode/schema/session"
import { Cause, Effect, Exit } from "effect"
import type { LlmStream, ScopeHandle } from "nemo-relay-node"
import { RelayHost } from "./host.js"
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
      if (UNMANAGED.has(call.protocol)) return yield* next(call.request)
      const text = yield* Effect.promise(() => call.request.clone().text())
      const content = RelayHost.parse(text)
      if (!RelayHost.isRecord(content)) return yield* next(call.request)
      const request = {
        headers: Object.fromEntries(
          Array.from(call.request.headers).filter(([key]) => !CREDENTIALS.has(key) && !key.startsWith("x-amz-")),
        ),
        content,
      }
      const managed: Managed = {
        runtime,
        call,
        parent: yield* runtime.scope(call.sessionID, parentID(call.sessionID)),
        request,
        send: (intercepted) => next(rebuild(call.request, text, request, intercepted)),
      }
      if (content.stream === true || call.request.url.includes(":streamGenerateContent")) return yield* stream(managed)
      return yield* unary(managed)
    })

const stream = (managed: Managed) =>
  Effect.gen(function* () {
    const relay = managed.runtime.relay
    const run = Effect.runPromiseExitWith(yield* Effect.context<never>())
    const abort = new AbortController()
    const upstream = Promise.withResolvers<Upstream>()
    const chunks: unknown[] = []
    const source: { named: boolean; done: boolean; failure?: Error } = { named: false, done: false }

    const produce = async (producer: Producer) => {
      const id = producer.__nemo_relay_stream_id
      const exit = await run(managed.send(producer.__nemo_relay_native), { signal: abort.signal })
      if (Exit.isFailure(exit)) {
        upstream.resolve({ type: "failed", cause: exit.cause })
        return fail(relay, id, "provider request failed")
      }
      if (!exit.value.ok || !exit.value.body) {
        upstream.resolve({ type: "response", response: exit.value, body: await exit.value.text() })
        return fail(relay, id, `provider returned HTTP ${exit.value.status}`)
      }
      upstream.resolve({ type: "stream", response: exit.value })
      const failure = await RelaySSE.read(
        exit.value.body,
        (event) => {
          source.named ||= event.event !== undefined
          if (event.data === "[DONE]") {
            source.done = true
            return true
          }
          const chunk = RelayHost.parse(event.data)
          if (chunk === undefined) return true
          chunks.push(chunk)
          return relay.pushStreamChunk(id, chunk)
        },
        abort.signal,
      ).then(
        () => undefined,
        (error: unknown) => (error instanceof Error ? error : new Error(String(error))),
      )
      if (failure === undefined || abort.signal.aborted) return relay.endStream(id)
      source.failure = failure
      fail(relay, id, failure.message)
    }

    const opened = yield* Effect.tryPromise({
      try: () =>
        withCodec(codecFor(relay, managed.call.protocol), (codecs) =>
          relay.llmStreamCallExecute(
            managed.call.model.providerID,
            managed.request,
            (producer: Producer) => void produce(producer),
            undefined,
            () => RelaySSE.aggregate(managed.call.protocol, chunks),
            managed.parent,
            STREAMING,
            null,
            metadata(managed.call),
            managed.call.model.id,
            ...codecs,
          ),
        ),
      catch: (error) => error,
    }).pipe(Effect.exit)
    if (Exit.isFailure(opened)) return yield* fallback(managed, opened.cause)

    const llm = opened.value
    // Pull eagerly: Relay may defer the provider callback until the first read, and an execution
    // intercept may answer without calling the provider at all.
    const first = llm.next()
    const started = yield* Effect.promise((signal) => {
      signal.addEventListener("abort", () => abort.abort(), { once: true })
      return Promise.race([upstream.promise, first.then(local, local)])
    })
    if (started.type === "failed") {
      yield* Effect.promise(() => llm.close().catch(() => undefined))
      return yield* Effect.failCause(started.cause)
    }
    if (started.type === "response") {
      yield* Effect.promise(() => llm.close().catch(() => undefined))
      return new Response(started.body, {
        status: started.response.status,
        statusText: started.response.statusText,
        headers: bodyHeaders(started.response.headers),
      })
    }
    return new Response(body(llm, first, source, abort), {
      status: started.type === "stream" ? started.response.status : 200,
      headers:
        started.type === "stream" ? bodyHeaders(started.response.headers) : { "content-type": "text/event-stream" },
    })
  })

const local = (): Upstream => ({ type: "local" })

const body = (
  llm: LlmStream,
  first: Promise<unknown>,
  source: { readonly named: boolean; readonly done: boolean; readonly failure?: Error },
  abort: AbortController,
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
      await llm.close().catch(() => undefined)
      // The provider's own failure keeps its identity for retry classification; Relay reports it wrapped.
      if (source.failure) return controller.error(source.failure)
      if ("error" in read) return controller.error(read.error)
      if (source.done) controller.enqueue(encoder.encode("data: [DONE]\n\n"))
      controller.close()
    },
    async cancel() {
      abort.abort()
      await llm.close().catch(() => undefined)
    },
  })
}

const unary = (managed: Managed) =>
  Effect.gen(function* () {
    const relay = managed.runtime.relay
    const run = Effect.runPromiseExitWith(yield* Effect.context<never>())
    const captured: {
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
        withCodec(codecFor(relay, managed.call.protocol), (codecs) =>
          relay.llmCallExecuteAsync(
            managed.call.model.providerID,
            managed.request,
            async (intercepted: LlmRequest) => {
              const exit = await run(managed.send(intercepted), { signal })
              if (Exit.isFailure(exit)) {
                captured.failure = exit.cause
                throw new Error("provider request failed")
              }
              const text = await exit.value.text()
              captured.response = {
                status: exit.value.status,
                statusText: exit.value.statusText,
                headers: exit.value.headers,
                text,
              }
              if (!exit.value.ok) throw new Error(`provider returned HTTP ${exit.value.status}`)
              return RelayHost.parse(text) ?? text
            },
            managed.parent,
            0,
            null,
            metadata(managed.call),
            managed.call.model.id,
            ...codecs,
          ),
        ),
      catch: (error) => error,
    }).pipe(Effect.exit)
    if (captured.failure) return yield* Effect.failCause(captured.failure)
    const upstream = captured.response
    if (upstream && (upstream.status >= 400 || Exit.isFailure(executed)))
      return new Response(upstream.text, {
        status: upstream.status,
        statusText: upstream.statusText,
        headers: bodyHeaders(upstream.headers),
      })
    if (Exit.isFailure(executed)) return yield* fallback(managed, executed.cause)
    return new Response(
      upstream && RelayHost.sameJson(executed.value, RelayHost.parse(upstream.text))
        ? upstream.text
        : JSON.stringify(executed.value),
      {
        status: upstream?.status ?? 200,
        headers: upstream ? bodyHeaders(upstream.headers) : { "content-type": "application/json" },
      },
    )
  })

/** A guardrail block answers locally; any other Relay failure before the provider call fails open. */
const fallback = (managed: Managed, cause: Cause.Cause<unknown>) =>
  Effect.gen(function* () {
    const reason = RelayHost.rejection(cause)
    if (reason !== undefined) return blocked(reason)
    yield* Effect.logWarning("NeMo Relay could not manage a model request; sending it unmanaged", {
      cause: Cause.pretty(cause),
    })
    return yield* managed.send(managed.request)
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
  Object.entries(intercepted.headers).forEach(([key, value]) => headers.set(key, value))
  headers.delete("content-length")
  return new Request(original.url, {
    method: original.method,
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
  "opencode.session_id": call.sessionID,
  "opencode.agent": call.agent,
  "opencode.request_kind": call.kind,
  "opencode.protocol": call.protocol,
})

// Older bindings cannot fail a pushed stream; ending it keeps the partial output and the consumer still errors.
const fail = (relay: RelayHost.Relay, id: number, message: string) => {
  if ("failStream" in relay && typeof relay.failStream === "function") return void relay.failStream(id, message)
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
