export * as RelaySSE from "./sse.js"

import { RelayHost } from "./host.js"

export interface Event {
  readonly event?: string
  readonly data: string
}

/**
 * Reads server-sent events from `body` until it ends, `onEvent` returns false, or `signal` aborts.
 * Rejects when the body fails mid-stream.
 */
export const read = async (
  body: ReadableStream<Uint8Array>,
  onEvent: (event: Event) => boolean | Promise<boolean>,
  signal: AbortSignal,
) => {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  const cleanup: { cancelled?: Promise<void> } = {}
  const cancel = () => (cleanup.cancelled ??= reader.cancel().catch(() => undefined))
  const onAbort = () => {
    void cancel()
  }
  signal.addEventListener("abort", onAbort, { once: true })
  if (signal.aborted) onAbort()
  const consume = async () => {
    let buffer = ""
    while (true) {
      const chunk = await reader.read()
      const text = chunk.done ? decoder.decode() + "\n\n" : decoder.decode(chunk.value, { stream: true })
      const incoming = buffer + text
      const trailingCR = !chunk.done && incoming.endsWith("\r") ? "\r" : ""
      buffer = (trailingCR ? incoming.slice(0, -1) : incoming).replaceAll(/\r\n?|\n/g, "\n")
      const blocks = buffer.split("\n\n")
      buffer = (blocks.pop() ?? "") + trailingCR
      for (const block of blocks) {
        if (signal.aborted) return cancel()
        const event = parse(block)
        if (event && (!(await onEvent(event)) || signal.aborted)) return cancel()
      }
      if (chunk.done) return
    }
  }
  await consume().finally(async () => {
    signal.removeEventListener("abort", onAbort)
    await cancel()
    reader.releaseLock()
  })
}

const parse = (block: string): Event | undefined => {
  const fields = block
    .split("\n")
    .filter((line) => line.length > 0 && !line.startsWith(":"))
    .map((line) => {
      const colon = line.indexOf(":")
      const name = colon === -1 ? line : line.slice(0, colon)
      const value = colon === -1 ? "" : line.slice(colon + 1)
      return [name, value.startsWith(" ") ? value.slice(1) : value] as const
    })
  const data = fields.filter(([name]) => name === "data").map(([, value]) => value)
  if (data.length === 0) return undefined
  const event = fields.findLast(([name]) => name === "event")?.[1]
  return { ...(event === undefined ? {} : { event }), data: data.join("\n") }
}

/** Encodes a provider chunk as one event, naming it by its `type` when the provider names its events. */
export const encode = (chunk: unknown, named: boolean) => {
  const name = named && RelayHost.isRecord(chunk) && typeof chunk.type === "string" ? `event: ${chunk.type}\n` : ""
  return `${name}data: ${JSON.stringify(chunk)}\n\n`
}

/** Keep malformed provider data visible to the host's own protocol parser. */
export const encodeEvent = (event: Event) =>
  `${event.event === undefined ? "" : `event: ${event.event}\n`}data: ${event.data.replaceAll("\n", "\ndata: ")}\n\n`

export const RESPONSES = new Set(["openai-responses", "open-responses", "openai-compatible-responses", "xai-responses"])

export interface Accumulator {
  readonly push: (chunk: unknown) => void
  readonly value: () => unknown
}

/** Retains response output/accounting state, not the history of raw streamed frames. */
export const accumulator = (protocol: string): Accumulator => {
  const state =
    protocol === "openai-chat" || protocol === "openai-compatible-chat"
      ? chat()
      : protocol === "anthropic-messages"
        ? messages()
        : RESPONSES.has(protocol)
          ? responses()
          : protocol === "gemini"
            ? gemini()
            : latest()
  return {
    push: (chunk) => {
      if (RelayHost.isRecord(chunk)) state.push(chunk)
    },
    value: state.value,
  }
}

/** Convenience fold using the same incremental state as the live streaming bridge. */
export const aggregate = (protocol: string, chunks: ReadonlyArray<unknown>): unknown => {
  const state = accumulator(protocol)
  chunks.forEach(state.push)
  return state.value()
}

const list = (value: unknown) => (Array.isArray(value) ? value.filter(RelayHost.isRecord) : [])
const text = (value: unknown) => (typeof value === "string" ? value : "")

/** Provider output, not role frames, accounting, signatures, or stream lifecycle events. */
export const outputKind = (protocol: string, chunk: unknown): "text" | "reasoning" | "tool" | undefined => {
  if (!RelayHost.isRecord(chunk)) return
  if (protocol === "openai-chat" || protocol === "openai-compatible-chat") {
    for (const choice of list(chunk.choices)) {
      const delta = RelayHost.isRecord(choice.delta)
        ? choice.delta
        : RelayHost.isRecord(choice.message)
          ? choice.message
          : {}
      if (text(delta.reasoning_content) || text(delta.reasoning)) return "reasoning"
      if (text(delta.content)) return "text"
      if (
        list(delta.tool_calls).some(
          (call) => RelayHost.isRecord(call.function) && (text(call.function.name) || text(call.function.arguments)),
        )
      )
        return "tool"
    }
  }
  if (protocol === "anthropic-messages") {
    const delta = chunk.type === "content_block_delta" && RelayHost.isRecord(chunk.delta) ? chunk.delta : {}
    const block =
      chunk.type === "content_block_start" && RelayHost.isRecord(chunk.content_block) ? chunk.content_block : {}
    if (text(delta.text) || (block.type === "text" && text(block.text))) return "text"
    if (text(delta.thinking) || (block.type === "thinking" && text(block.thinking))) return "reasoning"
    if (text(delta.partial_json) || (block.type === "tool_use" && text(block.name))) return "tool"
    for (const block of list(chunk.content)) {
      if (block.type === "text" && text(block.text)) return "text"
      if (block.type === "thinking" && text(block.thinking)) return "reasoning"
      if (block.type === "tool_use" && text(block.name)) return "tool"
    }
  }
  if (RESPONSES.has(protocol)) {
    if (chunk.type === "response.output_text.delta" && text(chunk.delta)) return "text"
    if (
      ["response.reasoning_text.delta", "response.reasoning_summary_text.delta"].includes(text(chunk.type)) &&
      text(chunk.delta)
    )
      return "reasoning"
    const item = RelayHost.isRecord(chunk.item) ? chunk.item : {}
    if (
      (chunk.type === "response.function_call_arguments.delta" && text(chunk.delta)) ||
      (chunk.type === "response.output_item.added" && item.type === "function_call" && text(item.name))
    )
      return "tool"
    for (const item of list(chunk.output)) {
      if (item.type === "function_call" && text(item.name)) return "tool"
      if (list(item.content).some((part) => part.type === "output_text" && text(part.text))) return "text"
      if (item.type === "reasoning" && list(item.summary).some((part) => text(part.text))) return "reasoning"
    }
  }
  if (protocol === "gemini") {
    const content = list(chunk.candidates)[0]?.content
    if (!RelayHost.isRecord(content)) return
    for (const part of list(content.parts)) {
      if (text(part.text)) return part.thought === true ? "reasoning" : "text"
      if (RelayHost.isRecord(part.functionCall) && text(part.functionCall.name)) return "tool"
    }
  }
}

const chat = () => {
  const metadata: Record<string, unknown> = {}
  const choices = new Map<
    number,
    { content: string; reasoning: string; finish: unknown; calls: Map<number, Record<string, unknown>> }
  >()
  const push = (chunk: Record<string, unknown>) => {
    for (const key of ["id", "created", "model"]) {
      if (metadata[key] === undefined && chunk[key] !== undefined) metadata[key] = chunk[key]
    }
    if (RelayHost.isRecord(chunk.usage)) metadata.usage = chunk.usage
    for (const choice of list(chunk.choices)) {
      const index = typeof choice.index === "number" ? choice.index : 0
      const entry = choices.get(index) ?? { content: "", reasoning: "", finish: null, calls: new Map() }
      choices.set(index, entry)
      const delta = RelayHost.isRecord(choice.delta) ? choice.delta : {}
      entry.content += text(delta.content)
      entry.reasoning += text(delta.reasoning_content)
      if (choice.finish_reason) entry.finish = choice.finish_reason
      list(delta.tool_calls).forEach((call) => {
        const key = typeof call.index === "number" ? call.index : entry.calls.size
        const previous = entry.calls.get(key) ?? { type: "function", function: { name: "", arguments: "" } }
        const fn = RelayHost.isRecord(call.function) ? call.function : {}
        const prior = RelayHost.isRecord(previous.function) ? previous.function : {}
        entry.calls.set(key, {
          ...previous,
          ...(call.id === undefined ? {} : { id: call.id }),
          function: {
            name: text(prior.name) + text(fn.name),
            arguments: text(prior.arguments) + text(fn.arguments),
          },
        })
      })
    }
  }
  const value = () => ({
    id: metadata.id,
    object: "chat.completion",
    created: metadata.created,
    model: metadata.model,
    choices: Array.from(choices, ([index, entry]) => ({
      index,
      message: {
        role: "assistant",
        content: entry.content,
        ...(entry.reasoning ? { reasoning_content: entry.reasoning } : {}),
        ...(entry.calls.size > 0 ? { tool_calls: Array.from(entry.calls.values()) } : {}),
      },
      finish_reason: entry.finish,
    })),
    ...(metadata.usage === undefined ? {} : { usage: metadata.usage }),
  })
  return { push, value }
}

const messages = () => {
  const state: {
    start?: Record<string, unknown>
    delta?: Record<string, unknown>
    usage: Record<string, unknown>
  } = { usage: {} }
  const blocks = new Map<number, Record<string, unknown>>()
  const push = (chunk: Record<string, unknown>) => {
    if (state.start === undefined && chunk.type === "message_start" && RelayHost.isRecord(chunk.message))
      state.start = chunk.message
    if (chunk.type === "message_delta") {
      state.delta = RelayHost.isRecord(chunk.delta) ? chunk.delta : undefined
      if (RelayHost.isRecord(chunk.usage)) Object.assign(state.usage, chunk.usage)
    }
    const index = typeof chunk.index === "number" ? chunk.index : 0
    if (chunk.type === "content_block_start" && RelayHost.isRecord(chunk.content_block))
      blocks.set(index, { ...chunk.content_block })
    const block = blocks.get(index)
    if (chunk.type !== "content_block_delta" || !block || !RelayHost.isRecord(chunk.delta)) return
    const delta = chunk.delta
    if (delta.type === "text_delta") block.text = text(block.text) + text(delta.text)
    if (delta.type === "thinking_delta") block.thinking = text(block.thinking) + text(delta.thinking)
    if (delta.type === "signature_delta") block.signature = text(block.signature) + text(delta.signature)
    if (delta.type === "input_json_delta") block.partial_json = text(block.partial_json) + text(delta.partial_json)
  }
  const value = () => ({
    ...state.start,
    content: Array.from(blocks.values()).map((block) => {
      if (block.partial_json === undefined) return block
      const { partial_json, ...rest } = block
      return { ...rest, input: RelayHost.parse(text(partial_json)) ?? rest.input }
    }),
    ...state.delta,
    usage: { ...(RelayHost.isRecord(state.start?.usage) ? state.start.usage : {}), ...state.usage },
  })
  return { push, value }
}

const responses = () => {
  const state = { response: { object: "response", status: "incomplete", output: [] } as Record<string, unknown> }
  return {
    push: (chunk: Record<string, unknown>) => {
      if (
        ["response.completed", "response.incomplete", "response.failed"].includes(text(chunk.type)) &&
        RelayHost.isRecord(chunk.response)
      )
        state.response = chunk.response
    },
    value: () => state.response,
  }
}

const gemini = () => {
  const state: { last: Record<string, unknown>; parts: Record<string, unknown>[] } = { last: {}, parts: [] }
  return {
    push: (chunk: Record<string, unknown>) => {
      state.last = chunk
      const content = list(chunk.candidates)[0]?.content
      if (RelayHost.isRecord(content)) state.parts.push(...list(content.parts))
    },
    value: () => ({
      ...state.last,
      candidates: [{ ...list(state.last.candidates)[0], content: { role: "model", parts: state.parts } }],
    }),
  }
}

const latest = () => {
  const state: { last: Record<string, unknown> | null } = { last: null }
  return {
    push: (chunk: Record<string, unknown>) => {
      state.last = chunk
    },
    value: () => state.last,
  }
}
