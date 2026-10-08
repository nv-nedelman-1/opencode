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
  onEvent: (event: Event) => boolean,
  signal: AbortSignal,
) => {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  const cancel = () => void reader.cancel().catch(() => undefined)
  signal.addEventListener("abort", cancel, { once: true })
  const consume = async () => {
    let buffer = ""
    while (true) {
      const chunk = await reader.read()
      const text = chunk.done ? decoder.decode() + "\n\n" : decoder.decode(chunk.value, { stream: true })
      buffer = (buffer + text).replaceAll("\r\n", "\n")
      const blocks = buffer.split("\n\n")
      buffer = blocks.pop() ?? ""
      const events = blocks.map(parse).filter((event): event is Event => event !== undefined)
      if (!events.every(onEvent)) return cancel()
      if (chunk.done) return
    }
  }
  await consume().finally(() => signal.removeEventListener("abort", cancel))
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

/** Folds streamed chunks into the provider's non-streaming response shape so response codecs can read usage. */
export const aggregate = (protocol: string, chunks: ReadonlyArray<unknown>): unknown => {
  const records = chunks.filter(RelayHost.isRecord)
  if (protocol === "openai-chat" || protocol === "openai-compatible-chat") return chat(records)
  if (protocol === "anthropic-messages") return messages(records)
  if (RESPONSES.has(protocol)) return responses(records)
  if (protocol === "gemini") return gemini(records)
  return records.at(-1) ?? null
}

export const RESPONSES = new Set(["openai-responses", "open-responses", "openai-compatible-responses", "xai-responses"])

const list = (value: unknown) => (Array.isArray(value) ? value.filter(RelayHost.isRecord) : [])
const text = (value: unknown) => (typeof value === "string" ? value : "")

const chat = (records: ReadonlyArray<Record<string, unknown>>) => {
  const choices = new Map<
    number,
    { content: string; reasoning: string; finish: unknown; calls: Map<number, Record<string, unknown>> }
  >()
  records
    .flatMap((chunk) => list(chunk.choices))
    .forEach((choice) => {
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
    })
  const usage = records.findLast((chunk) => RelayHost.isRecord(chunk.usage))?.usage
  return {
    id: records.find((chunk) => chunk.id !== undefined)?.id,
    object: "chat.completion",
    created: records.find((chunk) => chunk.created !== undefined)?.created,
    model: records.find((chunk) => chunk.model !== undefined)?.model,
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
    ...(usage === undefined ? {} : { usage }),
  }
}

const messages = (records: ReadonlyArray<Record<string, unknown>>) => {
  const start = records.find((chunk) => chunk.type === "message_start" && RelayHost.isRecord(chunk.message))?.message
  const blocks = new Map<number, Record<string, unknown>>()
  const deltas = records.filter((chunk) => chunk.type === "message_delta")
  records.forEach((chunk) => {
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
  })
  const base = RelayHost.isRecord(start) ? start : {}
  const last = deltas.at(-1)
  const usage = deltas.reduce(
    (merged, delta) => (RelayHost.isRecord(delta.usage) ? { ...merged, ...delta.usage } : merged),
    RelayHost.isRecord(base.usage) ? base.usage : {},
  )
  return {
    ...base,
    content: Array.from(blocks.values()).map((block) => {
      if (block.partial_json === undefined) return block
      const { partial_json, ...rest } = block
      return { ...rest, input: RelayHost.parse(text(partial_json)) ?? rest.input }
    }),
    ...(last && RelayHost.isRecord(last.delta) ? { ...last.delta } : {}),
    usage,
  }
}

const responses = (records: ReadonlyArray<Record<string, unknown>>) =>
  records.findLast(
    (chunk) =>
      (chunk.type === "response.completed" ||
        chunk.type === "response.incomplete" ||
        chunk.type === "response.failed") &&
      RelayHost.isRecord(chunk.response),
  )?.response ?? { object: "response", status: "incomplete", output: [] }

const gemini = (records: ReadonlyArray<Record<string, unknown>>) => {
  const last = records.at(-1) ?? {}
  const candidate = list(last.candidates)[0] ?? {}
  const parts = records.flatMap((chunk) => {
    const content = list(chunk.candidates)[0]?.content
    return RelayHost.isRecord(content) ? list(content.parts) : []
  })
  return { ...last, candidates: [{ ...candidate, content: { role: "model", parts } }] }
}
