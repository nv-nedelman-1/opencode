import { describe, expect, test } from "bun:test"
import { RelaySSE } from "../src/sse"

const body = (...parts: string[]) =>
  new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder()
      parts.forEach((part) => controller.enqueue(encoder.encode(part)))
      controller.close()
    },
  })

const collect = async (stream: ReadableStream<Uint8Array>, stopAfter = Infinity) => {
  const seen: RelaySSE.Event[] = []
  await RelaySSE.read(stream, (event) => seen.push(event) < stopAfter, new AbortController().signal)
  return seen
}

describe("RelaySSE.read", () => {
  test("parses named events, multi-line data, comments, and CRLF split across chunks", async () => {
    expect(
      await collect(
        body(
          ": keep-alive\r\n\r\nevent: message_start\r",
          '\ndata: {"a":1}\r\n\r\ndata: line one\ndata: line two\n\n',
          "data: [DONE]",
        ),
      ),
    ).toEqual([{ event: "message_start", data: '{"a":1}' }, { data: "line one\nline two" }, { data: "[DONE]" }])
  })

  test("stops reading when the consumer declines more events", async () => {
    expect(await collect(body("data: 1\n\ndata: 2\n\ndata: 3\n\n"), 2)).toEqual([{ data: "1" }, { data: "2" }])
  })

  test("awaits callbacks in order and honors an asynchronous stop", async () => {
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const seen: string[] = []
    const consumed = RelaySSE.read(
      body("data: 1\n\ndata: 2\n\ndata: 3\n\n"),
      async (event) => {
        seen.push(event.data)
        if (event.data === "1") {
          entered.resolve()
          await release.promise
        }
        return event.data !== "2"
      },
      new AbortController().signal,
    )
    await entered.promise
    expect(seen).toEqual(["1"])
    release.resolve()
    await consumed
    expect(seen).toEqual(["1", "2"])
  })

  test("abort cancels the source but waits for the pending callback before releasing its reader", async () => {
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const state = { cancelled: false, completed: false }
    const seen: string[] = []
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: 1\n\ndata: 2\n\n"))
      },
      cancel: () => {
        state.cancelled = true
      },
    })
    const abort = new AbortController()
    const consumed = RelaySSE.read(
      stream,
      async (event) => {
        seen.push(event.data)
        entered.resolve()
        await release.promise
        return true
      },
      abort.signal,
    ).then(() => {
      state.completed = true
    })
    await entered.promise
    abort.abort()
    expect(state).toEqual({ cancelled: true, completed: false })
    expect(stream.locked).toBe(true)
    release.resolve()
    await consumed
    expect(seen).toEqual(["1"])
    expect(stream.locked).toBe(false)
  })

  test("accepts bare CR delimiters and releases the reader after an already-aborted signal", async () => {
    expect(await collect(body("data: 1\r\rdata: 2\r", "\r"))).toEqual([{ data: "1" }, { data: "2" }])
    const stream = body("data: 1\n\n")
    const abort = new AbortController()
    abort.abort()
    await RelaySSE.read(
      stream,
      () => {
        throw new Error("Must not consume after cancellation")
      },
      abort.signal,
    )
    expect(stream.locked).toBe(false)
  })

  test("waits for source cancellation and releases its lock before completing", async () => {
    const cancelled = Promise.withResolvers<void>()
    const state = { settled: false }
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: 1\n\n"))
      },
      cancel: () => cancelled.promise,
    })
    const consumed = RelaySSE.read(stream, () => false, new AbortController().signal).then(() => {
      state.settled = true
    })
    await Promise.resolve()
    expect(state.settled).toBe(false)
    cancelled.resolve()
    await consumed
    expect(stream.locked).toBe(false)
  })
})

describe("RelaySSE.encode", () => {
  test("names events by type only for providers that name their events", () => {
    expect(RelaySSE.encode({ type: "ping" }, true)).toBe('event: ping\ndata: {"type":"ping"}\n\n')
    expect(RelaySSE.encode({ type: "ping" }, false)).toBe('data: {"type":"ping"}\n\n')
  })

  test("preserves invalid JSON frames for the host parser instead of hiding them", () => {
    expect(RelaySSE.encodeEvent({ event: "provider.error", data: "invalid\nJSON" })).toBe(
      "event: provider.error\ndata: invalid\ndata: JSON\n\n",
    )
  })
})

describe("RelaySSE.aggregate", () => {
  test("folds OpenAI chat deltas, tool call fragments, and usage", () => {
    expect(
      RelaySSE.aggregate("openai-compatible-chat", [
        { id: "c", model: "m", choices: [{ index: 0, delta: { role: "assistant", content: "Look" } }] },
        {
          choices: [
            {
              index: 0,
              delta: { tool_calls: [{ index: 0, id: "t1", function: { name: "read", arguments: '{"pa' } }] },
            },
          ],
        },
        {
          choices: [
            {
              index: 0,
              delta: { tool_calls: [{ index: 0, function: { arguments: 'th":"a"}' } }] },
              finish_reason: "tool_calls",
            },
          ],
        },
        { choices: [], usage: { prompt_tokens: 3, completion_tokens: 4 } },
      ]),
    ).toEqual({
      id: "c",
      object: "chat.completion",
      created: undefined,
      model: "m",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: "Look",
            tool_calls: [{ type: "function", id: "t1", function: { name: "read", arguments: '{"path":"a"}' } }],
          },
          finish_reason: "tool_calls",
        },
      ],
      usage: { prompt_tokens: 3, completion_tokens: 4 },
    })
  })

  test("folds Anthropic message events into a message", () => {
    expect(
      RelaySSE.aggregate("anthropic-messages", [
        {
          type: "message_start",
          message: { id: "msg", role: "assistant", model: "claude", content: [], usage: { input_tokens: 9 } },
        },
        { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
        { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Hi" } },
        {
          type: "content_block_start",
          index: 1,
          content_block: { type: "tool_use", id: "tu", name: "read", input: {} },
        },
        { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: '{"path":' } },
        { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: '"a"}' } },
        { type: "message_delta", delta: { stop_reason: "tool_use" }, usage: { output_tokens: 5 } },
        { type: "message_stop" },
      ]),
    ).toEqual({
      id: "msg",
      role: "assistant",
      model: "claude",
      content: [
        { type: "text", text: "Hi" },
        { type: "tool_use", id: "tu", name: "read", input: { path: "a" } },
      ],
      stop_reason: "tool_use",
      usage: { input_tokens: 9, output_tokens: 5 },
    })
  })

  test("uses the completed response of OpenAI Responses streams", () => {
    expect(
      RelaySSE.aggregate("openai-responses", [
        { type: "response.output_text.delta", delta: "Hi" },
        { type: "response.completed", response: { id: "resp", status: "completed", usage: { total_tokens: 3 } } },
      ]),
    ).toEqual({ id: "resp", status: "completed", usage: { total_tokens: 3 } })
  })
})

describe("RelaySSE.accumulator", () => {
  test("updates chat choices, reasoning, first header metadata, and latest usage incrementally", () => {
    const state = RelaySSE.accumulator("openai-chat")
    state.push(null)
    state.push({ id: "first", created: 1, choices: [{ index: 1, delta: { reasoning_content: "Think" } }] })
    expect(state.value()).toMatchObject({ id: "first", created: 1, choices: [{ index: 1, finish_reason: null }] })
    state.push({ model: "model", choices: [{ index: 0, delta: { content: "Hi" } }], usage: { total_tokens: 1 } })
    state.push({ id: "later", choices: [{ index: 1, delta: { reasoning_content: " more" }, finish_reason: "stop" }] })
    state.push({ choices: [], usage: { total_tokens: 3 } })
    expect(state.value()).toEqual({
      id: "first",
      object: "chat.completion",
      created: 1,
      model: "model",
      choices: [
        {
          index: 1,
          message: { role: "assistant", content: "", reasoning_content: "Think more" },
          finish_reason: "stop",
        },
        { index: 0, message: { role: "assistant", content: "Hi" }, finish_reason: null },
      ],
      usage: { total_tokens: 3 },
    })
  })

  test("Anthropic retains block output, merged usage, and the latest message delta without wire history", () => {
    const state = RelaySSE.accumulator("anthropic-messages")
    ;[
      { type: "message_start", message: { id: "first", content: [], usage: { input_tokens: 2 } } },
      { type: "message_start", message: { id: "ignored" } },
      { type: "content_block_start", index: 0, content_block: { type: "thinking", thinking: "", signature: "" } },
      { type: "content_block_delta", index: 0, delta: { type: "thinking_delta", thinking: "Thought" } },
      { type: "content_block_delta", index: 0, delta: { type: "signature_delta", signature: "sig" } },
      { type: "content_block_start", index: 1, content_block: { type: "tool_use", input: { fallback: true } } },
      { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: "invalid" } },
      { type: "message_delta", delta: { stop_reason: "old" }, usage: { output_tokens: 1 } },
      { type: "message_delta", delta: { stop_reason: "tool_use" }, usage: { output_tokens: 3 } },
    ].forEach(state.push)
    expect(state.value()).toEqual({
      id: "first",
      content: [
        { type: "thinking", thinking: "Thought", signature: "sig" },
        { type: "tool_use", input: { fallback: true } },
      ],
      stop_reason: "tool_use",
      usage: { input_tokens: 2, output_tokens: 3 },
    })
  })

  test("Responses retains the latest terminal response, Gemini first-candidate output, and unknown the last record", () => {
    const response = RelaySSE.accumulator("openai-responses")
    expect(response.value()).toEqual({ object: "response", status: "incomplete", output: [] })
    response.push({ type: "response.completed", response: { status: "completed" } })
    response.push({ type: "response.failed", response: { status: "failed" } })
    response.push({ type: "response.output_text.delta", delta: "ignored" })
    expect(response.value()).toEqual({ status: "failed" })

    const gemini = RelaySSE.accumulator("gemini")
    gemini.push({
      candidates: [{ content: { parts: [{ text: "Hi" }] } }, { content: { parts: [{ text: "ignored" }] } }],
    })
    gemini.push({
      candidates: [{ finishReason: "STOP", content: { parts: [{ text: " there" }] } }],
      usageMetadata: { totalTokenCount: 3 },
    })
    expect(gemini.value()).toEqual({
      candidates: [{ finishReason: "STOP", content: { role: "model", parts: [{ text: "Hi" }, { text: " there" }] } }],
      usageMetadata: { totalTokenCount: 3 },
    })
    const unknown = RelaySSE.accumulator("future-protocol")
    unknown.push({ id: "first" })
    unknown.push({ id: "last" })
    unknown.push("not a record")
    expect(unknown.value()).toEqual({ id: "last" })
  })
})
