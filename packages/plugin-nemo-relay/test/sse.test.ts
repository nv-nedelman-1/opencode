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
})

describe("RelaySSE.encode", () => {
  test("names events by type only for providers that name their events", () => {
    expect(RelaySSE.encode({ type: "ping" }, true)).toBe('event: ping\ndata: {"type":"ping"}\n\n')
    expect(RelaySSE.encode({ type: "ping" }, false)).toBe('data: {"type":"ping"}\n\n')
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
