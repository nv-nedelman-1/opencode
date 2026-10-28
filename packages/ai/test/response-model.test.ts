import { expect } from "bun:test"
import { LLM, LLMClient, LLMEvent } from "../src/index.js"
import { Auth } from "../src/route.js"
import { AnthropicMessages, Gemini, OpenAIChat, OpenAICompatibleResponses } from "../src/protocols.js"
import { Effect, Stream } from "effect"
import { it } from "./lib/effect.js"
import { fixedResponse } from "./lib/http.js"
import { sseEvents } from "./lib/sse.js"

for (const fixture of [
  {
    name: "OpenAI Chat",
    route: OpenAIChat.route,
    frames: (responseModel?: unknown) => [
      { model: responseModel, choices: [{ index: 0, delta: { content: "answer" } }] },
      { choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
    ],
  },
  {
    name: "Anthropic Messages",
    route: AnthropicMessages.route,
    frames: (responseModel?: unknown) => [
      { type: "message_start", message: { model: responseModel } },
      { type: "content_block_start", index: 0, content_block: { type: "text", text: "answer" } },
      { type: "content_block_stop", index: 0 },
      { type: "message_delta", delta: { stop_reason: "end_turn" } },
      { type: "message_stop" },
    ],
  },
  {
    name: "Open Responses",
    route: OpenAICompatibleResponses.route,
    frames: (responseModel?: unknown) => [
      { type: "response.created", response: { model: responseModel } },
      { type: "response.completed", response: { id: "response" } },
    ],
  },
  {
    name: "Gemini",
    route: Gemini.route,
    frames: (responseModel?: unknown) => [
      { modelVersion: responseModel, candidates: [{ content: { parts: [{ text: "answer" }] } }] },
      { candidates: [{ finishReason: "STOP" }] },
    ],
  },
]) {
  for (const responseModel of ["accepted-version", undefined, null, 42, "", "  "]) {
    const expected = typeof responseModel === "string" && responseModel.trim() ? responseModel : undefined
    it.effect(`${fixture.name} observes response model ${JSON.stringify(responseModel)} without inventing identity`, () =>
      Effect.gen(function* () {
        const request = LLM.request({
          model: fixture.route
            .with({
              endpoint: { baseURL: "https://provider.test" },
              auth: Auth.bearer("test"),
            })
            .model({ id: "configured-alias", provider: "configured" }),
          prompt: "Respond.",
        })
        const events = yield* LLMClient.stream(request).pipe(
          Stream.runCollect,
          Effect.provide(fixedResponse(sseEvents(...fixture.frames(responseModel)))),
        )
        const finishes = events.filter(LLMEvent.is.finish)
        expect(finishes).toHaveLength(1)
        expect(finishes[0].responseModel).toBe(expected)
        const response = yield* LLMClient.generate(request).pipe(
          Effect.provide(fixedResponse(sseEvents(...fixture.frames(responseModel)))),
        )
        expect(response.responseModel).toBe(expected)
        expect(response.responseModel).not.toBe("configured-alias")
      }),
    )
  }
}
