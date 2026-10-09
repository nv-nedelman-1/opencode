# NeMo Relay V2 integration audit

Experimental in-process integration. This targets OpenCode's `v2` branch, not the older `dev`/V1 implementation. No upstream compatibility or production qualification is claimed yet.

Reference implementations:

- [Earlier observation-only spike](https://github.com/afourniernv/opencode/tree/relay-production), reviewed locally at `130732135ea5b66549acddc6d82bd1fc38b92414`; additional historical reference `relay-skill-metrics` at `37ee6ec3a3635e6a6c272493e304bb8b005a66be`.
- [Nicholas's V2 integration](https://github.com/nv-nedelman-1/opencode/tree/nedelman/v2-relay-native-integration), reviewed at `b77a08fcc0f8240da1ad02f0a47538c7465115ac`.
- [Corresponding native Relay changes](https://github.com/nv-nedelman-1/NeMo-Relay/tree/nedelman/opencode2-native-integration), reviewed at `122bf9bc602ce94356b5d841988749d737ac8802`.

## Attachment shape

The built-in plugin acquires one process-wide Relay host across Location plugin instances. It wraps the actual local-tool callback and physical HTTP model dispatch with Relay managed execution, enabling configured conditional-execution guardrails and request/execution intercepts. It does not require a sidecar or reroute traffic through a gateway.

Each session execution owns a Relay Agent scope. Concurrent operations use propagation-derived isolated scope stacks. Accepted operations retain the host and parent scope until completion, including response-body consumption. Terminal execution events request closure; they do not prematurely close scopes still owned by children or operations.

Additional host observations preserve allowlisted Relay **marks**, with `opencode.observation.source = "host"`, and compile supported values into typed Relay Counter/Histogram measurements. Readonly, non-durable Core hooks provide canonical retry decisions, context estimates, auxiliary usage, compaction outcomes, and permission evaluations. Native provider telemetry remains a separate observation plane.

## Coverage comparison

| Surface                  | Earlier spike                                                | Nicholas's baseline                               | Audit additions or remaining boundary                                                                                              |
| ------------------------ | ------------------------------------------------------------ | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Managed LLM/tool plugins | Observation only; could not intercept execution              | Actual HTTP and local-tool callbacks              | Retains this native shape; prevents replay of an already dispatched operation                                                      |
| Session execution        | Run scopes and summaries                                     | Lazy session scope; terminal close                | Run marks, count/duration instruments, deferred closure, and operation ownership                                                   |
| Step records             | Host processor/step observations                             | No custom step observations                       | Record marks and count/duration instruments; retries preserve the same observed message record                                     |
| Provider tokens and cost | Explicit custom metric instruments                           | Relay codec-derived observations                  | Host primary-terminal and canonical auxiliary charges produce role-labeled token/cost counters; aggregate compaction is excluded   |
| Auxiliary calls          | Role-labeled title/compaction/generation observations        | HTTP calls are managed                            | Context marks distinguish primary, title, compaction, and generate roles                                                           |
| Host retries             | Counts, selected delay, and normalized error                 | No custom host-retry observation                  | Final policy/hook decisions, selected versus remaining delay, exhaustion, and timeout-limit reasons                                |
| Physical provider I/O    | Lower request-executor attempt/retry instruments             | Physical HTTP calls are managed                   | Actual attempt count/outcome/duration, time-to-headers and meaningful first-output latency; hidden SDK retries are not invented    |
| Tool outcomes            | Local/provider counts and categories                         | Local managed callback lifecycle                  | Actual called-to-terminal duration, bounded terminal result metadata, provider-executed tools and cancellation                     |
| Permissions              | Allow/deny evaluation and human wait                         | Managed tools may encounter permission handling   | Final automatic allow/deny/ask decisions plus waits; terminal cancellation closes every outstanding wait                           |
| Human input              | Question-wait observations                                   | No custom wait observation                        | Form marks and count/duration instruments; answers are never copied                                                                |
| Context                  | Token utilization and compaction estimates                   | Native request payload                            | Structural sizes plus primary measured/estimated input utilization against a known positive context limit                          |
| Compaction               | Outcome, timing, and estimated token changes                 | Provider call is managed                          | Skipped/completed/failed/interrupted outcomes, before/after estimates, retained ratios and auxiliary charges; opaque sizes omitted |
| Skills                   | Tool category in the reviewed reference                      | Skill calls are managed as tools                  | User activation/model tool counters and catalog-change count; prompt attachment counts remain mark data                            |
| Subagents                | Delegation tools observed                                    | Parent-session linkage when active                | Child-session creation marks and active-parent execution scopes; not a complete orchestration graph                                |
| MCP                      | Calls treated as tools                                       | Calls through the tool registry are managed       | Status/resource-change notifications; connection status details and reliable dynamic MCP classification remain gaps                |
| Distribution             | Standalone, desktop, and installer work in separate branches | Bundled Core plugin plus workerd binding boundary | Published npm/standalone/desktop artifacts require separate qualification                                                          |

Source owners: [host runtime](src/host.ts), [HTTP bridge](src/model.ts), [tool bridge](src/tool.ts), [host observer](src/observe.ts), [metric compiler](src/metrics.ts), and [plugin registration](src/index.ts).

## Host metric instruments

Recognized counted marks produce `<mark-name>.count` Counter instruments. Duration histograms require an observed start; missing boundaries produce no sample. The added families are:

| Instruments                                                                                                                  | Source and counting boundary                                                                                    |
| ---------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `opencode.llm.tokens`, `cost_usd`                                                                                            | Primary terminal accounting and canonical title/compaction usage, labeled by role and accounting source         |
| `opencode.llm.logical.completed.count`, `logical.duration`, `opencode.model_route.count`, `opencode.llm.finish_reason.count` | Observed assistant-step terminal, not every physical HTTP attempt                                               |
| `opencode.llm.provider_attempt.started.count`, `completed.count`, `duration`, `time_to_headers`, `provider_route.count`      | Each managed physical HTTP dispatch and actual transport/body completion                                        |
| `opencode.llm.time_to_first_output`                                                                                          | First nonempty text/reasoning or actual tool output, with separate `logical_step` and `provider_attempt` scopes |
| `opencode.llm.error.count`                                                                                                   | Separate physical I/O failures and host-normalized logical provider failures; cancellations are excluded        |
| `opencode.llm.retry.decision.count`, `retry.error.count`, `provider_retry.scheduled.count`, `provider_retry.delay`           | Final host retry decision and selected delay; not hidden SDK-internal policy                                    |
| `opencode.tool.duration`, `terminal_result.count`                                                                            | Actual Tool.Called to terminal, plus explicit exit/signal/timeout/background metadata                           |
| `opencode.permission.evaluated.count`                                                                                        | Final configured/hook allow, deny, or ask decision                                                              |
| `opencode.llm.input_context_utilization`                                                                                     | Primary host measured plus estimated prompt tokens divided by a known positive context limit                    |
| `opencode.compaction.outcome.count`, `context_tokens`, `estimated_tokens`, `retained_ratio`                                  | Actual outcome and source-provided estimates; skipped attempts stay distinct from executed compactions          |
| `opencode.agent.run.operation_count`                                                                                         | Per-run observed step starts, tool calls, scheduled retries, human waits, and executed compactions              |
| `opencode.runtime.activation.count`                                                                                          | Once after successful process-wide initialization, not once per Location                                        |

Token totals overlap their components: `input_total` includes non-cached input plus cache read/write; `output_total` includes visible output plus reasoning. Do not sum totals and components. Host cost is labeled `host_calculated`, not provider-billed cost: a zero with no known pricing does not prove that inference was free. Aggregate compaction usage repeats request charges and is never fed into these counters.

Context hooks produce `opencode.context.messages`, `parts`, `system_parts`, `system_characters`, and `tools` Histograms. These are distributions per context-hook observation, not a process-global gauge that concurrent sessions overwrite or a count of provider attempts. Later hooks may mutate the request, and compaction adds its summary prompt or reminder afterward, so these are not final transmitted payload sizes. Optional fields that were not observed produce no sample.

Every measurement has `opencode.metric.schema_version = "1"` and host-source attribution. Only applicable finite outcome/category/family/role/error dimensions are copied. Session/event IDs, arbitrary tool/model/skill names, resources, and payload content never become metric attributes. Configured provider IDs use an exact finite family allowlist; model families are bounded name-prefix classifications, not backend identity verification. Unknown aliases remain `other`.

Metric export requires a configured Relay metrics subscriber and a metrics-capable destination. Phoenix trace visibility alone does not verify OTLP metric delivery. The compiler preserves the original marks so event-based trace/log inspection remains available.

The native provider accounting plane, host billing counters, physical and logical error views, and physical versus logical first-output samples must not be summed as independent activity. Native time-to-first-chunk can include metadata-only frames; meaningful output latency excludes them. Physical `cancelled` means I/O/consumer abortion, which can also result from a chunk timeout, not necessarily a user cancellation. Host logical duration uses the stream-end boundary when observed and otherwise the step terminal, labeled explicitly.

## Observation semantics

- `session.step.started` is published lazily after provider execution has begun. Its `started` field preserves the earlier source dispatch time. The observer emits marks and retains the earliest start for repeated pre-output retries; it does **not** fabricate a step scope and retroactively parent provider spans beneath it.
- An assistant-message step record is not the same as an agent-step allowance. Incomplete-stream continuation can create another message record without consuming another allowance. These marks count observed records, not an inferred user-turn or allowance count.
- Retry `remaining_delay_ms` is `max(0, at - event.created)`; the new decision hook separately records the final selected delay. Neither proves that the whole delay elapsed. Specialized overflow recovery bypasses generic retry policy and therefore does not generate a fictional policy decision.
- A skill-tool success means that the skill call completed successfully. It does not prove that the model applied the instructions. Names, paths, instruction text, and skill identities are not copied into custom marks.
- Primary usage comes from host step terminals; title and compaction charges come from the actual auxiliary accounting owner. Provider accounting remains Relay's native codec plane.
- Run step counts describe observed assistant-message starts. Continuation and overflow recovery can create another record without consuming another runner allowance, and overflow recovery can abandon an old record without a terminal event. No synthetic successful completion or user-turn count is invented.
- Tool result families use explicit structured terminal metadata, never output-text matching. Provider-executed identity is authoritative; local versus MCP identity remains `unknown` where the host registry does not provide it.
- Context estimates precede later request-hook mutations and are not final wire tokenization. Opaque native-compaction sizes remain absent rather than zero. Skipped compactions do not increment the executed-compaction run summary.
- Event delivery is a volatile, Location-scoped live stream. Process-wide admission deduplicates recent event IDs across observers before mutating scopes. This is bounded duplicate protection, not persistent or exactly-once accounting. Missing start events omit duration rather than inventing one.
- Forms may have a non-session `global` owner. Those observations are explicitly unparented; no fake session is created.

The event surface is defined in [SessionEvent](../schema/src/session-event.ts) and [EventManifest](../schema/src/event-manifest.ts). The [plugin host](../core/src/plugin/host.ts) excludes internal `session.usage.recorded` events; the generic readonly `session.usage` hook observes those charges directly without expanding the durable server protocol or introducing Relay into Core contracts.

## Privacy and transport limits

Custom marks allow only counts, source timestamps/durations, bounded outcomes/reasons/categories, and fixed-schema host accounting. Error messages, response bodies, permission resources, form answers, tool input/output, and arbitrary metadata are excluded. Event IDs are used for bounded deduplication.

This whitelist applies to the added **host marks**, not every native Relay model/tool payload. Managed callbacks intentionally give Relay the actual payload for configured policy processing. Configure Relay's content policies and sanitizer plugins according to the deployment's privacy requirements.

HTTP support must not be mistaken for universal transport coverage:

- Signed Bedrock/SigV4 requests are explicitly unmanaged; rewriting a signed wire payload would invalidate its signature.
- Non-JSON payloads are not passed through the current JSON bridge.
- WebSocket-native model traffic does not cross the HTTP middleware. Public experimental WebSocket hooks exist, but this plugin does not yet offer equivalent managed execution for that transport.
- Generic or newly added protocols are not automatically codec-conformant. Qualify each protocol's unary shape, streaming frames, aggregate response, and metadata-only export before claiming support.

The AI-SDK adapter identifies its route as `ai-sdk`, not the actual provider wire protocol. The Relay bridge therefore cannot promise structured provider decoding on that path. A protocol annotation must come from the component selecting the actual SDK/language model; guessing from model names or provider labels is unsafe because hooks can replace that selection.

Serial producer pushes now await native consumption, bounding chunk-count read-ahead through the native bridge; the synchronous native push API remains available for compatibility and is not bounded. The integration retains incremental aggregate response output, not the full raw-frame history. This is not a byte-memory limit: large network chunks, oversized frames, and the accumulated response itself still need workload qualification and deployment limits.

## Activation and cleanup

An unavailable optional native runtime disables the adapter with a warning. Once a binding loads, incompatible streaming APIs, invalid selected configuration, and failed selected plugins instead register blocking tool/HTTP middleware and a blocking WebSocket-handshake hook. Unselected dynamic-plugin failures do not independently block activation. Raw configuration details are not copied into those errors.

Shutdown stops admission, cancels operations, and waits within its budget. Late operations retain the native host until they drain. A failed native unload is a sticky, restart-required failure; the next Location cannot start a competing activation. Subscriber flush errors are not proof of successful delivery, and exporter qualification remains separate.

Configuration is process-global and selected by the first acquiring Location; per-Location programmatic overrides are not reconciled. Binding load and native initialization still have no startup deadline. Cold-start/stalled-initializer qualification and a bounded late-activation cleanup design remain open lifecycle work.

## Qualification

The audit tests use a native addon built from the corresponding Relay source and its corrections, not just the originally pinned published `0.10.0-rc.4` package. Built-in codec-instance identity, explicit `failStream`, and awaitable `pushStreamChunkAsync` require the appropriate Node binding changes. The current pin cannot activate this hardened adapter. The latest inspected release, `0.10.0`, also lacks the required streaming APIs. A released dependency containing those changes must be selected and requalified before shipping; no speculative version bump is made here.

Focused verification from this package:

```sh
bun typecheck
bun test
```

Run the repository's canonical `bun run check` separately from the root. Native lifecycle tests cover overlapping sessions, child/parent closure, cancellation, host retirement, and isolated propagation. Observer tests cover interleaving, duplicate Location delivery, retries, missing timing boundaries, provider-tool summaries, and privacy whitelists. Metric tests exercise numeric/attribute validation, missing samples, exclusion of provider accounting, and real native serialization into the typed metric-event schema.

The October 9 live qualification uses real NIM inference through the V2 harness and the source-built binding. Two concurrent Locations produced four logical step completions, five physical attempts (including one real retry), and two successful reads. Phoenix accepted the traces; exact timestamp/parent checks found no cross-session or containment violations. OTLP/protobuf metrics were decoded separately. That cohort also exposed a retry-attribute schema collision, fixed afterward and qualified separately rather than rewriting the earlier result.

Post-fix edge workloads exported three real retry decisions (HTTP 200 transport timeout and two HTTP 429 responses), with selected-delay samples matching host observations. Rejected and interrupted permission waits each produced one completion before their parent closed. An initial rejection assertion raced asynchronous event delivery; its failed report is preserved, and a rerun awaiting the reply observation passed streaming cancellation, permission rejection, and permission interruption. Both cohorts have valid exact-timestamp span parentage and matching Phoenix delivery. A separate isolated native/protobuf probe verifies exhausted decisions do not emit scheduled-retry metrics. Final source checks passed 123 Relay-plugin tests, 148 targeted Core tests, and the repository's canonical check.

Remaining qualification includes live auxiliary pricing/cache/reasoning/actual compaction cases, more providers, long-duration soak, WebSocket coverage decisions, and packaged artifacts. A passing synthetic workload is not production-readiness or universal metric-coverage proof. Deterministic lifecycle regressions additionally cover terminal-hook interruption and retention of unary/tool native execution tails after host callbacks return.
