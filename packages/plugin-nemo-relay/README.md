# NeMo Relay V2 integration audit

Experimental in-process integration. This targets OpenCode's `v2` branch, not the older `dev`/V1 implementation. No upstream compatibility or production qualification is claimed yet.

Reference implementations:

- [Earlier observation-only spike](https://github.com/afourniernv/opencode/tree/relay-production), reviewed locally at `130732135ea5b66549acddc6d82bd1fc38b92414`; additional historical reference `relay-skill-metrics` at `37ee6ec3a3635e6a6c272493e304bb8b005a66be`.
- [Nicholas's V2 integration](https://github.com/nv-nedelman-1/opencode/tree/nedelman/v2-relay-native-integration), reviewed at `b77a08fcc0f8240da1ad02f0a47538c7465115ac`.
- [Corresponding native Relay changes](https://github.com/nv-nedelman-1/NeMo-Relay/tree/nedelman/opencode2-native-integration), reviewed at `122bf9bc602ce94356b5d841988749d737ac8802`.

## Attachment shape

The built-in plugin acquires one process-wide Relay host across Location plugin instances. It wraps the actual local-tool callback and physical HTTP model dispatch with Relay managed execution, enabling configured conditional-execution guardrails and request/execution intercepts. It does not require a sidecar or reroute traffic through a gateway.

Each session execution owns a Relay Agent scope. Concurrent operations use propagation-derived isolated scope stacks. Accepted operations retain the host and parent scope until completion, including response-body consumption. Terminal execution events request closure; they do not prematurely close scopes still owned by children or operations.

Additional host observations preserve allowlisted Relay **marks**, with `opencode.observation.source = "host"`, and compile their supported counts/durations into typed Relay Counter/Histogram measurements. The resulting metric plane complements native provider telemetry; the remaining gaps below still prevent full parity with the earlier spike.

## Coverage comparison

| Surface                   | Earlier spike                                                | Nicholas's baseline                               | Audit additions or remaining boundary                                                                               |
| ------------------------- | ------------------------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Managed LLM/tool plugins  | Observation only; could not intercept execution              | Actual HTTP and local-tool callbacks              | Retains this native shape; prevents replay of an already dispatched operation                                       |
| Session execution         | Run scopes and summaries                                     | Lazy session scope; terminal close                | Run marks, count/duration instruments, deferred closure, and operation ownership                                    |
| Step records              | Host processor/step observations                             | No custom step observations                       | Record marks and count/duration instruments; retries preserve the same observed message record                      |
| Provider tokens and cost  | Explicit custom metric instruments                           | Relay codec-derived observations                  | Host step/compaction accounting is separately labeled; not an additional provider-token counter                     |
| Auxiliary calls           | Role-labeled title/compaction/generation observations        | HTTP calls are managed                            | Context marks distinguish primary, title, compaction, and generate roles                                            |
| Host retries              | Counts, selected delay, and normalized error                 | No custom host-retry observation                  | Scheduled count and remaining-delay histogram; bounded error marks, not exact selected backoff                      |
| Provider-internal retries | Lower request-executor attempt/retry instruments             | Physical HTTP calls are managed                   | Exact internal retry decisions and time-to-headers still need an owner hook                                         |
| Tool outcomes             | Local/provider counts and categories                         | Local managed callback lifecycle                  | Host outcome counters include provider-executed tools and cancellation; no guessed execution duration               |
| Permissions               | Allow/deny evaluation and human wait                         | Managed tools may encounter permission handling   | Public wait/reply marks and count/duration instruments; automatic allow/deny evaluation still lacks an owner hook   |
| Human input               | Question-wait observations                                   | No custom wait observation                        | Form marks and count/duration instruments; answers are never copied                                                 |
| Context                   | Token utilization and compaction estimates                   | Native request payload                            | Per-request structural-size histograms; not tokenizer estimates or utilization ratios                               |
| Compaction                | Outcome, timing, and estimated token changes                 | Provider call is managed                          | Count/duration instruments and host recorded usage/cost marks; no fabricated before/after estimates                 |
| Skills                    | Tool category in the reviewed reference                      | Skill calls are managed as tools                  | User activation/model tool counters and catalog-change count; prompt attachment counts remain mark data             |
| Subagents                 | Delegation tools observed                                    | Parent-session linkage when active                | Child-session creation marks and active-parent execution scopes; not a complete orchestration graph                 |
| MCP                       | Calls treated as tools                                       | Calls through the tool registry are managed       | Status/resource-change notifications; connection status details and reliable dynamic MCP classification remain gaps |
| Distribution              | Standalone, desktop, and installer work in separate branches | Bundled Core plugin plus workerd binding boundary | Published npm/standalone/desktop artifacts require separate qualification                                           |

Source owners: [host runtime](src/host.ts), [HTTP bridge](src/model.ts), [tool bridge](src/tool.ts), [host observer](src/observe.ts), [metric compiler](src/metrics.ts), and [plugin registration](src/index.ts).

## Host metric instruments

Recognized counted marks produce `<mark-name>.count` Counter instruments. Run/step/compaction completion and permission/form completion also produce `opencode.<domain>.duration` Histograms when an observed source start exists. Host retry scheduling has a separate `opencode.llm.host_retry.remaining_delay` Histogram.

Context hooks produce `opencode.context.messages`, `parts`, `system_parts`, `system_characters`, and `tools` Histograms. These are distributions per context-hook observation, not a process-global gauge that concurrent sessions overwrite or a count of provider attempts. Later hooks may mutate the request, and compaction adds its summary prompt or reminder afterward, so these are not final transmitted payload sizes. Optional fields that were not observed produce no sample.

Every measurement has `opencode.metric.schema_version = "1"` and host-source attribution. Only applicable bounded outcome/category/provider-executed/family/resolution/call-role dimensions are copied. Session/event IDs, arbitrary tool/model/skill names, resources, and payload content never become metric attributes. Native provider token/cost values are deliberately not recorded again by this compiler.

Metric export requires a configured Relay metrics subscriber and a metrics-capable destination. Phoenix trace visibility alone does not verify OTLP metric delivery. The compiler preserves the original marks so event-based trace/log inspection remains available.

Native provider token/cost span attributes are not equivalent to the earlier spike's token/cost Counter instruments. Per-run operation summaries, finish-reason/model-route counters, terminal exit-result families, and exact host time-to-first-output also remain separate parity work.

## Observation semantics

- `session.step.started` is published lazily after provider execution has begun. Its `started` field preserves the earlier source dispatch time. The observer emits marks and retains the earliest start for repeated pre-output retries; it does **not** fabricate a step scope and retroactively parent provider spans beneath it.
- An assistant-message step record is not the same as an agent-step allowance. Incomplete-stream continuation can create another message record without consuming another allowance. These marks count observed records, not an inferred user-turn or allowance count.
- Retry `remaining_delay_ms` is `max(0, at - event.created)`. It is not the originally selected backoff delay or proof that the full delay elapsed.
- A skill-tool success means that the skill call completed successfully. It does not prove that the model applied the instructions. Names, paths, instruction text, and skill identities are not copied into custom marks.
- Host step and compaction usage comes from OpenCode's accounting events. Provider accounting remains Relay's native codec plane. These are separate views of the same activity and must not be summed as independent consumption.
- Event delivery is a volatile, Location-scoped live stream. Process-wide admission deduplicates recent event IDs across observers before mutating scopes. This is bounded duplicate protection, not persistent or exactly-once accounting. Missing start events omit duration rather than inventing one.
- Forms may have a non-session `global` owner. Those observations are explicitly unparented; no fake session is created.

The event surface is defined in [SessionEvent](../schema/src/session-event.ts) and [EventManifest](../schema/src/event-manifest.ts); the [plugin host](../core/src/plugin/host.ts) exposes public server events, not internal `session.usage.recorded` events. Consequently there is no independently sourced title-usage mark in this observer.

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

The audit tests use a native addon built from the corresponding Relay source and its corrections, not just the originally pinned published `0.10.0-rc.4` package. Built-in codec-instance identity, explicit `failStream`, and awaitable `pushStreamChunkAsync` require the appropriate Node binding changes. The current pin cannot activate this hardened adapter. A released dependency containing those changes must be selected and requalified before shipping; no speculative version bump is made here.

Focused verification from this package:

```sh
bun typecheck
bun test
```

Run the repository's canonical `bun run check` separately from the root. Native lifecycle tests cover overlapping sessions, child/parent closure, cancellation, host retirement, and isolated propagation. Observer tests cover interleaving, duplicate Location delivery, retries, missing timing boundaries, provider-tool summaries, and privacy whitelists. Metric tests exercise numeric/attribute validation, missing samples, exclusion of provider accounting, and real native serialization into the typed metric-event schema.

Remaining qualification includes live inference through the V2 harness, actual Phoenix exporter delivery and topology inspection, failure/cancellation workloads, WebSocket coverage decisions, and packaged artifacts. Mocked providers and local native subscribers are useful contract tests; they are not substitutes for those checks.
