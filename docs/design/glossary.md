# Glossary

Terms used by the implementation and documentation. Engine field names are mapped to
tap terminology at the adapter boundary.

| term | meaning | where it lives |
|---|---|---|
| **engine** | The inference server whose cache the tap observes: vLLM or SGLang. | `--engine`, `source::Engine` |
| **adapter** | The code that maps an engine's native telemetry to the event model. vLLM and SGLang share the wire format, so one adapter serves both. | `src/source/wire/` |
| **profile** | What differs per engine on the shared wire: the known fields, the hash width, the salt's place, which tiers derive, and what a store means. One unit struct per engine module. | `wire::Profile`, `src/source/vllm.rs`, `src/source/sglang.rs` |
| **transport** | What delivers raw engine frames to the adapter: the live ZMQ SUB socket, or an in-memory queue in tests. | `FrameTransport`, `src/source/wire/transport.rs` |
| **source** | The transport key for one subscription: the label given on `--endpoints` or `--engine-endpoint`, with a `#<rank>` suffix under data parallelism. The sequence tracker and the content bridge key on it. | `EventSource::seen(source)`, `Opts.endpoints` |
| **instance** | The configured publisher label in `instance_id`. Local data-parallel ranks share a label; packaged Kubernetes sidecars use a pod-scoped label. | `Opts.labels`, `Record::Store::instance_id` |
| **origin** | The scope a block hash is unique within: `(dp_rank, group_idx)`. | `event_model::Origin` |
| **event** | One normalized cache event from the adapter: `Stored`, `Removed`, `Cleared`, `HolderReset`. | `event_model::Event` |
| **transducer** | The stateless step from one event to one record. | `compute::Transducer` |
| **record** | One line of the stream, typed: `store`, `evict`, `clear`, `holder_reset`, the lifecycle kinds, the delivery kinds. | `record::Record` |
| **sink** | Where records go: a decorator (pseudonymizing, the size guard) or a destination (segments, a file, memory). | `sink::Sink` |
| **egress** | The output pipeline. Pseudonymization occurs before local writing; the separate shipper transfers sealed files off the host. | `src/sink/pseudonymizing.rs` |
| **identity** | An identifier in the engine-block or content identity space. These spaces have distinct scopes and must not be joined. | `record::Id` |
| **operator text** | Operator-controlled free text that can name a tenant: a cache salt in `extra_keys`, a `lora_name`, or backend ownership. Mapped at egress under its own context. | `record::OperatorText` |
| **tier** | The cache tier a block sits in (`GPU`, `CPU`, ...). The wire calls it `medium`; the adapter renames it at the boundary. | `Event::Stored::tier` |
| **holder** | One `(instance, origin)`: the unit that holds a block and can refuse an identity promise. | `identity_refused` records |
| **content bridge** | The adapter's per-source map from engine hash to content identity, bounded per origin and in total. Its refusals ship as `content_bridge_evicted` on the heartbeat, a wire name kept for the contract; the field counts refusals, not evictions. | `src/source/wire/content.rs` |
| **segment** | One sealed file of the stream, `seg-<incarnation>-<seq>`. The write-ahead form is JSONL; the shipped form is its Parquet twin. | `src/sink/segments/` |
| **write-ahead segment** | A sealed segment in its JSONL form, terminal to its writer and awaiting its Parquet twin. Never ships; deleted once the twin is verified and committed. | `src/sink/segments/worker.rs` |
| **incarnation** | One process lifetime, `<run_start_ms>-<pid>`; segments are numbered within it. | `SegmentedSink::incarnation` |
| **orphan** | An `active.jsonl` a dead incarnation left behind. With its header intact it seals at the next open under its own identity; without one it is discarded and the discard is declared in the new incarnation's first segment. | `src/sink/segments/recovery.rs` |
| **ring** | The disk cap: oldest sealed segments reclaimed past `--max-disk-bytes`, each drop declared in-band. | `src/sink/segments/ring.rs` |
| **shipper** | Whatever moves sealed Parquet segments off the node: the systemd timer unit or the sidecar's script. Not part of the binary. | `packaging/` |
| **stream** | One telemetry type's directory under `--out-dir`; today only `kv/`. | `docs/streams.md` |
| **contract** | The inference-events specification and its conformance corpora, pinned by `SPEC_PIN`. | `conformance/` |
