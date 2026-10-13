# Adapter and record pipeline

The tap converts engine events into inference-events records. The public wire schema,
identity constructions, and consumer requirements belong to the
[inference-events specification](https://github.com/infertap/inference-events), at the
revision in [SPEC_PIN](../SPEC_PIN). This document describes the tap's implementation;
it does not define a separate wire contract or Rust SDK.

## Data flow

```text
Engine publisher → transport → adapter → normalized event → record conversion
  → pseudonymization → output guards → local segments → shipper
```

The transport reads configured publisher endpoints and optionally retrieves replay history.
The adapter interprets engine fields and maintains bounded identity state. Record conversion
maps each event without retaining a residency model. The pseudonymizer maps identifiers
once, before output. Raw mode bypasses that component. Output guards enforce record size
and check for unexpected raw values. The shipper uploads sealed Parquet segments.

## Normalized events

These types are internal. Their definitions are in [src/event_model.rs](../src/event_model.rs).

| Event | Meaning |
| --- | --- |
| `Stored` | A block was announced as stored. Includes scope, parent state, tier, metadata, and an optional content identity. |
| `Removed` | A block was removed. Includes available tier, locality, backend identity, and a content identity resolved from retained state. |
| `Cleared` | The engine cleared a scope. Undeclared rank, group, or tier fields make the clear global over that dimension. |
| `HolderReset` | A publisher sequence regression identifies a restart of the affected source or rank. |

Engine timestamps are converted to milliseconds at the adapter boundary. Producer
lifecycle timestamps use the tap's clock. Consumers must keep these clock domains distinct.

A stored block's parent has three states: root, known hash, or unknown. An unknown parent
is not a root. Conversion preserves that distinction in the record's optional parent fields.

## vLLM mapping

The ZMQ publisher sends `(topic, sequence, payload)` multipart messages. The adapter
reads MessagePack event fields by name. It rejects malformed input and counts unknown
event kinds. Supported engine configurations are listed in [Compatibility](compatibility.md).
`--engine` selects the adapter profile; `vllm` is the default.

| Engine field or event | Tap behavior |
| --- | --- |
| `block_hashes` | Emit one normalized block event per hash. |
| `parent_block_hash` | Parent of the first block in a contiguous stored run. Later blocks use their predecessor. |
| `token_ids` | Derive content identities from complete inputs, then discard the tokens. |
| `extra_keys` | Preserve declared values for pseudonymization as `partition`. |
| `medium` | Map to storage tier. |
| `dp_rank`, `group_idx` | Preserve cache scope. |
| `lora_id`, `lora_name` | Preserve adapter metadata; pseudonymize the name. |
| `kv_cache_spec_kind`, `locality` | Preserve declared metadata. |
| `ownership` | Preserve as optional `backend_id` and pseudonymize at output. |
| `session_id` | Omit; the tap does not provide request attribution. |
| `AllBlocksCleared` | Preserve the declared clear scope and release matching identity state. |

Skipped or masked blocks can prevent a contiguous token-to-hash mapping. In that case,
the adapter does not infer adjacent parents or content identities from list position.
Missing metadata remains absent.

## SGLang mapping

SGLang's `ZmqEventPublisher` uses vLLM's wire format: the same three-frame message, the
same positional batch `[ts, events, attn_dp_rank]`, and the same tagged event maps.
`--engine sglang` selects the SGLang profile of the same transport and adapter. The
profile changes four field mappings, the drift table, and the `reuse_reporting`
declaration. Everything else in this document applies unchanged.

| Engine field or event | Tap behavior |
| --- | --- |
| `block_hashes`, `parent_block_hash` | Signed 64-bit integers: the engine folds the leading 64 bits of its SHA-256 into `int64`, so about half of all hashes arrive negative. The profile reads a negative value as the same 64 bits unsigned. Under the vLLM profile a negative hash remains malformed. |
| `cache_salt` | Carried once per event as a string. The adapter folds a nonempty salt into the first block's extra keys on a run whose parent is absent, the place vLLM carries it, and into no later block. The same content under the same salt yields the same content identity on both engines. An empty salt is absent. |
| `lora_id` | Preserved. SGLang emits no `lora_name`; the tap leaves it absent and does not synthesize one. |
| `medium` | `GPU`, `CPU_PINNED`, `DISK`, and `EXTERNAL` pass through as tier terms without remapping. The contract's tier vocabulary is open. The recorder announces every tier with the same tokens and salt, so a store on any tier derives its content identity. |
| `attn_dp_rank` | The third batch field; preserved as `dp_rank`. |
| `group_idx` | Not on this wire; the single cache group is group 0. |
| `session_id` | Omitted; the tap does not provide request attribution. |
| `BlockRemoved` | Carries `block_hashes` and `medium` only. |
| `AllBlocksCleared` | Carries no fields. The batch's rank is the only declared scope. |

The recorder emits one `BlockStored` per page and coalesces consecutive pages into one
run. Radix keys are page-aligned, so every stored page is full and the hash count times
`block_size` covers the tokens. Bigram keys used by EAGLE speculative decoding expose
token pairs in `token_ids`; the adapter keeps the store, derives no identity, and counts
the omission.

Under the SGLang profile, `cache_salt` and `session_id` are known fields. Under the vLLM
profile they are reported as drift, and vLLM's `extra_keys`, `lora_name`, `group_idx`,
`kv_cache_spec_kind`, `locality`, and `ownership` are drift under the SGLang profile.
The drift table stays per profile.

The SGLang profile declares `reuse_reporting: none`. SGLang's radix cache records a store
only when it creates a tree node. A prefix hit walks existing nodes and records nothing.
A node split records nothing. The hierarchical cache records a host-tier store when a
write-through completes and a GPU store when it loads a block back from the host; each is
a block entering that tier. Every `store` is an insertion into its tier. This declaration
is a fact about the engine, so it is constant for an incarnation.

Each SGLang data-parallel rank publishes on the configured base port plus its rank, and
the replay port follows the same offset. The tap's `--dp-*` flags derive per-rank ports
the same way for both engines; see [Data parallelism](install.md#data-parallelism).

## Identity state

Engine block hashes and content identities are separate identity spaces. Engine hashes
are scoped to a holder and incarnation. Content identities use the contract's 128-bit
content construction and fixed root, independent of the engine's hash algorithm and seed.
An absent content identity is never replaced with an engine hash.

The content bridge maps `(rank, group, engine hash)` to an optional content identity.
Each entry counts outstanding store announcements by tier and optional backend. Removals
release announcements; clear and restart events release matching state. Incomplete offload
announcements reuse an established identity when available. Contradictory complete
announcements invalidate further lookups until release or clear.

Entries and scope names have fixed capacity limits. A refusal omits the affected identity
and produces accounting records; it does not merge scopes or suppress the underlying event.
Repeated announcements or missing removals can retain state beyond physical residency.
See [resource limits](trust.md#3-resource-limits).

Backend identity is optional. Its absence is normal tier-level scope and causes no warning.
Identifiers from separate publishers do not by themselves establish a shared physical cache.

## Replay and loss accounting

On attachment, replay retrieves the publisher's available history within bounded time and
byte budgets. The transport deduplicates overlapping live messages by sequence. Expired,
truncated, or unavailable history can leave cache state incomplete.

Sequence gaps, unknown kinds, oversized messages, and unresolved identities have distinct
counters. These counters describe collection limits; they do not reconstruct missing events.
A publisher restart invalidates its retained identity state. Segment and lifecycle records
provide the additional continuity information defined by the public contract.

## Record contract and validation

The vendored inference-events package supplies typed wire records and structural validation.
JSON Schema and physical column definitions belong to that package. Stream interpretation,
key-space comparison, continuity, and consumer obligations belong to the pinned specification.

See [Conformance](conformance-notes.md) for pin updates and fixture ownership,
[Storage layout](streams.md) for local and uploaded files, and
[Security and resource bounds](trust.md) for output controls and recovery limits.
