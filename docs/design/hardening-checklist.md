# Hardening verification

This inventory covers the deployed tap and its dependencies. Tests establish the stated
behavior for their inputs; production observation and platform checks have separate limits.

## Input and memory bounds

- `Subscriber::poll` refuses payloads above `MAX_PAYLOAD_BYTES`; the live transport also
  sets `ZMQ_MAXMSGSIZE`. `tests/decode_fuzz.rs` covers truncated payloads, mutated bytes,
  length prefixes, and malformed frames.
- Malformed messages are counted and skipped. Unknown event kinds are counted separately.
  `tests/vllm_wire.rs` verifies the pinned decode and normalization corpus.
- Compute retains no per-block state. The adapter's content bridge has per-origin and
  total caps, with declared identity refusals (`src/source/wire/content.rs`).
- The receive HWM, queue shed, process RSS check, and deployment cgroup limits address
  different parts of the memory envelope. See `docs/trust.md`, section 3.

## Output and identity

- The adapter derives content identity from token IDs and drops those IDs before building
  normalized events. The record model has no dedicated token-ID or KV-tensor fields.
  Arbitrary operator text is outside this structural guarantee.
- Raw and pseudonymized records have distinct types. Compile-fail tests on
  `PseudonymizingSink` reject already-mapped input and nested pseudonymizers.
- `tests/pseudonym_vectors.rs` checks the HMAC constructions against the vendored corpus.
  `tests/guard_chain.rs` checks mapping and size enforcement through a custom destination.
- The no-raw scan aborts emission on an unexpected match. Its numeric floor and preexisting
  string exemptions are documented in `src/sink/noraw.rs` and tested in
  `tests/egress_false_positive.rs`.
- The serialized size limit includes the newline. The segment writer checks its own
  generated records; oversized header configuration fails before output creation.
- `Key` redacts its debug representation. Tests verify that key bytes are not printed.
- The demo and live egress paths produce identical bytes for the same scenario
  (`src/cli/demo.rs`, `shown_payload_byte_equals_the_live_chain_output`).

## Supply chain and static checks

- Rust forbids unsafe code in this crate. `tests/static_audit.rs` checks the lint policy,
  dependency allowlist, socket boundary, and absence of shell execution in deployed code.
  Its text checks are regression checks, not a proof about transitive dependencies.
- CI builds the declared MSRV, checks both feature sets, audits dependencies, enforces
  licence policy, and generates a CycloneDX SBOM. Actions and release tools are pinned.
- Release validation executes both packaged architectures. Tag builds also sign and
  verify artifacts; manual validation does not publish signatures.
- `scripts/preflight.sh` runs the local gate. CI-only checks are listed in `CONTRIBUTING.md`.

## Failure behavior

| Failure mode | Behavior | Evidence | Consumer consequence |
|---|---|---|---|
| Malformed multipart frame | Count and skip the message | `source/wire/mod.rs`, `tests/decode_fuzz.rs` | No event derived from the message; a later sequence can expose the gap |
| Invalid msgpack or schema | Count and skip the message | `tests/vllm_wire.rs`, `tests/failure_modes.rs` | Same gap semantics as other message loss |
| Oversized payload | Reject and count; subscription continues | `source/wire/mod.rs` | `oversized` reports refusal |
| Publisher sequence reset | Emit a holder reset; continue | `tests/failure_modes.rs`, `tests/lifecycle_records.rs` | Invalidate the affected holder state |
| Subscription overrun | Count interior gaps | `tests/failure_modes.rs` | Spanning intervals are incomplete; final losses may be undetectable |
| Abrupt process termination | Recover complete WAL records; discard a torn tail | `tests/failure_modes.rs`, `tests/segments.rs` | Preserve attributed records and declare recovery loss |
| Disk write failure | Latch the error and stop the run | `sink/jsonl.rs`, `cli/run.rs` | Stream ends at its last complete record |
| Unknown event kind | Count and skip the event | `tests/vllm_wire.rs` | `unknown_types` reports missing semantics |
| Trailing payload bytes | Refuse the message | `trailing_garbage` corpus fixture | No events from the malformed message |
| Unexpected raw value after mapping | Panic before emission | `sink/noraw.rs`, `sink/pseudonymizing.rs` | Offending record does not reach the destination |
| Oversized output record | Outer guard counts refusal; inner writer latches an error | `sink/guard.rs`, `tests/segments.rs` | Record omitted or run stopped; size violation is reported |
| Orchestrated stop | Emit stop reason and closing counters, then flush | `tests/lifecycle_records.rs` | A recorded stop distinguishes departure from unexplained silence |

## Platform and live-engine verification

`packaging/smoke/runbook.md` checks systemd permissions, sandbox settings, shipping,
lifecycle handling, and container volume access using a corpus-replay publisher.
It does not establish behavior under a production inference workload.

See [Compatibility](../compatibility.md) for support scope and
[Validation](../validation.md) for workload results. Use
[Release acceptance](../release-acceptance.md) for new packaged candidates.
