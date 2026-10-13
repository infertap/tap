# Record contract and conformance

The [inference-events specification](https://github.com/infertap/inference-events)
defines the emitted record contract. [SPEC_PIN](../SPEC_PIN) identifies the exact
upstream commit and corpus hash used by this release. Read `spec/kv-cache-v2.md` at
that revision for field meanings, identity constructions, and consumer requirements.

The vendored inference-events package provides typed records, JSON Schema, and physical
column definitions. The [adapter reference](telemetry-event-model.md) describes the tap's
mapping into this contract; internal normalized-event types are not another public schema.

## Update the pin

Land contract and fixture changes in inference-events first. Update `SPEC_PIN` and run
`scripts/sync-spec.sh`, then run the contribution checks. Never edit the vendored corpus
locally to resolve an implementation failure.

`tests/contract_hash.rs` verifies corpus bytes against the pin offline. CI's `spec-pin`
job verifies the pinned upstream revision and vendored tree. Expected fixture results
are derived upstream independently of the tap implementation.

## Producer behavior

The tap emits `dp_rank` and `group_idx` on block records even where the contract permits
omission. This is a supported producer choice, not a contract divergence. Clear events
preserve absent scope dimensions as defined by the contract.

For externally introduced segment filenames that do not contain a parseable sequence,
retention can omit `first_seq` and `last_seq` from its loss declaration. This differs from
the contract's required sequence range. Files named by the tap do not take that path.
Keep the output directory private to the tap and its shipper.

## Producer test references

| Requirement | Test |
| --- | --- |
| Serialized record-size limit | `src/sink/guard.rs::a_record_past_the_size_bound_is_refused_and_counted` |
| Time-based segment sealing | `tests/segments.rs::a_quiet_instance_still_seals_on_the_time_ceiling` |
| Heartbeat deadline | `src/cli/run.rs::the_heartbeat_deadline_absorbs_the_wake_jitter` |

See [Hardening verification](design/hardening-checklist.md) for failure behavior and
[Validation](validation.md) for live-engine results. Changes to vendored README files
follow the same upstream ownership and pin-update process as the corpus.
