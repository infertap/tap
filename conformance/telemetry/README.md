# Telemetry conformance corpus

Each fixture checks the mapping from engine events to cache records: `store`, `evict`,
and `clear`. Expected records preserve declared identity, parent relationships, scope,
and event order. Optional values are omitted unless the contract permits explicit null.

Clear events produce scope-level records. Readers use their residency model to determine
the affected blocks. Classification and aggregate calculations are outside this mapping.

The `same_hash_distinct_dp_ranks`, `same_hash_distinct_groups`, and
`same_hash_distinct_instances` cases check that block identities remain scoped to their
origin. `records` and `record_counts` define each expected output stream.

Regenerate with `tools/gen-telemetry-corpus.py`. Expectations are derived from the
specification independently of the producer implementation.
