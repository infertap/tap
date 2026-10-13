# inference-events

Typed wire records and structural validation for inference telemetry, under Apache-2.0.
The package carries the canonical JSON Schema and physical column definitions.

```rust
use inference_events::Record;
use serde_json::json;

let value = json!({
    "kind": "store", "at_ms": 1.25, "instance_id": "engine-0",
    "block_id": "7", "n_tokens": 16, "tier": "GPU"
});
let record = Record::decode(value)?;
let wire_value = record.to_value()?;
# Ok::<(), inference_events::ValidationError>(())
```

Use `Record::decode` at complete JSON boundaries. It validates required fields, types,
integer bounds and record-local conditions. Use `validate_field` for projected Parquet
reads that intentionally omit columns. Unknown kinds and extension fields are preserved.
Generated structs describe wire layout; direct Serde decoding alone does not validate all
semantic constraints. Stream ordering, lifecycle scope and provenance obligations are in
the specification and conformance suite in the source repository.

Content construction `sha256-chain-128-v2` uses full 128-bit content identities. The wire
contract is 2.1 and pseudonymized identities remain 32 hexadecimal characters.

Use `validate_producer_record` before emitting a record under the current contract. It
rejects unknown kinds. Reader validation accepts unknown kinds with a valid `kind` and
`at_ms` envelope so newer producers remain readable.

`ValidationError` exposes a field `path` and an `ErrorKind` category. Errors do not echo
field values. Serialization rejects extension keys reserved by the enclosing structure,
including omitted optional fields and nested endpoint fields.
