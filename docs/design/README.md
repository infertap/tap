# Architecture notes

- [Design rationale](design-log.md) explains implementation constraints and tradeoffs.
- [Hardening verification](hardening-checklist.md) maps failure behavior to tests and
  identifies the limits of local verification.
- [Glossary](glossary.md) defines the vocabulary shared by the code and documentation.

See the [documentation index](../README.md) for operator guides.
The [pipeline reference](../telemetry-event-model.md) describes engine mapping;
[Conformance](../conformance-notes.md) identifies the public record contract.

## Source layout

- `cli/opts/spec.rs` owns option identifiers, spellings, arity, configuration layers,
  and descriptions. Parsing dispatches on those identifiers; help uses the same registry
  and shared defaults.
- `cli/run.rs` coordinates collection and shutdown. Its `memory` module owns RSS and
  shedding policy; `reporting` builds lifecycle records and diagnostics.
- `source/wire/` owns the shared wire: transport, framing, decoding, normalization, and
  the content bridge. `source/wire/adapter/names.rs` holds the event and field
  vocabulary; `source/vllm.rs` and `source/sglang.rs` each implement the adapter's
  `Profile` from it. Unknown vocabulary remains observable.
- `sink/convert.rs` owns conversion file handling and commit ordering. Its `schema`
  module derives column plans from the pinned contract, and `encoding` buffers records
  and writes row groups. The caller still synchronizes commit with retention.

All paths above are relative to `src/`. Rust modules are internal implementation details.
