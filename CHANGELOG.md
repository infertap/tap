# Changelog

Notable changes to InferTap. Release tags use `vX.Y.Z` and follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). The supported interfaces and
upgrade requirements are defined in [the compatibility policy](docs/compatibility.md).

## [Unreleased]

The first public release, 0.1.0, is in preparation.

### Added

- A captured-wire demo that checks message-gap detection, publisher resets, and
  pseudonymized identity continuity without a GPU or running engine.
- Passive vLLM KV-event ingestion over ZMQ, with replay and sequence-gap accounting.
- SGLang as a second engine, `--engine sglang`: the same transport and adapter under an
  SGLang profile that folds the event-level cache salt into the identity chain, carries
  `lora_id` without a name, passes SGLang's tier vocabulary through, reads the engine's
  signed 64-bit hashes as unsigned, and declares `reuse_reporting: none`.
- Content-derived identities, keyed pseudonymization, and an explicit raw inspection mode.
- Optional engine-emitted backend identity, with separate residency accounting and pseudonyms.
- Bounded reference counts for repeated cache announcements and conservative offload identities.
- Bounded identity state, receive queues, serialized records, and disk retention, with
  loss and lifecycle records in the output stream.
- Crash recovery of JSONL write-ahead segments and conversion to sealed Parquet segments.
- The `check-config`, `show-payload`, and `probe` commands: configuration validation,
  sample output, and a portless liveness check.
- Configuration errors that omit rejected values, endpoint syntax checks, and help/version
  commands that work independently of runtime configuration.
- Systemd units and Kubernetes sidecar examples with separate object-storage shipping.
- Linux x86-64 and ARM64 release packaging, container images, checksums, dependency
  notices, SBOM generation, and keyless signing in the tagged release workflow.
- Conformance, property, failure-injection, live-transport, and deployment checks.

### Compatibility

- The CLI, configuration, emitted record contract, and documented deployment behavior
  are the supported interfaces. Internal Rust modules are not a supported SDK.
- The live-transport Cargo feature is `zmq`. It was `vllm-zmq` before the SGLang engine
  landed; a build script or Dockerfile that names the old feature must change.
- Content identities retain 128 bits; pseudonyms remain 32 hexadecimal characters.
- The record contract is 2.1, pinned by commit and corpus hash in `SPEC_PIN`.
- The engine support baseline and limitations are listed in
  [the compatibility policy](docs/compatibility.md).

[Unreleased]: https://github.com/infertap/tap/commits/main
