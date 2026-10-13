# Roadmap

This roadmap describes planned work without committing to release dates. Propose changes
through issues and the [contribution process](CONTRIBUTING.md).

## Current priorities

- Track vLLM event-schema changes and maintain the documented support baseline.
- Expand captured conformance data for ranks, KV groups, LoRA, and offload through the
  inference-events repository.
- Extend validation for additional tensor-parallel widths, ARM64 GPU deployments, and
  multiple named storage backends as suitable engine configurations become available.

## Future work

- Evaluate adapters for additional inference engines with accessible event publishers.
- Add a capture-replay transport for offline adapter development.

## Scope

The tap collects telemetry outside the request path. It does not proxy inference requests,
provide engine failover, or classify downstream metrics. Raw-ID mode remains for local
inspection. The project ships an executable, not a supported Rust SDK.

See [Compatibility](docs/compatibility.md) for current support and [CHANGELOG.md](CHANGELOG.md)
for implemented changes.
