# infertap

[![ci](https://github.com/infertap/tap/actions/workflows/ci.yml/badge.svg)](https://github.com/infertap/tap/actions/workflows/ci.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)
[![msrv](https://img.shields.io/badge/MSRV-1.85-informational.svg)](Cargo.toml)

KV-cache telemetry for inference engines.

`infertap` subscribes to vLLM or SGLang cache events and writes pseudonymized metadata to
sealed Parquet segments. Run it as a systemd service, a container, or a Kubernetes sidecar.
It operates outside the request path and does not access KV tensors.

The event history lets downstream tools reconstruct cache lifetimes, compare content
identities across compatible workers, and identify gaps in observation.

- Collect stores, evictions, cache resets, and publisher lifecycle events.
- Preserve data-parallel rank, KV group, storage tier, and available backend identity.
- Derive content identities and pseudonymize identifiers with an operator-managed key.
- Bound identity state, receive queues, and local segment retention. Report detected loss.
- Ship sealed segments to object storage with the packaged rclone integration.

The engine baselines are **vLLM 0.30.0** and **SGLang 0.5.20**, selected with `--engine`.
See [Compatibility and support](docs/compatibility.md) for parallelism, LoRA, offload,
and identity limitations. Before 1.0, a minor release may change a documented interface;
the changelog names every such change.

## Try it without a GPU

From a checkout with Rust installed, replay a captured prefix chain through the adapter,
record pipeline, and pseudonymizer:

```sh
cargo run --locked --quiet --example replay_corpus
```

The demo repeats one captured payload with simulated sequence numbers `10`, `12`, and
`1`. It detects the missing message and publisher reset, then checks that pseudonymization
preserves parent links and identity continuity under the same key. Its summary is:

```text
Messages received: 3
Missing messages between observed sequences: 1
Publisher resets: 1
Output: 135 stores, 1 holder reset
Checks passed: pseudonymized identities, parent links, identity continuity after reset
```

The command also prints one pseudonymized store record. It uses a public test key and
historical vLLM 0.26.0 bytes from the vendored corpus. Transport and output stay in memory;
the live setup below writes Parquet. The first build downloads the toolchain and crates
if needed. See [the example](examples/replay_corpus.rs) for the complete scenario.

## How it works

```text
vLLM or SGLang engine, ZMQ event publisher
  → transport: subscribe, drain replay history, track sequence gaps
  → adapter: decode MessagePack, derive content identity, discard token IDs
  → records: store, evict, clear, plus lifecycle and loss records
  → pseudonymizer: HMAC-SHA256 under the operator's key
  → sealed Parquet segments on local disk
  → shipper (rclone) → object store
```

Each record is one JSON object before conversion. A `store` record looks like this, wrapped
for width:

```json
{"kind":"store","instance_id":"i0","at_ms":1.0,"seq":1,"dp_rank":0,"group_idx":0,
 "block_id":"db83955d1a485fe98364fd9e47c8bc08","n_tokens":4,"tier":"GPU",
 "spec_kind":"full_attention","epoch":0}
```

`block_id` is a keyed pseudonym of the engine's block hash. Records carry token counts,
never token IDs. `infertap show-payload` prints a complete sample stream with a test key.

## Quickstart

This example runs the tap beside a single-rank vLLM engine on a Linux host and writes
local segments. Install the binary using the [installation guide](docs/install.md#get-the-binary),
or build from this checkout with Rust and a C++ toolchain:

```sh
cargo build --release --features zmq --locked
sudo install -m 0755 target/release/infertap /usr/local/bin/infertap
```

### 1. Configure the engine

Enable prefix caching and the ZMQ event publisher. The event ports carry tokenized
prompts without authentication. Apply [packaging/systemd/kv-events.nft](packaging/systemd/kv-events.nft)
before starting the engine; integrate the rule into your host's persistent firewall.
For Kubernetes, use the [sidecar guide](packaging/kubernetes/README.md).

```sh
sudo nft -f packaging/systemd/kv-events.nft
vllm serve <model> --kv-events-config '{"enable_kv_cache_events": true, "publisher": "zmq", "endpoint": "tcp://*:5557", "replay_endpoint": "tcp://*:5657"}'
```

Replace `<model>` with your model identifier. Leave the engine running in this terminal.

### 2. Start the tap

In another terminal, create a private working directory and a key. Run these commands
once; retain the key when restarting the tap.

```sh
mkdir -m 0700 infertap-local
cd infertap-local
(umask 077; set -C; openssl rand -hex 64 > pseudonym.key)
infertap --engine-endpoint local=tcp://127.0.0.1:5557 \
  --engine-replay-endpoint local=tcp://127.0.0.1:5657 \
  --key-file pseudonym.key --out-dir ./data --segment-secs 30
```

### 3. Verify collection

Send inference requests to the engine through your usual client. After 30 seconds,
check `infertap-local/data/kv/` for `seg-*.parquet` files and inspect the tap's logs.
Use Ctrl-C to stop the tap cleanly and seal the final segment.

For a persistent deployment and object-store shipping, follow the
[installation guide](docs/install.md). See [Troubleshooting](docs/onboarding.md#troubleshooting)
if events or segments do not appear.

## Documentation

See the [documentation index](docs/README.md) for operator and contributor guides.

- [Installation](docs/install.md): systemd, containers, configuration, and shipping.
- [Kubernetes](packaging/kubernetes/README.md): sidecar deployment and network isolation.
- [Engine configuration and verification](docs/onboarding.md): publisher setup and troubleshooting.
- [Compatibility and support](docs/compatibility.md): supported features and operational limitations.
- [Security and resource bounds](docs/trust.md): data handling, failure behavior, and controls.
- [Adapter and record pipeline](docs/telemetry-event-model.md): engine mapping and the record contract.

To inspect sample records without an engine:

```sh
infertap show-payload > sample.jsonl
```

The sample uses a test-only key. Live collection requires your own key unless explicitly
configured for local raw-ID inspection. Token IDs are discarded after identity derivation;
keep prompt content and secrets out of operator-supplied labels and provenance.

## Contributing

Maintained by Cort Fisher as an independent project. Support is best effort.

See [CONTRIBUTING.md](CONTRIBUTING.md) for builds, checks, and the DCO sign-off.
The [validation record](docs/validation.md) contains release test results.
The event contract is pinned by `SPEC_PIN` and checked against the conformance corpora.
Rust modules are internal implementation details; this project does not provide a Rust SDK.

See [CHANGELOG.md](CHANGELOG.md) for changes and [ROADMAP.md](ROADMAP.md) for planned work.

For a code review, start with the [design rationale](docs/design/design-log.md): typed
raw and pseudonymized records, bounded identity state, and coordination between crash
recovery, conversion, and retention. The [validation report](docs/validation/0.1.md)
records a one-hour workload with 419,880 collected block events and peak tap RSS of
9.32 MiB, alongside the tested hardware, independent comparisons, and support limits.
These are workload-specific observations.

## Licence

The Apache-2.0 licence (`LICENSE`, `NOTICE`). Contributions are accepted under the Developer
Certificate of Origin (`CONTRIBUTING.md`). The contract is the same licence: the
inference-events specification and its conformance corpora are Apache-2.0 in the
[spec repo](https://github.com/infertap/inference-events). Third-party components
redistributed with a release are listed in `THIRD-PARTY-NOTICES`.
