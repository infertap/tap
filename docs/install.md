# Installation

Run the tap as a systemd service, a standalone container, or a Kubernetes sidecar.
Use vLLM 0.30.0 with prefix caching and the ZMQ event publisher enabled. Follow
[Engine configuration](onboarding.md#configure-the-publisher) to configure and isolate
the publisher before starting the engine.

## Get the binary

To build from a checkout:

```sh
cargo build --release --features zmq --locked
sudo install -m 0755 target/release/infertap /usr/local/bin/infertap
```

Source builds require the pinned Rust toolchain and a C++ compiler. Release binaries
require Linux, glibc 2.31 or newer, libstdc++ with GLIBCXX_3.4.28, and libgcc_s.
No separate libzmq installation is required.

For a published release, choose a tag from [Releases](https://github.com/infertap/tap/releases).
Set the architecture to `x86_64` or `aarch64`. Download into an empty directory:

```sh
INFERTAP_VERSION='<tag>'
INFERTAP_ARCH='x86_64'
INFERTAP_ARCHIVE="infertap-${INFERTAP_VERSION}-${INFERTAP_ARCH}-linux"
INFERTAP_RELEASE="https://github.com/infertap/tap/releases/download/${INFERTAP_VERSION}"
curl -fLO "${INFERTAP_RELEASE}/${INFERTAP_ARCHIVE}.tar.gz"
curl -fLO "${INFERTAP_RELEASE}/SHA256SUMS"
curl -fLO "${INFERTAP_RELEASE}/SHA256SUMS.bundle"
```

With cosign installed, verify the signed checksum manifest and then the downloaded archive.
Continue only if both commands succeed:

```sh
cosign verify-blob --bundle SHA256SUMS.bundle \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity "https://github.com/infertap/tap/.github/workflows/release.yml@refs/tags/${INFERTAP_VERSION}" \
  SHA256SUMS
sha256sum -c --ignore-missing SHA256SUMS
```

Extract the archive and install the binary:

```sh
tar -xzf "${INFERTAP_ARCHIVE}.tar.gz"
cd "$INFERTAP_ARCHIVE"
sudo install -m 0755 infertap /usr/local/bin/infertap
```

The archive includes systemd units, a configuration template, the port-isolation rule,
and a shipper environment template. Releases also include an SBOM and GitHub build
attestations. To verify an archive's build provenance:

```sh
gh attestation verify "${INFERTAP_ARCHIVE}.tar.gz" --repo infertap/tap
```

Run that command from the download directory.

## systemd

The packaged unit requires systemd 247 or newer. On systemd 254 and newer, failed
restarts back off; versions 247–253 use a fixed two-second interval.

From the extracted release directory:

```sh
INFERTAP_PACKAGING=.
INFERTAP_CONFIG_TEMPLATE=tap.toml.example
```

Or, from a source checkout:

```sh
INFERTAP_PACKAGING=packaging/systemd
INFERTAP_CONFIG_TEMPLATE=packaging/tap.toml.example
```

Create the service account and install the files:

```sh
sudo useradd --system --home-dir /var/lib/infertap --shell /usr/sbin/nologin infertap
sudo install -d -m 0755 /etc/infertap /etc/nftables.d
sudo install -m 0644 "$INFERTAP_PACKAGING/infertap.service" /etc/systemd/system/
sudo install -m 0644 "$INFERTAP_CONFIG_TEMPLATE" /etc/infertap/tap.toml
sudo install -m 0644 "$INFERTAP_PACKAGING/kv-events.nft" /etc/nftables.d/
sudo sh -c 'umask 077; set -C; openssl rand -hex 64 > /etc/infertap/pseudonym.key'
```

Skip account creation if the account already exists. Generate the key only once; the
command refuses to overwrite an existing file. Provision your existing fleet key instead
when identities must match other taps.

Edit `/etc/infertap/tap.toml` for the engine endpoints and resource limits. Apply the port
rule before starting the engine, and include it in your persistent firewall configuration:

```sh
sudo nft -f /etc/nftables.d/kv-events.nft
sudo systemctl daemon-reload
sudo systemctl enable --now infertap
sudo journalctl -u infertap -f
```

The service runs as `infertap`, creates `/var/lib/infertap`, and loads the root-owned key
through `LoadCredential`. It checks configuration before startup. Default OS limits are
1 GiB memory and 1.5 CPU cores; the in-process RSS limit is 768 MiB. OS memory accounting
also includes file cache.

### Enable object-store shipping

Install rclone at `/usr/bin/rclone`. Install the timer and configure its destination:

```sh
sudo install -m 0644 "$INFERTAP_PACKAGING/infertap-ship.service" /etc/systemd/system/
sudo install -m 0644 "$INFERTAP_PACKAGING/infertap-ship.timer" /etc/systemd/system/
sudo install -m 0600 "$INFERTAP_PACKAGING/ship.env.example" /etc/infertap/ship.env
sudoedit /etc/infertap/ship.env
sudo systemctl daemon-reload
sudo systemctl enable --now infertap-ship.timer
```

Set `INFERTAP_DEST` to an rclone destination ending in `/kv/`, including the trailing slash.
For S3, grant PutObject and GetObject on the node's destination prefix and ListBucket
restricted to that prefix. DeleteObject is not required. Prefer instance or workload
credentials. Check uploads with `journalctl -u infertap-ship`.

## Container

For published releases, use `ghcr.io/infertap/tap:<tag>`. The image supports
linux/amd64 and linux/arm64. Project and dependency notices are installed under
`/usr/share/doc/infertap/`. Verify the image before running:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity "https://github.com/infertap/tap/.github/workflows/release.yml@refs/tags/${INFERTAP_VERSION}" \
  "ghcr.io/infertap/tap:${INFERTAP_VERSION}"
```

To build from a checkout:

```sh
docker build -f packaging/docker/Dockerfile -t infertap:local .
```

The following example uses the local image and Linux host networking to reach an engine
on host loopback. Isolate the publisher ports as described above. Provision a key readable
only by the container's UID 65532:

```sh
sudo install -d -m 0755 /etc/infertap
sudo sh -c 'umask 077; set -C; openssl rand -hex 64 > /etc/infertap/container.key'
sudo chown 65532:65532 /etc/infertap/container.key
sudo chmod 0400 /etc/infertap/container.key
docker run --name infertap --rm --network host \
  --read-only --cap-drop=ALL --security-opt no-new-privileges \
  --memory 1g --cpus 1.5 \
  -v infertap-out:/out \
  -v /etc/infertap/container.key:/run/secrets/pseudonym.key:ro \
  infertap:local \
  --engine-endpoint local=tcp://127.0.0.1:5557 \
  --engine-replay-endpoint local=tcp://127.0.0.1:5657 \
  --key-file /run/secrets/pseudonym.key --out-dir /out
```

Retain the key on subsequent runs. If the engine runs in a separate container network
namespace, use `--network container:<engine-container>` instead of host networking.
A fresh `infertap-out` volume inherits the image's writable ownership. Existing volumes
must be writable by UID 65532. Mount this volume into a separate shipper to upload sealed
segments; the tap image does not include a shipper.

## Kubernetes

Follow the [Kubernetes guide](../packaging/kubernetes/README.md) to add the tap and shipper
sidecars to an existing engine Deployment or configure the supplied example.

## Data parallelism

Each vLLM data-parallel rank publishes on the configured base port plus its **global rank**.
The replay port uses the same offset. SGLang's `ZmqEventPublisher` offsets both ports by
the rank in the same way. Configure the tap with the base endpoints and the ranks local
to its host or pod:

```toml
engine_endpoint = ["engine=tcp://127.0.0.1:5557"]
engine_replay_endpoint = ["engine=tcp://127.0.0.1:5657"]
dp_start_rank = 0
dp_local = 2
dp_max = 2
```

This subscribes to publisher ports 5557–5558 and replay ports 5657–5658. Match
`dp_start_rank` to the engine's `--data-parallel-start-rank` and `dp_local` to
`--data-parallel-size-local`. Set `dp_max` to the maximum local rank count the tap should
cover; it must be at least `dp_local` and defaults to that value. Isolate the entire
configured port range.

For two pods with one rank each, use `dp_local = 1` and `dp_max = 1` in both, and
`dp_start_rank = 0` or `1` respectively. Each sidecar connects to its own pod's loopback.
The packaged sidecar identifies the pod as the instance; `dp_rank` identifies the rank.

Use derived endpoints for multiple local ranks. Manual `--endpoints` accepts one endpoint
per label and must not combine separate rank streams under the same label.

## Delivery

The tap writes to `<out_dir>/kv/`. Sealed `seg-*.parquet` files are ready to ship. The
packaged shippers exclude active JSONL, conversion sources, temporary Parquet files, and
the writer lock. Keep these exclusions when configuring another shipper.

The shipper uploads whole sealed files and deletes them locally after success. The tap
discards the oldest sealed segments when retention exceeds `max_disk_bytes` and reports
the loss. Allow additional disk space for the active segment and conversion work.

Use one writer per output directory on a filesystem supporting hard links. Do not delete
`.writer.lock.jsonl` while a writer is running. The process releases the lock on exit,
including after a crash; the empty file remains.

After a process crash, the tap recovers complete active records and discards a truncated
tail. Recovery after power loss is best effort. Stop cleanly and drain sealed segments
before upgrading; see [Upgrades and retained data](compatibility.md#upgrades-and-retained-data).

## First-run verification

The service validates configuration automatically. For a foreground deployment, pass the
same configuration and key to a configuration check before starting collection:

```sh
infertap check-config --config /path/to/tap.toml --key-file /path/to/pseudonym.key
```

Then follow [Verify collection](onboarding.md#verify-collection) to check incoming events,
local segments, and uploads. `infertap show-payload` produces sample records with a test
key; it does not check your engine or deployment.

## Configuration file and environment

Configure the tap with a TOML file, environment variables, or command-line flags.
A flag takes its value as the next argument or as `--name=value`.
Precedence is **flags > environment > file**. Lists are replaced, not appended.
TOML keys use underscores; environment variables use `INFERTAP_` followed by the
uppercase key name. `INFERTAP_CONFIG` selects a file when `--config` is absent.

```toml
engine_endpoint = ["i0=tcp://127.0.0.1:5557"]
engine_replay_endpoint = ["i0=tcp://127.0.0.1:5657"]
key_file = "/etc/infertap/pseudonym.key"
out_dir = "/var/lib/infertap"
heartbeat_secs = 60
```

For example, `INFERTAP_HEARTBEAT_SECS=30` overrides the file's heartbeat interval;
`--heartbeat-secs 15` overrides both. The systemd service accepts environment overrides
from `/etc/default/infertap` and supplies its credential path on the command line.

Use `key_file` for key provisioning. Inline `--key` is available only on the command
line, not in TOML or the environment. Unknown keys and invalid values cause exit status 2.
`check-config` reads the key and validates configuration without opening engine sockets.
It does not check connectivity. `--help` and `--version` do not load runtime configuration.

## Logging

The tap writes diagnostics to stderr. Use the system journal or container runtime for
log collection and rotation. The default level is `info`; `--log-level` accepts `error`,
`warn`, `info`, `debug`, and `trace`.

Set `--log-format json` for one JSON object per line with `ts`, `level`, `target`, and
`msg` fields. The default text format integrates with systemd journal priorities:

```sh
journalctl -u infertap -p warning
```

Warnings include detected loss, identity-cap refusals, adjusted resource limits, and
raw-ID mode. Errors include fatal stream faults and failures to enforce retention.

## Commands

The first argument selects the command. Without one, the tap runs live collection.

| command | behavior |
|---|---|
| `run` | live collection; the default |
| `check-config` | validate flags, file, and environment (key file read, provenance parsed), connect nothing, print a summary, exit 0; exit 2 with the reason otherwise |
| `probe` | liveness: exit 0 if `<out-dir>/kv/` was written within twice the heartbeat interval (or twice the segment age with heartbeats off), else 1; one line on stdout |
| `show-payload` | print sample records with a test-only key; no engine needed |

## Flag reference

| flag | required | default | notes |
|---|---|---|---|
| `--endpoints <MAP>` | live subscribe | none | `instance=endpoint` pairs, comma-separated |
| `--engine-endpoint <E>` | live subscribe (or `--endpoints`) | none | engine base endpoints, `label=tcp://host:port` (a bare address takes the label `engine`); the tap derives each rank's port itself; see "Data parallelism" |
| `--dp-start-rank <N>` | no | `0` | the engine's `--data-parallel-start-rank`; the first derived port is `base+N` |
| `--dp-local <N>` | no | `1` | the engine's `--data-parallel-size-local`; refused above `--dp-max` |
| `--dp-max <N>` | no | `--dp-local` | rank **bound**, the pod's own GPU limit in Kubernetes; ports past the live set deliver nothing until an elastic resize binds them |
| `--engine-replay-endpoint <E>` | no | none | the engine's `kv_events_config.replay_endpoint`, dialable form, same syntax and expansion as `--engine-endpoint`; drains the replay ring on attach, best effort |
| `--engine <NAME>` | no | `vllm` | engine adapter profile: `vllm` or `sglang`; the transport and the port expansion are the same for both |
| `--out-dir <DIR>` | live subscribe | none | output root; sealed segments are written under `kv/`. See [Delivery](#delivery) |
| `--out <PATH>` | one-shot | none | a single file for a bounded run. Refuses to overwrite rather than truncating |
| `--segment-bytes <N>` | no | 67108864 | segment size threshold in bytes |
| `--segment-secs <N>` | no | 300 | segment age threshold in seconds |
| `--max-disk-bytes <N>` | no | 17179869184 | sealed-segment retention limit; allow additional space for active and conversion files. The active file may exceed its size threshold while recording retention losses |
| `--workload-class <N>` | no | none | what this producer's traffic is for, in your own vocabulary (`agentic`, `batch`); declared once per segment header, and a reader applies it to every record in the segment. Omit it to say nothing |
| `--key-file <PATH>` | live subscribe (or `--raw-ids`) | none | the deployment form: key off argv. **Exactly 64 bytes**, hex or base64: a 32-byte HMAC key plus a 32-byte canary nonce. `openssl rand -hex 64`. Fail-closed on any other length, and on two identical halves |
| `--key <K>` | no | none | inline form for local runs; mutually exclusive with `--key-file` |
| `--raw-ids` | no | none | the explicit keyless opt-out: raw block ids (content fingerprints), cache salts and LoRA names in the clear, at rest. A bounded local inspection only; never for records that leave the host. Mutually exclusive with a key |
| `--key-epoch <N>` | no | 0 | key epoch, folded into every pseudonym, stamped on records and declared on every segment header; requires a key |
| `--provenance <P\|JSON>` | no | none | JSON object stamped into every segment header: inline where it starts with `{`, else a file path; must be an object (rejected otherwise) |
| `--duration <SECS>` | no | none | measurement window; report printed at exit. Finite and non-negative, rejected otherwise |
| `--max-batches <N>` | no | none | stop after N messages |
| `--heartbeat-secs <N>` | no | 60 | lifecycle heartbeat interval (0 disables); heartbeats carry envelope stats in-band, and a departure without an `agent_stop` record is the anomaly signal |
| `--poll-timeout-ms <N>` | no | 250 | socket poll interval; minimum 1 ms |
| `--resident-blocks <N>` | no | 1000000 | declared per-instance resident-block capacity across subscribed tiers; bounds the content-identity map, which retains outstanding store announcements. Derivation below |
| `--version`, `-V` | no | none | print the version and the spec pin, exit |
| `--log-level <L>` | no | info | error, warn, info, debug, trace |
| `--log-format <F>` | no | text | text, or json for one object per line |
| `--config <PATH>` | no | `$INFERTAP_CONFIG` | a TOML file of these flags without the dashes; flag beats `INFERTAP_<FLAG>` beats file (see above) |
| `--max-rss-bytes <N>` | no | 805306368 | RSS limit (768 MiB). Exceeding it stops the process with reason `memory_bound` and exit 1. 0 disables; nonzero minimum 268435456. Set OS memory limits separately |

A live subscribe **requires a key**. Keyless exists only behind the explicit `--raw-ids`
opt-out, and the run says on stderr exactly what that leaves at rest: raw block ids are
content fingerprints a key holder can test candidate prompts against, and `extra_keys`
carries the tenant's `cache_salt` verbatim. Never for records that leave the host.

## Deriving `--resident-blocks`

The content-identity map retains outstanding store announcements. Start with the
instance's declared cache capacity across subscribed tiers:

```
blocks(tier) = tier_kv_bytes / (block_size_tokens × kv_bytes_per_token)
resident_blocks = Σ blocks(tier) over subscribed tiers
```

`kv_bytes_per_token` is `2 × n_layers × n_kv_heads × head_dim × bytes_per_element` (the 2 is
K and V). Worked example, Llama-3-8B on one H100, fp16 KV, 16-token blocks: `2 × 32 × 8 ×
128 × 2 = 128 KiB` per token, 2 MiB per block, so even all 80 GB of HBM as KV cache is
**~40,000 blocks**. The default declared cap is 1,000,000 entries per origin.
The tap clamps it using a conservative 512-byte entry budget, four origins per source,
and a quarter of the configured RSS limit. At the default 768 MiB RSS limit, one source
receives 98,304 entries per origin. Additional sources divide that budget.

Repeated reuse announcements or missing removals can retain entries beyond physical
residency. The cap remains enforced, and refusals are declared through `identity_refused`.
Scope names add at most 256 KiB per source. Increase memory limits together with a larger
retained-state requirement; a larger declared cap alone cannot override the RSS clamp.

When the cap is reached, affected records omit `content_id`. Heartbeats report
`content_bridge_evicted`, and `identity_refused` records identify the holder and affected
engine-time window.

## Reading the exit line

```
events=… | msgs=… dropped=… oversized=… unknown_types=… noraw_scanned=… records=… -> <out>
```

The exit line reports transport and output counters. The adapter retains bounded identity
state; consumer analysis uses the record stream in `<out>`. `dropped` counts observed sequence
gaps, and `oversized` counts messages rejected by the payload cap. These counts cannot be
added to obtain an exact number of missing records: a message can contain multiple events,
and rejected messages can also cause sequence gaps. Loss at the end of a stream may remain
undetected without a later sequence number. `unknown_types` counts events this build does
not model.

`noraw_scanned` counts values visited by the egress scanner. A successful scan means no
unexpected raw-value matches were detected within its scope, excluding small numeric IDs and strings already present outside mapped fields. Typed pseudonymization is the primary control. See the failure semantics
in [`design/hardening-checklist.md`](design/hardening-checklist.md).
