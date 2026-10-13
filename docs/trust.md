# Security and resource bounds

This document describes the tap's data handling, isolation, and failure behavior.
Report vulnerabilities through [SECURITY.md](../SECURITY.md). See
[Compatibility and support](compatibility.md) for engine requirements and limitations.

## 1. Process and network isolation

The tap runs outside the inference request path. It opens no listening ports, executes
no child processes, and accesses no KV tensors or Kubernetes API. The shipper is a
separate process with its own object-store credentials and network access.

The tap connects only to configured engine endpoints. Live collection uses ZMQ SUB
sockets. Optional replay uses a bounded DEALER exchange per rank: a one-second receive
timeout, a five-second deadline per endpoint, and a shared 64 MiB replay budget.
Unavailable replay reduces available history without preventing live collection.

The tap shares CPU, memory, disk, and deployment lifecycle with the engine. Packaged
systemd and Kubernetes deployments apply OS resource limits and privilege restrictions.
Follow the [installation guide](install.md) when configuring equivalent deployments.

## 2. Data handling and threat model

### Engine input

The engine's KV-event publisher carries token IDs without authentication or encryption.
Isolate publisher and replay ports before enabling them. Use loopback within the engine's
network namespace and the supplied host firewall or Kubernetes NetworkPolicy.

The adapter reads token IDs to derive content identities, then discards them before
constructing normalized events. Event and record types have no dedicated token-ID or
KV-tensor fields. Request attribution is not collected.

Labels, engine vocabulary, and provenance can carry arbitrary text. Do not place prompts
or secrets in these fields. Provenance validation rejects recognized content-bearing key
names recursively, but cannot identify sensitive content stored under another name.
Rejected-message diagnostics omit input payload values.

### Pseudonymization

With a key configured, the tap maps block, parent, and content identities to keyed
HMAC-SHA256 pseudonyms before writing records. Cache partitions, LoRA names, and backend
identifiers use separate domain contexts. Pseudonyms retain 128 bits and are encoded as
32 lowercase hexadecimal characters. The pinned [record contract](conformance-notes.md)
defines the constructions and key-epoch semantics.

Pseudonymization prevents a record recipient without the key from computing candidate
pseudonyms directly. It does not protect against a key holder who can test candidate
inputs, and it preserves equality relationships within a key space. Keep key custody
separate from record storage when that separation is required by your deployment.
Hash collisions remain possible; pseudonymization does not make identities collision-free.

Provision keys with `--key-file`. The file encodes exactly 64 bytes as hex or base64:
a 32-byte HMAC key and a 32-byte canary nonce. Identical halves and invalid lengths are
rejected. Key types redact debug output; key parsing errors omit key values. Avoid
inline `--key`, which can expose the key in process arguments.

For rotation, replace the key, increment `key_epoch`, and restart the tap. Records from
different key epochs belong to separate identity spaces. Preserve the key and epoch
across restarts when identity continuity is required.

`--raw-ids` explicitly bypasses pseudonymization for local inspection. It exposes raw
identifiers and mapped operator text in local output. Do not ship raw-mode output.

### Output checks

A recursive scan checks for unexpected raw values after mapping. A match aborts emission.
The scan excludes small numeric IDs below 2^32 and preexisting operator-text occurrences
outside mapped fields. Typed pseudonymization is the primary control; the scan does not
prove that arbitrary output text contains no sensitive information.

`show-payload` emits sample records through the production egress pipeline with a test
key. It does not check a running engine, its configuration, or its output.

## 3. Resource limits

| Resource | Limit or behavior |
| --- | --- |
| Input payload | Messages over 16 MiB are rejected and counted; the live socket also applies a message-size limit. |
| Receive queue | A message-count high-water mark and byte-counted shedding limit queued work. A message-count limit alone is not a byte limit. |
| Content-identity state | Per-origin and per-source entry caps; the configured capacity is clamped against one quarter of the RSS budget. |
| Backend scope names | At most 32 scopes per source, with bounded names; a full roster refuses new scopes without merging identities. |
| Output record | Serialized records have a 1 MiB limit, including the newline. |
| Recovery buffers | Header and tail buffers are each bounded to 1 MiB plus one byte. |
| Sealed segments | Oldest segments are discarded when retained bytes exceed `max_disk_bytes`; allow additional space for active and conversion files. |
| Process RSS | Default 768 MiB guard; exceeding the limit stops the process. |
| Packaged OS limits | Tap memory limit 1 GiB and CPU limit 1.5 cores; configure equivalent limits for standalone containers. |

The compute stage retains no residency model. The adapter retains a bounded content
bridge; capacity refusal can omit content identities without suppressing the corresponding
events. Receive shedding, record-size refusal, retention, and I/O failures can still make
output incomplete. Monitor their counters and in-band loss records.

The RSS guard differs from an OS memory limit. Linux cgroups also account for file cache;
a `memory.events` limit increment does not by itself indicate an OOM or an RSS leak.
Inspect anonymous memory, file cache, pressure, and OOM counters alongside process RSS.
Moving a file does not release its originating cgroup's cache charge.
See the [kernel memory-ownership rules](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-ownership).

For sizing, see [Configuration](install.md#flag-reference) and
[identity-state capacity](install.md#deriving---resident-blocks).

## 4. Failure behavior

Malformed messages are rejected and counted. Unknown event kinds are skipped and counted.
Publisher sequence gaps report losses between observed messages; loss after the last
observed message can remain undetected. A message can contain multiple events, so message
loss counters are not exact missing-record counts.

Lifecycle records identify startup, heartbeats, and orderly stops. A crash or output
failure can prevent the final stop record from being written. Consumers must account for
missing history, gaps, and unresolved identities under the pinned record contract.

The segment writer retains output errors and stops the run. After a process crash,
recovery preserves complete WAL records and discards a truncated tail. Recovery after
power loss is best effort. Only sealed Parquet segments are shipped.

Conversion commits a Parquet file before removing its source WAL. A crash, shipment of
the Parquet file, and an encoder change during an upgrade can cause recovery to emit a
second encoding of the same records. Content-hash deduplication may not recognize it.
Stop cleanly and drain conversions and sealed segments before changing versions.
See [Upgrades](compatibility.md#upgrades-and-retained-data).

The [verification map](design/hardening-checklist.md) identifies implementation tests
for these behaviors.

## 5. Supply chain

Rust forbids unsafe code in this crate. Dependency policy checks review the direct
crate set, permitted licences, and sources. The lockfile, toolchain, and CI action
revisions are pinned. CI checks RustSec advisories and produces a CycloneDX SBOM with
the shipped live-transport feature enabled.

The optional ZMQ transport compiles vendored libzmq and links it statically. Platform
runtime libraries remain dynamic dependencies. See [runtime requirements](install.md#get-the-binary)
and [THIRD-PARTY-NOTICES](../THIRD-PARTY-NOTICES) for dependency and license information.

The tagged release workflow signs artifacts with Sigstore and publishes checksums and
build provenance. Follow the [verification instructions](install.md#get-the-binary) before
installing a release. Manual release rehearsals do not publish or sign releases.

## 6. Validation

[Supported configurations](compatibility.md) define the current compatibility scope.
[Validation results](validation.md) record the measured workloads and environments.
Finite workload tests do not establish a universal event-rate ceiling or resource profile.
Contributor checks are described in [Contributing](../CONTRIBUTING.md) and
[Release acceptance](release-acceptance.md).
