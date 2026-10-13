# Compatibility and support

## Requirements

The engine baselines are **vLLM 0.30.0** and **SGLang 0.5.20**, each with prefix caching
and the ZMQ KV-event publisher enabled. `--engine` selects the adapter profile; both
engines use the same transport and the same per-rank port expansion. The tap runs on
Linux x86-64 and ARM64. It runs on the CPU and does not require CUDA; GPU and driver
requirements belong to the engine.
See [Installation](install.md#get-the-binary) for binary runtime requirements.

## Support matrix

### Deployment

| Deployment | Supported configuration |
| --- | --- |
| systemd | Linux host service |
| Container | Standalone container |
| Kubernetes | Sidecar with a local event publisher |
| Local data parallelism | Two ranks, including two ranks in one Kubernetes pod |
| Distributed data parallelism | Two single-GPU pods; each tap reads its local publisher |

### Engine features

The following matrix covers vLLM. The SGLang matrix follows it.

| Feature | Support and constraints |
| --- | --- |
| Tensor parallelism | Widths 1, 2, and 4 |
| Dynamic LoRA | Supported; version adapter names when weights change |
| Hybrid KV groups | Supported; skipped-block spans can omit content identities |
| CPU and filesystem KV offload | Supported; identity completeness depends on engine metadata and retained history |
| GPU prefix-cache reset | Supported |
| Warm attachment and replay | Supported within the publisher's retained history |
| Object-store KV offload | Experimental; requires the engine fix below |

Backend ownership is retained when the engine supplies it. Otherwise, records describe
storage at tier scope. Request attribution is unavailable; records omit `session_id`.

Records retain publisher, data-parallel rank, and KV-group scope. Tensor parallelism does
not add per-GPU granularity to the engine's event stream. Distributed deployments must
isolate event ports and coordinate engine-group recovery; see the
[Kubernetes guide](../packaging/kubernetes/README.md).

Other tensor-parallel widths, engine versions, and real GPU engine integration on ARM64
have not been qualified.

### SGLang features

SGLang 0.5.20 was qualified on one GPU with the radix cache, page size 16, and the
hierarchical cache's host tier. See the [SGLang feature checks](validation/0.1.md#sglang-0520-feature-checks).

| Feature | Support and constraints |
| --- | --- |
| Tensor parallelism | Width 1 |
| Prefix reuse | Supported; the engine announces no reuse, and the tap declares `reuse_reporting: none` |
| Cache salt | Supported; the event-level salt enters the identity chain on the first block of a rooted run |
| Host-tier hierarchical cache | Supported with the write-through policy; `CPU_PINNED` stores derive the same content identity as the GPU store |
| Cache flush and publisher replay | Supported; a flush emits one clear scoped by rank, and replay recovers the retained ring |
| LoRA | `lora_id` is carried where the engine emits one; SGLang 0.5.20 emits no name, and the tap synthesizes none |

Tensor-parallel widths above 1, data parallelism and `attn_dp_rank` above 0, the
`DISK` and `EXTERNAL` storage tiers, EAGLE speculative decoding with bigram keys, and
`session_id` attribution have not been qualified. The engine records no store or
eviction for its storage tiers at 0.5.20; the tap passes those terms through if a later
version emits them. Bigram keys expose token pairs, so their stores carry no content
identity and are counted as unresolved.

### NVIDIA Dynamo

Under NVIDIA Dynamo, the tap subscribes to the engine's own ZMQ publisher inside the
worker pod, for vLLM and for SGLang alike. It never subscribes to Dynamo's event plane.
TensorRT-LLM in Dynamo's polling mode without the consolidator publishes no local
socket, and the tap does not support it.

## Limitations

### Incomplete content identities

The tap preserves events when it cannot derive `content_id`. Missing history, incomplete
metadata, conflicting announcements, or identity-state limits can leave this field absent.
Enable replay, size the identity state for your workload, and monitor `content_unresolved`
and capacity counters. See [identity-state sizing](install.md#deriving---resident-blocks).

Hybrid skipped-block spans can lack a contiguous token-to-hash mapping. Offload events
can omit extra keys used by LoRA or cache salts; the tap reuses an established GPU identity
when available. It does not derive an identity from incomplete inputs.

### Storage backend identity

The tap preserves engine `ownership` as a pseudonymized `backend_id`. Distinct identifiers
separate backends within a publisher, rank, KV group, and tier.

Missing backend identity is normal and produces no warning. Collection continues at tier
scope. Multiple caches in the same tier cannot be distinguished unless the engine supplies
distinct ownership metadata. The tap has no configuration option that adds this missing
engine metadata.

### LoRA revisions

Content identity does not fingerprint adapter weights. Assign a new versioned adapter
name whenever weights change. Reusing a name can produce equal content identities across
different weight revisions.

### Object-store KV offload

Stock vLLM 0.30.0 with NIXL 1.3.2 or 1.4.1 fails telemetry initialization with
`NIXL_ERR_NO_TELEMETRY`. Its object-store manager requires NIXL telemetry capture to be
enabled. A local engine patch setting `capture_telemetry=True` provides an experimental
workaround; see the [engine issue and workaround](validation/0.1.md#aws-s3-offload).

This limitation concerns the engine's KV offload. **Shipping tap segments to S3 is supported.**

## Identity compatibility

Compare `content_id` only across compatible models, tokenizers, block boundaries, and
extra-key semantics, using the same pseudonymization key and epoch. The tap derives this
identity independently of the engine's hash algorithm and random seed. Identity equality
does not establish that KV tensors are interchangeable.

`block_id` identifies a block within its holder and incarnation. It is not a cross-process
content identity and must not substitute for an absent `content_id`.
The contract uses 128-bit content identities; hash collisions remain possible.
See [Conformance](conformance-notes.md) for the pinned record contract.

## Supported interfaces

The supported interfaces are the executable's CLI and TOML configuration, its emitted
record contract, and its documented deployment behavior. Rust modules are implementation
details shared by the binary and tests. The crate is not a supported Rust SDK.

Before 1.0, a minor release may remove or incompatibly change a documented option,
configuration key, record interpretation, or deployment requirement. The changelog names
every such change and the operator or consumer action it requires. Patch releases fix
defects without intentionally changing those interfaces. Defaults that affect privacy,
identity, or retention are part of this compatibility boundary. From 1.0, an incompatible
change to any of these requires a major release.

Consumers must follow the contract pinned in `SPEC_PIN`, including its rules for unknown
fields and record kinds. A tap version and a contract revision are separate identifiers;
a contract pin change does not by itself establish backward compatibility. Release notes
must explain any consumer or operator action needed by a pin change.

Logs, diagnostic wording, internal Rust types, and byte-for-byte Parquet encoding are
not stable interfaces. Documented exit statuses and machine-readable records are.

## Upgrades and retained data

Keep the tap and its packaged units, scripts, and configuration templates at the same
release version. Review release notes and validate configuration before restarting.
Stop the tap cleanly and drain sealed segments before changing versions. Preserve the
pseudonymization key and epoch when continuity of identity is required.

A mixed-version crash can leave both a JSONL source and its Parquet twin. If the twin
ships before recovery and a different encoder regenerates it, downstream content hashes
may differ and permit duplicate ingestion. See [the recovery limitation](trust.md).
Do not infer rollback safety from a successful clean install. Recovery and rollback with
retained data require validation against the particular source and target releases.
