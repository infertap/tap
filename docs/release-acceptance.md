# Release acceptance

Use this procedure to validate a packaged release candidate. Track completion in the
release issue or report. Sanitize execution reports before publication: remove credentials,
prompt content, and private deployment identifiers.

## Deployment matrix

Validate the packaged release binary and container, not a developer build substituted
after a failure. Record the commit, artifact SHA-256, image digest, architecture, OS,
kernel, runtime versions, configuration, workload, and result for every run.

| Deployment | Publisher | Required evidence |
|---|---|---|
| Linux systemd | Synthetic publisher | Credentials, permissions, hardening, shipping, restart, crash recovery |
| Linux container | Synthetic publisher | Non-root operation, read-only root filesystem, key and volume access, resource limits |
| Kubernetes sidecar | Synthetic publisher | Restricted Pod Security, enforced NetworkPolicy, probes, container and pod lifecycle, shipping |
| Linux systemd or container | Single-GPU vLLM | Real events, replay, identities, sustained load, engine and tap restarts |
| Kubernetes sidecar | Real vLLM | Engine coexistence, network isolation, lifecycle and resource limits under load |
| Real engine deployment | Multi-GPU/data-parallel vLLM | Nonzero ranks, endpoint discovery, rank isolation, replay and restart accounting |

Exercise both advertised Linux architectures with the packaged binary. A synthetic
publisher can validate installation on a CPU host; it cannot validate engine behavior.
Record tensor-parallel and data-parallel settings separately. Multiple GPUs alone do
not demonstrate nonzero data-parallel ranks or KV groups.

## Installation and lifecycle

Start with the [packaging runbook](../packaging/smoke/runbook.md) on a disposable host.

- Install from the release artifact on a clean host using the published instructions.
  Confirm key validation fails closed and neither argv nor logs contain the key.
- Check every sealed Parquet segment is readable and starts with its own header.
  Confirm shipping removes successfully transferred segments and excludes active WALs,
  sealed WALs, temporary files, and the writer lock.
- Stop gracefully and verify the closing record. Kill abruptly and verify attributed
  recovery, a new incarnation, and preservation of previously sealed segments.
- Interrupt the shipper, fill a deliberately small ring, and verify bounded retention
  and loss accounting. Restore shipping and confirm forward progress.
- Test invalid configuration, malformed frames, unavailable endpoints, unwritable
  output, and a second writer. Record process status and recovery behavior.
- Upgrade and roll back with retained output. Include a retained WAL and a committed
  Parquet twin. Verify the documented procedure against the conversion caveat in
  [the trust document](trust.md), without claiming exactly-once delivery.

For Kubernetes, render and apply the shipped manifests with test secrets and pinned
images. Verify the CNI actually enforces NetworkPolicy. Test publisher and replay ports
from an unauthorized pod and from the engine pod; include every configured rank port.
Restart the tap container, engine container, and whole pod separately. Confirm the
documented identity, recovery, and `emptyDir` loss behavior for each case. Check the
shipper's final pass within the pod's termination grace period. Verify that a tap
failure does not become an engine readiness dependency.

## Engine semantics and load

Pin the engine image and model revision. Use generated, non-sensitive prompts. Keep
raw wire captures local; upstream any new conformance fixture through the specification
repository rather than editing the vendored corpus.

- Use controlled repeated and distinct prefixes to verify expected content-identity
  equality and separation in the emitted records. Include two engine instances using
  the same key and epoch. Verify a changed key or epoch separates identities.
- Attach to a warm engine, exercise replay, restart publishers, and verify sequence,
  loss, and reset accounting independently of the tap's own summary.
- Exercise nonzero data-parallel ranks. Validate nonzero KV groups, populated LoRA
  metadata, offload events, and clear events before claiming those engine shapes have
  live coverage. Distinguish unvalidated combinations from demonstrated failures.
  Scope each failure to the tested version and configuration.
- Inspect established Linux sockets during subscription and replay. Confirm the tap
  has no listening sockets and connects only to configured endpoints.
- Run steady, burst, and idle traffic. Compare publisher disabled, publisher enabled
  without the tap, and publisher enabled with the tap. Use the same workload, warmup,
  hardware, and repeated measurements; distinguish publisher cost from tap cost.
  Control prefix-cache state and request-to-rank routing. Balance measurement order
  and report variation between repetitions, not just a pooled mean.
- Record engine throughput and latency percentiles, tap CPU and RSS, cgroup memory
  usage, anonymous and file-cache memory, pressure and event counters, disk usage,
  conversion backlog, and loss counters throughout the run. Agree on the
  soak duration and performance thresholds before measuring. Check for growth trends
  as well as peak usage. An unexplained loss or breached threshold fails acceptance.

## Release decision

Each result is `pass`, `fail`, or `not run`, with commands and evidence attached. A
missing result is not a pass. Fix failures and repeat the affected checks on the final
candidate. Rerun the full code gate and release rehearsal after the final changes.

Before tagging a release:

- Publish the validated engine and deployment matrix, resource measurements, and
  remaining limitations. Reconcile these with the README and trust document.
- State the compatibility policy for CLI/configuration, records, on-disk recovery,
  and deployment behavior. Rust modules remain internal implementation, not an SDK.
- Update version metadata and the changelog consistently. Validate the final packages,
  image, notices, SBOM, checksums, and release verification instructions.
- Complete maintainer review and the public repository's contribution, security-reporting,
  and branch-protection setup. Verify signatures and provenance on the published
  artifacts before announcing the release.
