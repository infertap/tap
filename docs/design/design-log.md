# Design rationale

Implementation constraints and their technical basis. Operational guarantees and
verification limits are documented in [the trust document](../trust.md).

## One static user for both units (`packaging/systemd/`)

The tap and shipper use the same static service user. Shipping deletes uploaded files,
which requires write permission on their directory; group read permission on the files
is insufficient. Both units declare their sandbox restrictions explicitly.

## The libzmq packaging claim (`docs/install.md`)

The optional ZMQ transport compiles vendored libzmq and links it statically. Release
binaries still depend on the documented platform libraries, including glibc. The release
workflow tests both architectures against the supported glibc floor.

## The replay dedup window (`src/source/wire/transport.rs`)

Replay and live subscription overlap. The transport drops live messages already covered
by replay. A regressed live sequence clears that window so a publisher restart reaches
the adapter instead of being discarded as an old message. A restart before the first
live observation can still be ambiguous; see the transport's documented residual risk.

## The fan-in schedule (`src/source/wire/transport.rs`)

A rotating cursor serves each ready socket within one pass. Always starting with the
first socket can starve later endpoints. The scheduling test fills both queues and
confirms arrivals before measuring fairness, separating scheduling from network delay.

## Output failures (`src/cli/run.rs`)

Write errors remain latched. The loop checks them after processing and flushes on the
heartbeat cadence so buffered output failures also surface on a quiet stream.

## Commands and flags (`src/cli/opts.rs`)

The first argument selects the command: `run`, `probe`, `check-config`, or `show-payload`,
with `run` as the default. Every flag applies to every command, so one option registry
drives parsing, configuration layering, and help. Commands are mutually exclusive by
position rather than by a check.

## Dependencies (`Cargo.toml`, `tests/static_audit.rs`)

The dependency set is an allowlist pinned by a test. A crate enters when it carries
something the tap cannot write correctly in a page: Parquet encoding, libzmq, HMAC-SHA256,
MessagePack, TOML parsing, and serialization. Ergonomic crates stay out. The argument
parser is hand-written because error text must never echo a value, key material must be
refused from files and the environment, and one registry must define every option once.
The run loop is a blocking poll with one worker thread, so no async runtime is needed.
Errors are typed enums the run loop matches on, so no dynamic error crate is needed.

## Two on-disk forms (`src/sink/segments/`, `src/sink/convert.rs`)

Records land in a JSONL write-ahead file and convert to Parquet after sealing. The line
format makes crash recovery a scan for the last complete line, and a header fsync makes
every orphan self-describing or empty. Parquet is the shipped form because consumers
read it in bulk. Conversion writes a temporary, verifies its footer row count, and
commits by rename under the retention lock, so a partial twin never carries the shipped
name.

## The no-raw guard (`src/sink/noraw.rs`)

The raw and pseudonymized record types already make a raw identity in mapped output a
compile error. The scan is a second, independent check on the serialized bytes, so a
defect in the mapping itself is caught before the record leaves the process. A match is a
panic rather than a dropped record: the tap's own availability ranks below not shipping a
content fingerprint, and the engine is unaffected either way.

The scan traverses nested serialized values. IDs below 2^32 are excluded because they
can coincide with counters; this leaves a documented 2^-32 exclusion for uniform hashes.
For operator text, preexisting occurrences outside mapped fields are exempt. An
occurrence in a mapped field is never exempt. These are limits of the backstop scan,
not exceptions to identity mapping.

## SIGTERM inside the poll (`src/cli/run.rs`)

A termination signal can interrupt a blocking poll with EINTR. The signal flag takes
precedence over the poll result when choosing the stop reason.

## The receive high-water mark (`src/source/wire/transport.rs`)

The HWM divides a 256 MiB estimated receive budget across endpoints, using a 6.5 KiB
representative payload size. It absorbs short stalls; a message-count bound is not a
byte bound. Larger messages require the separate byte-counted shed below the RSS limit.
Workload measurements are recorded in [Validation](../validation.md).

## RSS sampling (`src/cli/run/memory.rs`)

The run loop reads `/proc/self/status` on every wake and every 4,096 events within a
drain. A read costs a few microseconds against a poll interval of hundreds of
milliseconds, and sampling on the loop's own cadence needs no timer thread. A shed
decision needs the current figure, not a recent one, because one large message can move
the resident set by tens of megabytes.

## The lock file's name (`src/sink/segments/mod.rs`)

The writer lock is `.writer.lock.jsonl`. The suffix places it under the shipper's
existing write-ahead exclusion, so no shipper configuration needs a rule for it, and the
delivery layout contract pins the exclusion list both sides assert against.

## Manifests and units under test (`tests/`)

Kubernetes manifests, systemd units, and the shipper script are asserted by Rust tests
that parse their text and, where possible, execute them with stubs. The tests run in the
same `cargo test` invocation as everything else, so a manifest edit cannot pass CI while
disagreeing with the constants or the contract it depends on.

## The content bridge cap (`src/source/wire/content.rs`)

The bridge limits entries per origin and to four origin caps per source. Budgeting
reserves at most one quarter of the configured RSS limit across sources. Refused
identities are declared without withholding cache-event records.

## One wire, one profile per engine (`src/source/wire/`, `src/source/vllm.rs`, `src/source/sglang.rs`)

vLLM and SGLang publish the same wire format, so the transport, framing, decoding,
run expansion, and content bridge live once under `source/wire/`. What differs per
engine is a `Profile` trait with five methods: the known-field table, how a hash is
read, where the cache salt enters the identity chain, whether offload tiers derive, and
the `reuse_reporting` declaration. Each engine module is a unit struct implementing it,
next to the tests for its own differences, and the roster maps `--engine` to one. The
salt folds into the first block of a rooted run on both profiles, so one content identity
covers both engines. `reuse_reporting` is declared per engine from the engine's own cache
code, because a reader derives insertion counts from it and a wrong declaration inverts
the measurement without looking wrong.

## The parent chain (`src/event_model.rs`)

A stored batch names the first block's parent. Every later block's parent is the preceding
block in the ordered list. Missing block positions prevent an inferred adjacent edge.

## Compute state (`src/compute.rs`, `src/lifecycle.rs`)

Compute transforms events without retaining a residency model. Consumers classify the
record stream using their own state. The adapter's bounded content bridge is separate
from compute and reports its population and refusals through lifecycle counters.

## The disk cap at startup (`src/sink/segments/mod.rs`)

Enforce retention after opening the first active segment and after each rotation. This
bounds repeated short-lived runs and provides an active segment for loss declarations.
Closing does not open a successor and therefore does not reclaim additional segments.

## The retention order (`src/sink/segments/ring.rs`)

Sort numerically by incarnation and sequence. Lexicographic filenames place sequence 10
before sequence 9. A WAL and its Parquet twin share one identity and are reclaimed
together under the conversion commit lock.

## Segment ownership and recovery

An OS file lock excludes competing writers before recovery starts. The descriptor stays
open until the converter has stopped. The persistent lock file is not unlinked, because
replacing its inode would permit independent locks on the same directory.

The ownership guard explicitly unlocks before closing its descriptor. Closing alone can
leave the lock held by a duplicate inherited during a concurrent fork, even with
close-on-exec enabled. The parallel Kubernetes tests exposed this during immediate reopen.
Regression tests retain a duplicate across successful close, failed close, and drop;
the next owner must acquire the lock without waiting for that duplicate to close.

Sealing creates a hard link without replacement, syncs the directory, then removes the
active name. Existing destinations are errors. Recovery validates the incarnation as a
single filename component and reads only a bounded header and tail. A crash between
link creation and unlink leaves both names for inspection rather than risking overwrite.

Ring cancellation entries remain only while a WAL survives partial reclamation. Once
it is gone, the worker's existence check under the commit lock prevents resurrection.

## Record representations

A sealed phase trait permits raw and pseudonymized records only. Each guarded sink is
bound to one phase, so its input type and lifecycle calls are unambiguous. Serialization
writers share the record layout; the pseudonymizer accepts only the raw phase.
