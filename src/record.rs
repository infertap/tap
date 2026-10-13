// SPDX-License-Identifier: Apache-2.0
//! Typed records and their serialization.
//!
//! [`Record`] carries raw identities. [`PseudonymizedRecord`] carries keyed identities
//! with the same field layout. The pseudonymizer converts between these types.
//!
//! Adapters discard token IDs before constructing records. No field stores token IDs
//! or KV tensors. Operator text remains arbitrary; provenance validation rejects
//! known content-bearing keys, not every possible encoding. See `docs/trust.md`.
//!
//! Absent optional fields are omitted, except an unknown tier is serialized as null.
//! Identities serialize as strings: raw engine and content identities use decimal
//! text; pseudonyms use hexadecimal text.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::clock::{EngineMs, ProducerMs};
use crate::event_model::Term;

shared_text! {
    /// The instance label a record carries: the label every rank of one engine rolls up to,
    /// and the key a reader joins the heartbeat's per-endpoint entries on. Shared by
    /// reference count, since every record repeats it.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    InstanceLabel
}

impl From<&crate::source::SourceKey> for InstanceLabel {
    /// A source that no expansion labelled is its own label; the text is shared, not copied.
    fn from(key: &crate::source::SourceKey) -> Self {
        Self(key.shared())
    }
}

/// A raw engine hash. Serializes as decimal text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Id(pub u64);

impl Serialize for Id {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}

/// A raw 128-bit content identity. Serializes as decimal text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentId(pub u128);
impl Serialize for ContentId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}

/// Raw operator text, such as a cache salt or `LoRA` adapter name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct OperatorText(pub String);

/// A keyed identity produced by the pseudonymizer. Its contents are not publicly mutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Pseudonym(pub(crate) String);

mod sealed {
    pub trait Sealed {}
}

/// The identity representation of a record. Only raw and pseudonymized phases exist.
///
/// Arbitrary serializable types cannot become record phases:
/// ```compile_fail
/// use infertap::record::RecordData;
/// fn unsupported(record: RecordData<String>) {}
/// ```
pub trait Phase: sealed::Sealed + std::fmt::Debug + Clone + PartialEq {
    /// Block and content identity in this phase.
    type Identity: Serialize + std::fmt::Debug + Clone + PartialEq;
    /// Content identity in this phase.
    type ContentIdentity: Serialize + std::fmt::Debug + Clone + PartialEq;
    /// Operator text in this phase.
    type Text: Serialize + std::fmt::Debug + Clone + PartialEq;
}

/// Records before identity mapping.
#[derive(Debug, Clone, PartialEq)]
pub enum Raw {}
impl sealed::Sealed for Raw {}
impl Phase for Raw {
    type Identity = Id;
    type ContentIdentity = ContentId;
    type Text = OperatorText;
}

/// Records after identity mapping.
#[derive(Debug, Clone, PartialEq)]
pub enum Pseudonymized {}
impl sealed::Sealed for Pseudonymized {}
impl Phase for Pseudonymized {
    type Identity = Pseudonym;
    type ContentIdentity = Pseudonym;
    type Text = Pseudonym;
}

/// The raw record consumed by compute sinks and the pseudonymizer.
pub type Record = RecordData<Raw>;
/// A record after pseudonymization. It cannot be input to a pseudonymizing sink.
pub type PseudonymizedRecord = RecordData<Pseudonymized>;

/// A block was (re)computed and inserted into a cache tier: `kind: store`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(bound = "")]
pub struct Store<P: Phase = Raw> {
    /// The instance label the tap gives this engine.
    pub instance_id: InstanceLabel,
    /// When the engine did it.
    pub at_ms: EngineMs,
    /// The transport sequence of the message this event arrived in, where the transport
    /// numbers its messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// Data-parallel rank of the emitting worker.
    pub dp_rank: u32,
    /// KV cache group index within that worker.
    pub group_idx: u32,
    /// The engine's block hash, an identity only within `(instance, dp_rank, group_idx)`.
    pub block_id: P::Identity,
    /// The prefix parent's engine hash, where the event named one. Absent for a root.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<P::Identity>,
    /// The run's shape does not determine the parent, as in a skipped-block run. A field
    /// of its own rather than a missing `parent_id`, because absence there reads as root.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub parent_unknown: bool,
    /// Tokens in the block, as the engine declared. A count, so unsigned here; the contract
    /// types the column `int64`, and the adapter refuses a value that column cannot hold.
    pub n_tokens: u64,
    /// The cache tier, as the engine named it; `null` where the wire named none.
    pub tier: Option<Term>,
    /// The engine's `extra_keys` for this block, where `cache_salt` arrives.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_keys: Option<Vec<P::Text>>,
    /// `LoRA` adapter id, where the engine reports one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lora_id: Option<u64>,
    /// `LoRA` adapter name, where the engine reports one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lora_name: Option<P::Text>,
    /// The engine's KV cache spec for the group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_kind: Option<Term>,
    /// The group's sliding-window size, where the engine declares one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_sliding_window: Option<i64>,
    /// Whether this store reports a block already cached; only under a `labelled` declaration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reused: Option<bool>,
    /// `LOCAL` or `REMOTE`, where the publisher said.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<Term>,
    /// Engine-emitted backend identity within this publisher and tier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<P::Text>,
    /// The fleet-stable content identity, where this run could derive it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_id: Option<P::ContentIdentity>,
    /// The key epoch the identities on this record are mapped under; stamped by egress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u32>,
}

/// A block was evicted from a cache tier: `kind: evict`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(bound = "")]
pub struct Evict<P: Phase = Raw> {
    /// The instance label the tap gives this engine.
    pub instance_id: InstanceLabel,
    /// When the engine did it.
    pub at_ms: EngineMs,
    /// The transport sequence of the message this event arrived in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// Data-parallel rank of the emitting worker.
    pub dp_rank: u32,
    /// KV cache group index within that worker.
    pub group_idx: u32,
    /// The engine's hash for the removed block.
    pub block_id: P::Identity,
    /// The tier the block was removed from; `null` where the wire named none.
    pub tier: Option<Term>,
    /// `LOCAL` or `REMOTE`, where the publisher said.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<Term>,
    /// Engine-emitted backend identity within this publisher and tier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<P::Text>,
    /// The content identity resolved through the bridge, where it resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_id: Option<P::ContentIdentity>,
    /// The key epoch the identities on this record are mapped under; stamped by egress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u32>,
}

/// A tier was cleared across a declared scope: `kind: clear`, one scope-level record.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Clear {
    /// The instance label the tap gives this engine.
    pub instance_id: InstanceLabel,
    /// When the engine did it.
    pub at_ms: EngineMs,
    /// The transport sequence of the message this event arrived in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// The rank the clear applies to; absent means global across ranks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dp_rank: Option<u32>,
    /// The group the clear applies to; absent means global across groups.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_idx: Option<u32>,
    /// Always `all`: the wire does not enumerate the cleared blocks.
    pub scope: AllBlocks,
    /// The tier cleared; absent means global across tiers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<Term>,
}

/// The publisher restarted, so the cache it fed is gone: `kind: holder_reset`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HolderReset {
    /// The instance label the tap gives this engine.
    pub instance_id: InstanceLabel,
    /// When the producer detected the regression.
    pub at_ms: ProducerMs,
    /// The transport sequence of the regressed message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// The rank whose publisher regressed, where the message named exactly one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dp_rank: Option<u32>,
    /// The engine instant the reader orders the reset at.
    pub boundary_ms: EngineMs,
}

/// The boot record: `kind: agent_start`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentStart {
    /// When the producer emitted it.
    pub at_ms: ProducerMs,
    /// The crate version.
    pub agent_version: String,
    /// Configured subscription endpoints, counted.
    pub endpoint_count: u64,
    /// The payload bound in force.
    pub max_payload_bytes: u64,
    /// Whether identities leave this producer mapped or in the clear.
    pub egress: Egress,
    /// What a `store` means on this stream (contract section 2.3).
    pub reuse_reporting: ReuseReporting,
    /// The key canary, where a key is in force.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<String>,
}

/// The completeness clock: `kind: heartbeat`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Heartbeat {
    /// When the producer emitted it.
    pub at_ms: ProducerMs,
    /// The run's cumulative envelope stats.
    #[serde(flatten)]
    pub stats: crate::lifecycle::EnvelopeStats,
    /// The key canary, where a key is in force.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<String>,
}

/// The blocks a `clear` covers: all of them, always, because the wire does not enumerate
/// the cleared blocks and the tap does not invent a list. Serializes as `all`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AllBlocks;

impl Serialize for AllBlocks {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str("all")
    }
}

/// How identities leave this producer: the `egress` of an `agent_start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Egress {
    /// Every identity is mapped under the deployment key before it is written.
    Pseudonymized,
    /// Identities are written as the adapter derived them; a local-only run.
    Raw,
}

/// What a `store` means on this stream (contract section 2.3): the `reuse_reporting` every
/// lifecycle record declares. A fact about the engine, not the traffic, so it is constant
/// for a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReuseReporting {
    /// The engine reports no reuse; every `store` is an insertion.
    None,
    /// The engine marks a reuse, and a `store` carries `reused` where it is one.
    Labelled,
    /// The engine emits one shape for an insertion and a reuse, so a `store` is either
    /// and a reader must not count stores as insertions.
    Unlabelled,
}

/// Why a run ended: the `reason` of an `agent_stop`. Three are clean stops; the other
/// three are faults, and the process also exits nonzero on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// SIGTERM or SIGINT.
    Signal,
    /// `--duration` elapsed.
    Duration,
    /// `--max-batches` was reached.
    MaxBatches,
    /// The resident set exceeded the memory bound.
    MemoryBound,
    /// The output latched a write error.
    WriteError,
    /// The source failed and could not continue.
    StreamError,
}

impl StopReason {
    /// The wire spelling, the same text serde writes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Signal => "signal",
            Self::Duration => "duration",
            Self::MaxBatches => "max_batches",
            Self::MemoryBound => "memory_bound",
            Self::WriteError => "write_error",
            Self::StreamError => "stream_error",
        }
    }
}

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The departure label: `kind: agent_stop`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentStop {
    /// When the producer emitted it.
    pub at_ms: ProducerMs,
    /// Why the run ended.
    pub reason: StopReason,
    /// The run's closing envelope stats.
    #[serde(flatten)]
    pub stats: crate::lifecycle::EnvelopeStats,
    /// The key canary, where a key is in force.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<String>,
}

/// One holder's identity refusals over one engine-clock window: `kind: identity_refused`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IdentityRefused {
    /// When the producer emitted it.
    pub at_ms: ProducerMs,
    /// The instance whose holder refused.
    pub instance_id: InstanceLabel,
    /// Data-parallel rank of the refusing holder.
    pub dp_rank: u32,
    /// KV cache group of the refusing holder.
    pub group_idx: u32,
    /// Stores denied an identity promise in the window.
    pub refused: u64,
    /// The first refused store, on the engine's clock.
    pub window_start_ms: EngineMs,
    /// The last refused store, on the engine's clock.
    pub window_end_ms: EngineMs,
}

/// Every segment's first record: `kind: segment_open`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SegmentOpen {
    /// Operator-declared provenance, stated once per segment. Declared first so a typed
    /// field below wins over a provenance key of the same name; the type refuses such
    /// keys anyway.
    #[serde(flatten)]
    pub provenance: Provenance,
    /// When the producer emitted it.
    pub at_ms: ProducerMs,
    /// The contract version every record in the segment conforms to.
    pub contract_version: &'static str,
    /// `<run_start_ms>-<pid>`, this process lifetime.
    pub incarnation: String,
    /// The segment's position in this incarnation's sequence.
    pub segment_seq: u64,
    /// What this producer's traffic is for, in the operator's vocabulary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_class: Option<String>,
    /// The declared heartbeat interval.
    pub heartbeat_secs: u64,
    /// The declared maximum segment age.
    pub max_segment_secs: u64,
    /// The key epoch scoping this producer's identity space, where keyed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_epoch: Option<u32>,
}

/// A bounded, declared loss at the disk cap: `kind: segments_dropped`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SegmentsDropped {
    /// When the producer emitted it.
    pub at_ms: ProducerMs,
    /// The run whose data is gone.
    pub incarnation: String,
    /// Segments dropped from that run.
    pub count: u64,
    /// The first dropped file's name.
    pub first: String,
    /// The last dropped file's name.
    pub last: String,
    /// The first dropped sequence, where the name parsed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_seq: Option<u64>,
    /// The last dropped sequence, where the name parsed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
}

/// An unfinished segment was found at startup: `kind: segment_recovered`. Attributed, it is
/// a trailer inside the recovered segment; headerless, it is the recovering incarnation's
/// declaration that the file was discarded (contract section 4.3).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SegmentRecovered {
    /// When the producer emitted it.
    pub at_ms: ProducerMs,
    /// Whether the file carried its own header and seals under its own incarnation.
    pub attributed: bool,
    /// Bytes discarded: a torn trailing record, or the whole headerless file.
    pub dropped_bytes: u64,
}

/// One record of the stream. The `kind` field is the variant.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[serde(bound = "")]
pub enum RecordData<P: Phase> {
    /// A block stored.
    Store(Store<P>),
    /// A block evicted.
    Evict(Evict<P>),
    /// A scope cleared.
    Clear(Clear),
    /// A publisher restart.
    HolderReset(HolderReset),
    /// The boot record.
    AgentStart(AgentStart),
    /// The completeness clock.
    Heartbeat(Heartbeat),
    /// The departure label.
    AgentStop(AgentStop),
    /// A holder's identity refusals over a window.
    IdentityRefused(IdentityRefused),
    /// A segment's header.
    SegmentOpen(SegmentOpen),
    /// A declared loss at the disk cap.
    SegmentsDropped(SegmentsDropped),
    /// A recovered or discarded unfinished segment.
    SegmentRecovered(SegmentRecovered),
}

impl<P: Phase> RecordData<P> {
    /// The record's wire name.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Store(_) => "store",
            Self::Evict(_) => "evict",
            Self::Clear(_) => "clear",
            Self::HolderReset(_) => "holder_reset",
            Self::AgentStart(_) => "agent_start",
            Self::Heartbeat(_) => "heartbeat",
            Self::AgentStop(_) => "agent_stop",
            Self::IdentityRefused(_) => "identity_refused",
            Self::SegmentOpen(_) => "segment_open",
            Self::SegmentsDropped(_) => "segments_dropped",
            Self::SegmentRecovered(_) => "segment_recovered",
        }
    }

    /// The record as a JSON value, the shape every sink writes and every test compares.
    ///
    /// # Panics
    /// Never: every variant serializes, and a `Value` accepts every serializable shape.
    #[must_use]
    pub fn to_value(&self) -> Value {
        #[allow(clippy::expect_used)] // a derived Serialize over plain data cannot fail
        serde_json::to_value(self).expect("a record serializes")
    }

    /// The record as one compact JSON line, without the newline: `kind` first, then the
    /// fields in declaration order.
    ///
    /// # Panics
    /// Never: see [`to_value`](Self::to_value).
    #[must_use]
    pub fn to_json_line(&self) -> String {
        #[allow(clippy::expect_used)] // a derived Serialize over plain data cannot fail
        serde_json::to_string(self).expect("a record serializes")
    }
}

/// Why a provenance object was refused for the segment header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvenanceError {
    /// A key that would carry token content (`text`, `prompt`, ...), at any depth: the
    /// provenance must reject recognized content-bearing keys. The value is the
    /// dotted path to the key.
    Content(String),
    /// A top-level key the header types itself (`kind`, `at_ms`, `incarnation`, ...):
    /// provenance cannot overwrite what the tap states.
    Reserved(String),
}

impl std::fmt::Display for ProvenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Content(k) => write!(f, "provenance carries a content-bearing field {k:?}"),
            Self::Reserved(k) => write!(
                f,
                "provenance key {k:?} is a field the segment header states itself"
            ),
        }
    }
}

/// Content-bearing provenance keys rejected at every nesting depth.
/// Matching is exact and case-insensitive; `n_tokens` remains valid.
/// This list detects known key names, not sensitive values under arbitrary names.
const CONTENT_KEYS: &[&str] = &[
    "token_ids",
    "tokens",
    "input_ids",
    "text",
    "prompt",
    "content",
    "tensor",
    "kv",
    "embedding",
];

impl std::error::Error for ProvenanceError {}

/// The header fields provenance may not shadow: the tag and every typed field of
/// [`SegmentOpen`].
const HEADER_FIELDS: &[&str] = &[
    "kind",
    "at_ms",
    "contract_version",
    "incarnation",
    "segment_seq",
    "workload_class",
    "heartbeat_secs",
    "max_segment_secs",
    "key_epoch",
];

/// The operator's provenance object, accepted for the segment header: the one dynamic
/// map on the stream. Built only through
/// [`TryFrom`], which refuses a content-bearing key and a key the header states itself,
/// so a header cannot carry either. The empty object is the default.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Provenance(Map<String, Value>);

impl Provenance {
    /// The number of declared keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the operator declared nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The declared keys and values.
    #[must_use]
    pub const fn as_map(&self) -> &Map<String, Value> {
        &self.0
    }
}

impl TryFrom<Map<String, Value>> for Provenance {
    type Error = ProvenanceError;

    /// Accept an operator's object: no content-bearing key, no key the header states
    /// itself. Run at load, so a bad object fails the start with its reason rather than
    /// a header.
    fn try_from(provenance: Map<String, Value>) -> Result<Self, ProvenanceError> {
        for k in provenance.keys() {
            if HEADER_FIELDS.contains(&k.as_str()) {
                return Err(ProvenanceError::Reserved(k.clone()));
            }
        }
        no_content_key(&Value::Object(provenance.clone()), &mut String::new())?;
        Ok(Self(provenance))
    }
}

/// Refuse a content-bearing key at any depth of `v`; `at` is the dotted path so far.
fn no_content_key(v: &Value, at: &mut String) -> Result<(), ProvenanceError> {
    use std::fmt::Write as _;
    match v {
        Value::Object(map) => {
            for (k, item) in map {
                let len = at.len();
                if !at.is_empty() {
                    at.push('.');
                }
                at.push_str(k);
                if CONTENT_KEYS.iter().any(|c| k.eq_ignore_ascii_case(c)) {
                    return Err(ProvenanceError::Content(at.clone()));
                }
                no_content_key(item, at)?;
                at.truncate(len);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                let len = at.len();
                let _ = write!(at, "[{i}]");
                no_content_key(item, at)?;
                at.truncate(len);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The reserved roster is exactly the header's serialized keys: a typed field added to
    /// `SegmentOpen` without a roster entry would let provenance shadow it, and this is
    /// what catches that.
    #[test]
    fn the_reserved_header_fields_are_exactly_the_segment_open_keys() {
        let header = Record::SegmentOpen(SegmentOpen {
            provenance: Provenance::default(),
            at_ms: ProducerMs(0.0),
            contract_version: "0.0",
            incarnation: "0-0".to_string(),
            segment_seq: 0,
            workload_class: Some("test".to_string()),
            heartbeat_secs: 1,
            max_segment_secs: 1,
            key_epoch: Some(0),
        });
        let value = header.to_value();
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        let mut roster = HEADER_FIELDS.to_vec();
        roster.sort_unstable();
        assert_eq!(keys, roster);
    }

    #[test]
    fn provenance_refuses_a_content_key_and_a_reserved_key() {
        let mut m = Map::new();
        m.insert("gpu".to_string(), Value::from("h100"));
        assert_eq!(Provenance::try_from(m.clone()).unwrap().len(), 1);
        let mut reserved = m.clone();
        reserved.insert("incarnation".to_string(), Value::from("mine"));
        assert!(matches!(
            Provenance::try_from(reserved),
            Err(ProvenanceError::Reserved(k)) if k == "incarnation"
        ));
        // A content key is refused case-insensitively; a token count is not content.
        let mut counted = m.clone();
        counted.insert("n_tokens".to_string(), Value::from(64));
        assert!(Provenance::try_from(counted).is_ok());
        m.insert("Prompt".to_string(), Value::from("hello"));
        assert!(matches!(
            Provenance::try_from(m),
            Err(ProvenanceError::Content(k)) if k == "Prompt"
        ));
    }

    /// The content guard reaches every depth: a content key nested under a benign one,
    /// or inside an array of objects, is refused and named by its path. A top-level
    /// check alone let `{"deployment": {"token_ids": [...]}}` into a header.
    #[test]
    fn provenance_refuses_a_content_key_at_any_depth() {
        let nested: Map<String, Value> =
            serde_json::from_str(r#"{"deployment": {"gpu": "h100", "token_ids": [101, 202]}}"#)
                .unwrap();
        assert!(matches!(
            Provenance::try_from(nested),
            Err(ProvenanceError::Content(k)) if k == "deployment.token_ids"
        ));
        let in_array: Map<String, Value> =
            serde_json::from_str(r#"{"nodes": [{"gpu": "h100"}, {"Text": "x"}]}"#).unwrap();
        assert!(matches!(
            Provenance::try_from(in_array),
            Err(ProvenanceError::Content(k)) if k == "nodes[1].Text"
        ));
        let deep_but_clean: Map<String, Value> =
            serde_json::from_str(r#"{"deployment": {"n_tokens": 4, "tiers": ["GPU"]}}"#).unwrap();
        assert_eq!(Provenance::try_from(deep_but_clean).unwrap().len(), 1);
    }

    /// Envelope stats with every counter at zero.
    fn empty_stats() -> crate::lifecycle::EnvelopeStats {
        crate::lifecycle::EnvelopeStats {
            msgs_seen: 0,
            dropped: 0,
            oversized: 0,
            noraw_scanned: None,
            unknown_types: 0,
            events_ingested: 0,
            content_unresolved: 0,
            content_bridge_entries: 0,
            content_bridge_refused: 0,
            publisher_restarts: 0,
            rss_bytes: None,
            reuse_reporting: ReuseReporting::Unlabelled,
            endpoints: Vec::new(),
        }
    }

    /// One record of every variant, with nothing in it but what the type requires.
    fn one_of_each() -> Vec<Record> {
        use crate::testing::store;
        let stats = empty_stats();
        let at = ProducerMs(0.0);
        vec![
            Record::Store(store(1)),
            Record::Evict(Evict {
                instance_id: "i0".into(),
                at_ms: EngineMs(0.0),
                seq: None,
                dp_rank: 0,
                group_idx: 0,
                block_id: Id(1),
                tier: None,
                locality: None,
                backend_id: None,
                content_id: None,
                epoch: None,
            }),
            Record::Clear(Clear {
                instance_id: "i0".into(),
                at_ms: EngineMs(0.0),
                seq: None,
                dp_rank: None,
                group_idx: None,
                scope: AllBlocks,
                tier: None,
            }),
            Record::HolderReset(HolderReset {
                instance_id: "i0".into(),
                at_ms: at,
                seq: None,
                dp_rank: None,
                boundary_ms: EngineMs(0.0),
            }),
            Record::AgentStart(AgentStart {
                at_ms: at,
                agent_version: "0".into(),
                endpoint_count: 0,
                max_payload_bytes: 0,
                egress: Egress::Raw,
                reuse_reporting: ReuseReporting::Unlabelled,
                canary: None,
            }),
            Record::Heartbeat(Heartbeat {
                at_ms: at,
                stats: stats.clone(),
                canary: None,
            }),
            Record::AgentStop(AgentStop {
                at_ms: at,
                reason: StopReason::Signal,
                stats,
                canary: None,
            }),
            Record::IdentityRefused(IdentityRefused {
                at_ms: at,
                instance_id: "i0".into(),
                dp_rank: 0,
                group_idx: 0,
                refused: 1,
                window_start_ms: EngineMs(0.0),
                window_end_ms: EngineMs(0.0),
            }),
            Record::SegmentOpen(SegmentOpen {
                provenance: Provenance::default(),
                at_ms: at,
                contract_version: "0.0",
                incarnation: "0-0".into(),
                segment_seq: 0,
                workload_class: None,
                heartbeat_secs: 0,
                max_segment_secs: 0,
                key_epoch: None,
            }),
            Record::SegmentsDropped(SegmentsDropped {
                at_ms: at,
                incarnation: "0-0".into(),
                count: 1,
                first: "a".into(),
                last: "a".into(),
                first_seq: None,
                last_seq: None,
            }),
            Record::SegmentRecovered(SegmentRecovered {
                at_ms: at,
                attributed: true,
                dropped_bytes: 0,
            }),
        ]
    }

    /// `kind()` is the serde tag, variant by variant: a renamed variant or a hand-edited
    /// name would split the wire name from the one the sinks count and the converter
    /// dispatches on.
    #[test]
    fn the_kind_is_the_serde_tag_for_every_variant() {
        let records = one_of_each();
        assert_eq!(records.len(), 11, "every variant is sampled");
        for record in records {
            assert_eq!(record.to_value()["kind"], record.kind(), "{record:?}");
        }
    }

    #[test]
    fn the_stream_vocabulary_serializes_as_the_contract_spells_it() {
        assert_eq!(serde_json::to_value(AllBlocks).unwrap(), "all");
        assert_eq!(
            serde_json::to_value(Egress::Pseudonymized).unwrap(),
            "pseudonymized"
        );
        assert_eq!(serde_json::to_value(ReuseReporting::None).unwrap(), "none");
        assert_eq!(
            serde_json::to_value(ReuseReporting::Unlabelled).unwrap(),
            "unlabelled"
        );
        assert_eq!(
            serde_json::to_value(StopReason::MaxBatches).unwrap(),
            "max_batches"
        );
        assert_eq!(StopReason::WriteError.as_str(), "write_error");
    }
}
