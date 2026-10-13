// SPDX-License-Identifier: Apache-2.0
//! Engine adapters and subscription interfaces.
//!
//! [`EventSource`] exposes polling, event draining, and collection counters. Adapters
//! normalize engine-specific payloads and discard token IDs after identity derivation.
//! [`connect`] selects the configured engine adapter.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::EngineMs;
use crate::event_model::Event;
use crate::record::ReuseReporting;

shared_text! {
    /// Subscription identifier used for sequence tracking and content lookup.
    /// Data-parallel expansion appends `#<rank>` to the operator's endpoint label.
    /// Records carry the shared [`InstanceLabel`](crate::record::InstanceLabel) instead.
    /// The text is reference counted so each event does not copy it.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    SourceKey
}

impl SourceKey {
    /// The shared text, for a label that is the key itself.
    pub(crate) fn shared(&self) -> Arc<str> {
        Arc::clone(&self.0)
    }
}

impl std::borrow::Borrow<str> for SourceKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// One configured subscription: the key the tap knows it by and the address it dials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The tap's key for the subscription.
    pub source: SourceKey,
    /// The address the tap dials, `tcp://host:port`.
    pub address: String,
}

/// A normalized event with its subscription and source message sequence.
/// All events decoded from one message share `seq`. Sequence gaps therefore measure
/// missing messages, not missing events.
#[derive(Debug, Clone, PartialEq)]
pub struct Sourced {
    /// The subscription the message arrived on.
    pub source: SourceKey,
    /// The event.
    pub event: Event,
    /// The transport sequence of the message.
    pub seq: u64,
}

/// What a memory shed discarded: the queued messages and their payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Shed {
    /// Messages discarded without decode.
    pub messages: u64,
    /// Their payload bytes.
    pub bytes: u64,
}

pub mod sglang;
pub mod vllm;
pub mod wire;

/// Source construction and delivery errors.
///
/// [`Message`](Self::Message) rejects one malformed message while collection continues.
/// Other variants stop collection. A later sequence can expose the rejected message
/// as a gap.
#[derive(Debug)]
pub enum SourceError {
    /// No engine by this name; the message carries this build's roster.
    UnknownEngine {
        /// The name that was asked for.
        engine: String,
    },
    /// This build lacks the engine's live transport; `feature` is the Cargo feature that
    /// carries it.
    TransportUnavailable {
        /// The engine whose transport is missing.
        engine: Engine,
        /// The Cargo feature that carries it.
        feature: &'static str,
    },
    /// Connecting to a configured endpoint failed at startup.
    Connect(io::Error),
    /// Connecting to a configured replay endpoint, or draining it, failed at startup.
    Replay(io::Error),
    /// The transport failed mid-run.
    Transport(io::Error),
    /// One message could not be read. Skippable: see the type doc.
    Message {
        /// Where the message failed: its envelope, or its payload.
        stage: MessageStage,
        /// The adapter's error. It may contain payload values and is not logged.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

/// Where a skippable message failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageStage {
    /// The multipart envelope did not match the adapter's framing.
    Frame,
    /// The payload did not decode, or its schema drifted.
    Decode,
}

impl SourceError {
    /// Whether the run must stop on this error. Only a bad message is survivable.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        !matches!(self, Self::Message { .. })
    }
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownEngine { engine } => write!(
                f,
                "unknown engine {engine:?}; this build supports: {}",
                Engine::ALL
                    .iter()
                    .map(|e| e.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::TransportUnavailable { engine, feature } => write!(
                f,
                "this build has no live {engine} transport; rebuild with --features {feature} \
                 (or run show-payload, which needs no engine)"
            ),
            Self::Connect(e) => write!(f, "connecting: {e}"),
            Self::Replay(e) => write!(f, "replay socket: {e}"),
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::Message { stage, .. } => f.write_str(match stage {
                MessageStage::Frame => "skipped a message: invalid envelope",
                MessageStage::Decode => "skipped a message: invalid payload",
            }),
        }
    }
}

impl std::error::Error for SourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(e) | Self::Replay(e) | Self::Transport(e) => Some(e),
            Self::Message { source, .. } => Some(source.as_ref()),
            Self::UnknownEngine { .. } | Self::TransportUnavailable { .. } => None,
        }
    }
}

/// Maximum accepted payload size, enforced before decoding. Adapters reject and
/// count oversized frames; transports also enforce their socket message-size limit.
pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Content-identity completeness and resource counters.
/// Adapters that do not derive content identities report the default zero counters.
/// Nonzero `unresolved` means emitted records lack some content identities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IdentityHealth {
    /// Blocks without a resolved content identity.
    pub unresolved: u64,
    /// Live entries in the adapter's identity state: its resident set, under
    /// delete-on-evict retention.
    pub entries: u64,
    /// Identity promises refused at the declared resident cap. Nonzero means residency
    /// exceeded the declared capacity and some blocks will not resolve.
    pub refused: u64,
    /// Publisher sequence resets observed. Each reset clears stale adapter identity state.
    pub publisher_restarts: u64,
    /// Clears with an undeclared scope dimension, applied globally over that dimension.
    pub unscoped_clears: u64,
    /// Conflicting content derivations refused for an existing engine block identity.
    /// Reported in the adapter's closing diagnostics, not in heartbeat records.
    pub identity_conflicts: u64,
}

/// Identity refusals for one origin over an engine-clock window.
/// The adapter drains these windows for `identity_refused` records. Window storage
/// is bounded; aggregate counters can include refusals without a retained window.
#[derive(Debug, Clone, PartialEq)]
pub struct IdentityRefusal {
    /// The subscription whose holder refused.
    pub source: SourceKey,
    /// Data-parallel rank of the refusing holder.
    pub dp_rank: u32,
    /// KV cache group of the refusing holder.
    pub group_idx: u32,
    /// The first refused store in the window.
    pub window_start_ms: EngineMs,
    /// The last refused store in the window.
    pub window_end_ms: EngineMs,
    /// Stores that exceeded identity-state or residency-accounting limits.
    pub refused: u64,
}

/// An adapter's accounting, read in one call: the stream's completeness figures and the
/// content-identity health. Cheap to read (every field is a counter), so the run loop
/// reads it on every wake without allocating.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceStats {
    /// Messages observed across all sources.
    pub messages_seen: u64,
    /// Messages lost between observed sequences across all sources. Tail loss may be undetected.
    pub dropped: u64,
    /// Messages rejected for exceeding [`MAX_PAYLOAD_BYTES`].
    pub oversized: u64,
    /// Events skipped because the adapter does not implement their kind. A drift signal:
    /// nonzero means the engine emits something this build does not model.
    pub unknown_types: u64,
    /// Messages skipped because they could not be framed or decoded
    /// ([`SourceError::Message`]). Each is a sequence gap to the reader; this is the local
    /// count for the closing report.
    pub undecodable: u64,
    /// The adapter's content-identity accounting. All zeros reads as "this adapter does
    /// not derive one".
    pub identity: IdentityHealth,
}

/// An engine adapter: poll for events, drain buffered events, and report accounting.
/// Engine-specific framing and identity derivation remain behind this interface.
pub trait EventSource {
    /// Poll once and buffer any normalized events; returns how many were buffered
    /// (`0` = idle tick, keep polling or stop).
    ///
    /// # Errors
    /// A transport failure is fatal ([`SourceError::is_fatal`]). A malformed frame or an
    /// undecodable payload is [`SourceError::Message`]: the adapter yields nothing derived
    /// from the misread message, counts it, and the caller keeps polling.
    fn poll(&mut self) -> Result<usize, SourceError>;

    /// Pop the next buffered event, with its source and its message's sequence.
    fn next_event(&mut self) -> Option<Sourced>;

    /// The topic this source last published under, verbatim and unparsed, where the transport
    /// names one. Relayed on the heartbeat's per-endpoint entry rather than per record: one
    /// publisher uses one topic for its lifetime.
    fn topic_for(&self, source: &SourceKey) -> Option<&str>;

    /// What a `store` means on this adapter's stream (contract section 2.3). A fact about
    /// the engine, not the traffic, so it is a constant per adapter.
    fn reuse_reporting(&self) -> ReuseReporting;

    /// Discard everything the transport has queued without decoding it: the memory shed's
    /// hook. The loss is declared through sequence gaps. The default has no transport
    /// queue to shed.
    fn shed_queue(&mut self) -> Shed {
        Shed::default()
    }

    /// The adapter's accounting, in one read: see [`SourceStats`].
    fn stats(&self) -> SourceStats;
    /// Wire fields the adapter saw but does not model, as `Kind.field`. The drift signal
    /// for fields, beside [`SourceStats::unknown_types`] for kinds: a renamed field this
    /// build depends on shows up here instead of vanishing silently. Defaults to none;
    /// read once, at the closing report.
    fn unknown_fields(&self) -> Vec<String> {
        Vec::new()
    }
    /// Messages observed on one subscription.
    fn seen(&self, source: &SourceKey) -> u64;
    /// Messages known lost on one subscription.
    fn dropped(&self, source: &SourceKey) -> u64;
    /// Sequence regressions observed on one subscription. Defaults to zero: an adapter
    /// without a numbered transport has none to observe.
    fn restarts(&self, source: &SourceKey) -> u64 {
        let _ = source;
        0
    }

    /// Take the identity refusals accumulated since the last drain, one per affected holder.
    /// Defaults to none: an adapter without a derived identity has nothing to refuse.
    fn drain_identity_refusals(&mut self) -> Vec<IdentityRefusal> {
        Vec::new()
    }
}

/// Facts about a successful [`connect`] that a caller may want to report. Returned rather
/// than logged, so the binary decides what reaches stderr.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConnectReport {
    /// Replay endpoints whose drain ended on a timeout, a deadline, the byte budget, or an
    /// error rather than the END marker. The history past that point is what the reader's
    /// guards declare missing; the run proceeds.
    pub replay_incomplete: u64,
    /// The identity cap was reduced to fit the aggregate RSS budget. The caller reports the reduction.
    pub resident_cap_clamped: Option<ResidentCapClamp>,
}

/// See [`ConnectReport::resident_cap_clamped`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentCapClamp {
    /// The per-origin cap the operator declared.
    pub declared: usize,
    /// The smaller cap actually enforced.
    pub enforced: usize,
    /// Subscribed sources the budget was divided over.
    pub sources: usize,
    /// Bytes per bridge entry, the analytic figure.
    pub entry_bytes: u64,
    /// The RSS bound the budget is a quarter of.
    pub max_rss_bytes: u64,
}

/// The engines this build can name. Parsed from the operator's word at the command line,
/// so an unknown engine is a usage error before anything connects; [`connect`] is the one
/// place a variant maps to concrete adapter code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Engine {
    /// vLLM's `ZmqEventPublisher`, over the `zmq` feature's transport.
    #[default]
    Vllm,
    /// `SGLang`'s `ZmqEventPublisher`. It speaks vLLM's wire format, so the same transport
    /// and adapter serve it under its own [`wire::Profile`].
    Sglang,
}

impl Engine {
    /// The engine's profile of the shared wire adapter.
    #[must_use]
    pub const fn profile(self) -> &'static dyn wire::Profile {
        match self {
            Self::Vllm => &vllm::Vllm,
            Self::Sglang => &sglang::Sglang,
        }
    }
}

impl Engine {
    /// Every engine this build can name, in the roster's order.
    pub const ALL: &[Self] = &[Self::Vllm, Self::Sglang];

    /// The operator's word for the engine.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vllm => "vllm",
            Self::Sglang => "sglang",
        }
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Engine {
    type Err = SourceError;

    /// # Errors
    /// [`SourceError::UnknownEngine`], carrying the roster.
    fn from_str(engine: &str) -> Result<Self, SourceError> {
        Self::ALL
            .iter()
            .copied()
            .find(|e| e.as_str() == engine)
            .ok_or_else(|| SourceError::UnknownEngine {
                engine: engine.to_string(),
            })
    }
}

/// What [`connect`] needs to build an adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectConfig {
    /// The engine whose adapter to build.
    pub engine: Engine,
    /// The subscriptions to dial.
    pub endpoints: Vec<Endpoint>,
    /// The publisher's replay sockets, keyed like `endpoints`, where the engine configures
    /// them. The recovered ring is served before live traffic, and a dead replay endpoint
    /// costs a bounded timeout and some history, never the run. Replay stays outbound
    /// only, to configured endpoints, with no listener (trust document, section 1).
    pub replay: Vec<Endpoint>,
    /// How long one poll blocks before returning idle.
    pub poll_timeout: Duration,
    /// The declared per-instance resident-block capacity across subscribed tiers, bounding
    /// the adapter's identity state; `None` takes the adapter's own derived default. The
    /// arithmetic from tier sizes to blocks is in the install docs.
    pub resident_blocks: Option<usize>,
    /// The process's memory bound, which the identity state's budget is a quarter of.
    pub max_rss_bytes: u64,
}

/// Construct the adapter `cfg` names, connected. The caller talks to the result only
/// through [`EventSource`].
///
/// # Errors
/// [`SourceError::TransportUnavailable`] when this build lacks the engine's transport,
/// [`SourceError::Connect`] for the transport's own connect failure, or
/// [`SourceError::Replay`] for a replay socket's.
pub fn connect(cfg: &ConnectConfig) -> Result<(Box<dyn EventSource>, ConnectReport), SourceError> {
    match cfg.engine {
        // Both engines publish the same wire format: one transport and one adapter serve
        // both, and the engine selects the adapter's profile.
        Engine::Vllm | Engine::Sglang => {
            #[cfg(feature = "zmq")]
            {
                let mut transport = wire::transport::ZmqSubTransport::connect(&cfg.endpoints)
                    .map_err(|e| SourceError::Connect(io::Error::other(e)))?;
                transport
                    .drain_replay(&cfg.replay)
                    .map_err(|e| SourceError::Replay(io::Error::other(e)))?;
                let declared = cfg
                    .resident_blocks
                    .unwrap_or(wire::content::DEFAULT_RESIDENT_CAP);
                let sources = cfg.endpoints.len();
                let cap = wire::content::budgeted_cap(declared, sources, cfg.max_rss_bytes);
                let report = ConnectReport {
                    replay_incomplete: transport.replay_incomplete(),
                    // The declared cap did not fit the bridge's budget, a quarter of
                    // --max-rss-bytes divided over this many sources. Reported so the
                    // caller can warn.
                    resident_cap_clamped: (cap < declared).then_some(ResidentCapClamp {
                        declared,
                        enforced: cap,
                        sources,
                        entry_bytes: wire::content::BRIDGE_ENTRY_BYTES,
                        max_rss_bytes: cfg.max_rss_bytes,
                    }),
                };
                Ok((
                    Box::new(
                        wire::Subscriber::new(transport, cfg.engine.profile(), cfg.poll_timeout)
                            .with_resident_cap(cap),
                    ),
                    report,
                ))
            }
            #[cfg(not(feature = "zmq"))]
            {
                Err(SourceError::TransportUnavailable {
                    engine: cfg.engine,
                    feature: "zmq",
                })
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::Engine;

    #[test]
    fn rejected_message_diagnostics_do_not_include_payload_values() {
        let error = super::SourceError::Message {
            stage: super::MessageStage::Decode,
            source: Box::new(super::wire::DecodeError::NotU32 {
                what: "lora_id",
                value: serde_json::json!({"text": "private-payload-canary"}),
            }),
        };
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("skipped a message"));
        assert!(!diagnostic.contains("private-payload-canary"));
        assert!(!error.is_fatal());
    }

    #[test]
    fn the_default_engine_is_on_the_roster_and_round_trips_its_name() {
        assert!(Engine::ALL.contains(&Engine::default()));
        for e in Engine::ALL {
            assert_eq!(e.as_str().parse::<Engine>().expect("round-trips"), *e);
        }
    }

    #[test]
    fn an_unknown_engine_is_rejected_with_the_roster() {
        let err = "not-an-engine".parse::<Engine>().expect_err("must err");
        assert!(matches!(err, super::SourceError::UnknownEngine { .. }));
        assert!(err.is_fatal());
        let text = err.to_string();
        assert!(text.contains("unknown engine"));
        assert!(text.contains("vllm")); // the roster is in the message
        assert!(text.contains("sglang"));
    }
}
