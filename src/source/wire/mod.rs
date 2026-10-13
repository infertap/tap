// SPDX-License-Identifier: Apache-2.0
//! The KV-event wire vLLM and `SGLang` share: three ZMQ frames carrying a `MessagePack`
//! batch of tagged events. What differs per engine is behind [`adapter::Profile`].
//!
//! `Subscriber` composes framing, sequence tracking, decoding, normalization, and
//! buffering over `FrameTransport`. The live ZMQ transport and in-memory test transport
//! exercise the same pipeline. Fixture tests validate modeled payloads; live engine tests
//! are also needed to establish wire compatibility and lifecycle behavior.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::time::Duration;

use crate::clock::{EngineMs, ProducerMs};
use crate::event_model::{Cleared, Event, HolderReset, Removed, Stored};
use crate::record::ReuseReporting;
use crate::source::{Shed, SourceKey, Sourced};

pub(crate) mod adapter;
pub mod framing;
#[cfg(feature = "zmq")]
pub mod transport;

pub mod content;
pub use adapter::{
    decode_payload, normalize_batch, unsigned, DecodeError, Fields, Normalized, Profile,
};

use framing::{parse_frame, FrameError, SeqTracker};

// The payload bound is engine-neutral policy owned by the adapter boundary; this adapter
// enforces it in `poll` (reject-and-count) and its ZMQ transport sets it as ZMQ_MAXMSGSIZE.
pub use crate::source::MAX_PAYLOAD_BYTES;

/// A source of raw publisher messages: multipart frames tagged with the source instance
/// they arrived from.
///
/// This is the swap point. A live deployment uses the ZMQ SUB socket; a test uses an in-memory
/// queue; a future replay tool could read a capture file. None of the pipeline above changes.
pub trait FrameTransport {
    /// Wait up to `timeout` for the next message.
    ///
    /// `Ok(None)` means the timeout elapsed with nothing available (an idle tick, the stream
    /// is still open); `Ok(Some(message))` is one multipart message.
    ///
    /// # Errors
    /// Returns the transport's I/O error if the poll itself fails.
    fn poll(&mut self, timeout: Duration) -> io::Result<Option<Message>>;

    /// Discard everything currently queued without decoding it: the memory shed's hook.
    /// The default has no queue of its own to shed, because replay and in-memory
    /// transports hold their messages in process structures the ordinary drop path
    /// already bounds.
    fn shed_queue(&mut self) -> Shed {
        Shed::default()
    }
}

/// One multipart message as the transport delivered it, with the subscription it arrived
/// on. The parts are the publisher's frames, unparsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The subscription the message arrived on.
    pub source: SourceKey,
    /// The multipart frames, in wire order.
    pub parts: Vec<Vec<u8>>,
}

/// A failure while receiving or decoding one message. Every variant fails closed: a
/// malformed frame, an unparseable payload, or schema drift stops the message rather than
/// letting a mis-decode through.
#[derive(Debug)]
pub enum RecvError {
    /// The transport (socket) failed to deliver the message.
    Transport(io::Error),
    /// The multipart envelope did not match the publisher framing.
    Frame(FrameError),
    /// The payload was not valid msgpack, or its schema drifted from the pinned model.
    Decode(DecodeError),
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::Frame(e) => write!(f, "{e}"),
            Self::Decode(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RecvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
            Self::Frame(e) => Some(e),
            Self::Decode(e) => Some(e),
        }
    }
}

/// Drives a [`FrameTransport`]: polls one message, normalizes it into events, and buffers
/// them for the caller to drain. Tracks the publisher sequence so dropped messages are
/// counted, not silently undercounted.
///
/// It is poll-driven rather than an iterator: a live stream has no end and can be idle, so the
/// caller decides when to stop (a duration window, a signal, a batch cap) by looping
/// [`poll`](Self::poll) and [`EventSource::next_event`](crate::source::EventSource::next_event).
#[derive(Debug)]
pub struct Subscriber<T> {
    transport: T,
    /// The engine's profile: the field table, how a hash is read, the salt's place, which
    /// tiers derive, and what a store means.
    profile: &'static dyn adapter::Profile,
    tracker: SeqTracker,
    pending: VecDeque<Sourced>,
    /// The topic each source last published under, kept as observed and never parsed.
    topics: HashMap<SourceKey, String>,
    poll_timeout: Duration,
    oversized: u64,
    unknown_types: u64,
    /// Messages refused by framing or decoding. Later sequence numbers can expose gaps;
    /// a skipped message at the end of a stream need not appear in the gap count.
    undecodable: u64,
    /// Wire fields this adapter does not read, `Kind.field`, bounded so a publisher that
    /// invents keys cannot grow it without limit.
    unknown_fields: std::collections::BTreeSet<String>,
    /// One content bridge per source rather than one shared map. The bridge keys on
    /// `Origin`, and two engine instances each running a single worker are both
    /// `dp_rank: 0`, so a shared map would let one instance's block resolve another's
    /// remove. A per-source map also makes restart handling exact: a publisher restart
    /// invalidates one source's entries only.
    bridges: HashMap<SourceKey, content::ContentBridge>,
    /// Per-instance resident-block cap each bridge is built with (the bridge's population is
    /// the resident set, so the cap is per instance by construction).
    resident_cap: usize,
    publisher_restarts: u64,
    /// Restarts per source. The envelope's `publisher_restarts` is this map's sum; the
    /// per-endpoint entry is what gives a reader the scope the sum discards.
    restarts_by_source: HashMap<SourceKey, u64>,
    /// A detected publisher restart, held per source until the stream yields the
    /// engine-clock instant it is ordered at: the earliest event of the regressed message,
    /// or of the first later message where the regressed one carried no events.
    pending_resets: HashMap<SourceKey, PendingReset>,
    /// The producer clock, a function so tests can pin it.
    now: fn() -> ProducerMs,
}

/// Distinct unknown field names one subscriber remembers. Real drift is a handful of
/// names; past this the roster is a publisher inventing keys, and the count of kinds
/// already says the stream is not what this build models.
const MAX_UNKNOWN_FIELDS: usize = 64;

/// See [`Subscriber::pending_resets`].
#[derive(Debug)]
struct PendingReset {
    detected_at_ms: ProducerMs,
    seq: u64,
}

/// The single worker every event in a batch names, where they all name the same one.
/// `Stored` and `Removed` always name one. A `Cleared` may declare none, meaning global
/// across ranks, which yields `None` as disagreement does: the reset stays scoped to the
/// whole source rather than guessing a rank.
fn uniform_dp_rank(events: &[Event]) -> Option<u32> {
    let mut ranks = events.iter().map(|e| match e {
        Event::Stored(Stored { origin, .. }) | Event::Removed(Removed { origin, .. }) => {
            Some(origin.dp_rank)
        }
        Event::Cleared(Cleared { dp_rank, .. }) => *dp_rank,
        // Never in a normalized batch: the subscriber synthesizes it, the wire cannot.
        Event::HolderReset(_) => None,
    });
    let first = ranks.next()??;
    ranks.all(|d| d == Some(first)).then_some(first)
}

impl<T: FrameTransport> Subscriber<T> {
    /// Wrap a transport, decoding under `profile`. `poll_timeout` bounds how long a single
    /// [`poll`](Self::poll) blocks waiting for a message before returning idle, so a
    /// caller's stop condition stays responsive.
    #[must_use]
    pub fn new(
        transport: T,
        profile: &'static dyn adapter::Profile,
        poll_timeout: Duration,
    ) -> Self {
        Self {
            transport,
            profile,
            tracker: SeqTracker::new(),
            pending: VecDeque::new(),
            topics: HashMap::new(),
            poll_timeout,
            oversized: 0,
            unknown_types: 0,
            undecodable: 0,
            unknown_fields: std::collections::BTreeSet::new(),
            bridges: HashMap::new(),
            resident_cap: content::DEFAULT_RESIDENT_CAP,
            publisher_restarts: 0,
            restarts_by_source: HashMap::new(),
            pending_resets: HashMap::new(),
            now: ProducerMs::now,
        }
    }

    /// Declare the per-instance resident-block cap (see
    /// [`content::DEFAULT_RESIDENT_CAP`] for the derivation the default carries). Applies to
    /// bridges created after the call, which in practice means before the first poll.
    #[must_use]
    pub fn with_resident_cap(mut self, resident_cap: usize) -> Self {
        self.resident_cap = resident_cap.max(1);
        self
    }

    /// Poll once and buffer normalized events. Return the number buffered.
    /// Zero can mean an idle poll or a message with no modeled events.
    ///
    /// # Errors
    /// Returns [`RecvError`] for transport, framing, or decoding failures. Rejected
    /// messages increment `undecodable`; their sequences are not observed. A later
    /// accepted message can expose the gap. A partially normalized batch may have
    /// updated bridge state before rejection.
    pub fn poll(&mut self) -> Result<usize, RecvError> {
        let Some(Message { source, parts }) = self
            .transport
            .poll(self.poll_timeout)
            .map_err(RecvError::Transport)?
        else {
            return Ok(0);
        };
        let (topic, seq, payload) = match parse_frame(&parts) {
            Ok(f) => f,
            Err(e) => {
                self.undecodable += 1;
                return Err(RecvError::Frame(e));
            }
        };
        // The engine's own name for its channel, kept as observed. Per source rather than
        // per record: one publisher uses one topic for its lifetime, and a reader joins it
        // to the records through `endpoints[].source`, which equals `instance_id`.
        if let Some(t) = topic {
            let cur = self.topics.entry(source.clone()).or_default();
            if cur != t {
                cur.clear();
                cur.push_str(t);
            }
        }
        // A sequence regression clears the source's retained identity state.
        // Subsequent stores repopulate the bridge; unresolved parents remain counted.
        if self.tracker.regressed(&source, seq) {
            self.note_regression(&source, seq);
        }
        // Reject oversized payloads before decode and continue collection. Observe the
        // sequence so this rejection is counted as oversized rather than as a gap.
        if payload.len() > MAX_PAYLOAD_BYTES {
            self.tracker.observe(&source, seq);
            self.oversized += 1;
            return Ok(0);
        }
        let value = match decode_payload(payload) {
            Ok(v) => v,
            Err(e) => {
                self.undecodable += 1;
                return Err(RecvError::Decode(e));
            }
        };
        let bridge = self
            .bridges
            .entry(source.clone())
            .or_insert_with(|| content::ContentBridge::new(self.resident_cap));
        let batch = match normalize_batch(self.profile, &value, bridge) {
            Ok(b) => b,
            Err(e) => {
                self.undecodable += 1;
                return Err(RecvError::Decode(e));
            }
        };
        // Observed only now: a message that failed above leaves its sequence unrecorded, so
        // the next processed message's gap counts it as lost.
        self.tracker.observe(&source, seq);
        Ok(self.queue(&source, batch, seq))
    }

    /// Record a sequence regression and clear the source's identity state.
    /// Emit a holder reset before processing events from the reset sequence.
    fn note_regression(&mut self, source: &SourceKey, seq: u64) {
        if let Some(b) = self.bridges.get_mut(source) {
            b.clear();
        }
        self.publisher_restarts += 1;
        *self.restarts_by_source.entry(source.clone()).or_default() += 1;
        self.pending_resets.insert(
            source.clone(),
            PendingReset {
                detected_at_ms: (self.now)(),
                seq,
            },
        );
    }

    /// Buffer a normalized batch: its drift counts, then a held reset where one is pending
    /// so stream order and timestamp order agree (contract section 2.3), then every event
    /// stamped with the message's sequence. The sequence identifies the message, which is
    /// what lets a reader localize a loss to it (contract section 5.4). Returns how many
    /// events were buffered.
    fn queue(&mut self, source: &SourceKey, batch: Normalized, seq: u64) -> usize {
        self.unknown_types += batch.unknown_types;
        for f in batch.unknown_fields {
            if self.unknown_fields.len() < MAX_UNKNOWN_FIELDS || self.unknown_fields.contains(&f) {
                self.unknown_fields.insert(f);
            }
        }
        let mut n = batch.events.len();
        if !batch.events.is_empty() {
            if let Some(pr) = self.pending_resets.remove(source) {
                // `boundary_ms` is the batch's earliest event; `dp_rank` is stamped only
                // where every event names the same one: observed, never guessed.
                let boundary_ms = batch
                    .events
                    .iter()
                    .map(Event::engine_ms)
                    .fold(EngineMs(f64::INFINITY), EngineMs::min);
                let reset = Event::HolderReset(HolderReset {
                    boundary_ms,
                    detected_at_ms: pr.detected_at_ms,
                    dp_rank: uniform_dp_rank(&batch.events),
                });
                self.pending.push_back(Sourced {
                    source: source.clone(),
                    event: reset,
                    seq: pr.seq,
                });
                n += 1;
            }
        }
        self.pending
            .extend(batch.events.into_iter().map(|event| Sourced {
                source: source.clone(),
                event,
                seq,
            }));
        n
    }

    /// Publisher restarts observed on one source, as sequence regressions. Zero is a
    /// measurement: the source was watched and no regression arrived.
    #[must_use]
    pub fn restarts(&self, source: &SourceKey) -> u64 {
        self.restarts_by_source.get(source).copied().unwrap_or(0)
    }
}

/// The engine-agnostic face (see [`crate::source::EventSource`]): everything above the
/// adapter boundary talks to this, never to the wire pipeline directly.
impl<T: FrameTransport> crate::source::EventSource for Subscriber<T> {
    fn shed_queue(&mut self) -> Shed {
        self.transport.shed_queue()
    }

    fn poll(&mut self) -> Result<usize, crate::source::SourceError> {
        use crate::source::{MessageStage, SourceError};
        Subscriber::poll(self).map_err(|e| match e {
            RecvError::Transport(io) => SourceError::Transport(io),
            RecvError::Frame(f) => SourceError::Message {
                stage: MessageStage::Frame,
                source: Box::new(f),
            },
            RecvError::Decode(d) => SourceError::Message {
                stage: MessageStage::Decode,
                source: Box::new(d),
            },
        })
    }
    /// `None` means the buffer is empty right now (poll to refill), not that the stream
    /// has ended.
    fn next_event(&mut self) -> Option<Sourced> {
        self.pending.pop_front()
    }
    fn topic_for(&self, source: &SourceKey) -> Option<&str> {
        self.topics.get(source).map(String::as_str)
    }
    fn reuse_reporting(&self) -> ReuseReporting {
        self.profile.reuse_reporting()
    }
    fn stats(&self) -> crate::source::SourceStats {
        let bridges = || self.bridges.values();
        crate::source::SourceStats {
            messages_seen: self.tracker.messages_seen(),
            dropped: self.tracker.dropped_total(),
            oversized: self.oversized,
            unknown_types: self.unknown_types,
            undecodable: self.undecodable,
            identity: crate::source::IdentityHealth {
                // A run seeded from a parent this run never saw stored, or an entry the
                // bridge's bound dropped: nonzero after a restart against a warm cache, and
                // expected to fall back to zero as the engine's cache turns over.
                unresolved: bridges().map(content::ContentBridge::unresolved).sum(),
                // The observed resident set, under delete-on-evict retention.
                entries: bridges().map(content::ContentBridge::len).sum::<usize>() as u64,
                // Identity promises refused at the declared resident cap; each is also
                // declared in-band, scoped, through `drain_identity_refusals`.
                refused: bridges().map(content::ContentBridge::refused).sum(),
                publisher_restarts: self.publisher_restarts,
                unscoped_clears: self
                    .bridges
                    .values()
                    .map(content::ContentBridge::unscoped_clears)
                    .sum(),
                identity_conflicts: self
                    .bridges
                    .values()
                    .map(content::ContentBridge::conflicts)
                    .sum(),
            },
        }
    }
    fn unknown_fields(&self) -> Vec<String> {
        self.unknown_fields.iter().cloned().collect()
    }
    fn drain_identity_refusals(&mut self) -> Vec<crate::source::IdentityRefusal> {
        // Sources in sorted order, windows already in origin order per source: emission is
        // deterministic, so two runs over one stream declare their losses identically.
        let mut sources: Vec<SourceKey> = self.bridges.keys().cloned().collect();
        sources.sort();
        let mut out = Vec::new();
        for source in sources {
            if let Some(bridge) = self.bridges.get_mut(&source) {
                for (origin, w) in bridge.take_refusals() {
                    out.push(crate::source::IdentityRefusal {
                        source: source.clone(),
                        dp_rank: origin.dp_rank,
                        group_idx: origin.group_idx,
                        window_start_ms: w.first_at_ms,
                        window_end_ms: w.last_at_ms,
                        refused: w.refused,
                    });
                }
            }
        }
        out
    }
    fn seen(&self, source: &SourceKey) -> u64 {
        self.tracker.seen(source)
    }
    fn dropped(&self, source: &SourceKey) -> u64 {
        self.tracker.dropped(source)
    }

    fn restarts(&self, source: &SourceKey) -> u64 {
        Subscriber::restarts(self, source)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use super::Message;
    use super::{FrameTransport, RecvError, Subscriber};
    use crate::clock::{EngineMs, ProducerMs};
    use crate::event_model::{Event, Parent, Removed, Stored};
    use crate::source::vllm::Vllm;
    use crate::source::{EventSource as _, SourceKey, Sourced};
    use crate::testing::QueueTransport as MockTransport;

    fn frame(source: &str, seq: u64, payload_hex: &str) -> Message {
        crate::testing::frame(source, seq, hex::decode(payload_hex).expect("payload hex"))
    }

    // Captured payloads from capture/capture-sha256.jsonl (vLLM 0.26.0).
    // These exercise the framing and adapter together on observed wire bytes.

    /// One `BlockStored`: hash 3795403207894256155, parent 3974460646483626940, GPU, 16
    /// tokens per block, `full_attention`, `dp_rank` 0.
    const STORED: &str = "93cb41da99d221898bd1918ba474797065ab426c6f636b53746f726564ac626c6f\
636b5f68617368657391cf34abfb1bd1ea4a1bb1706172656e745f626c6f636b5f68617368cf37281ee67f6d0bbca\
9746f6b656e5f696473dc00101018111018161012ccc62019ccdc16101811aa626c6f636b5f73697a6510a76c6f72\
615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c0aa65787472615f6b65797391c0a967726f75705\
f69647800b26b765f63616368655f737065635f6b696e64ae66756c6c5f617474656e74696f6e00";

    /// One `BlockRemoved`: hash 4898072202156647938, GPU, `dp_rank` 0. The smallest real
    /// message in the capture, at 76 bytes.
    const REMOVED: &str = "93cb41da99d2230c35cd9184a474797065ac426c6f636b52656d6f766564ac626c\
6f636b5f68617368657391cf43f972b77dc32602a66d656469756da3475055a967726f75705f6964780000";

    fn drain<T: FrameTransport>(sub: &mut Subscriber<T>) -> Vec<Sourced> {
        let mut out = Vec::new();
        loop {
            let n = sub.poll().expect("poll");
            while let Some(ev) = sub.next_event() {
                out.push(ev);
            }
            if n == 0 && sub.stats().messages_seen > 0 {
                break; // drained the canned queue
            }
        }
        out
    }

    /// A sequence regression synthesizes a
    /// `holder_reset` that precedes the regressed message's own events, carries the
    /// producer clock at detection beside the engine-clock boundary, and names the rank
    /// only because every event in the regressed message named it.
    #[test]
    fn a_sequence_regression_synthesizes_a_holder_reset_before_the_regressed_events() {
        let transport = MockTransport::new(vec![
            frame("i0", 5, STORED),
            frame("i0", 1, REMOVED), // 1 < 5: a fresh publisher counting from the start
        ]);
        let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(10));
        sub.now = || ProducerMs(424_242.0);
        let events = drain(&mut sub);

        assert_eq!(
            events.len(),
            3,
            "one store, then the synthesized reset, then the regressed message's event: \
             {events:?}"
        );
        let Event::HolderReset(reset) = &events[1].event else {
            panic!("the reset precedes the regressed event: {events:?}");
        };
        assert_eq!(
            reset.detected_at_ms,
            ProducerMs(424_242.0),
            "producer clock, as injected"
        );
        assert_eq!(
            reset.boundary_ms,
            events[2].event.engine_ms(),
            "the boundary is the regressed message's earliest event"
        );
        assert_eq!(
            reset.dp_rank,
            Some(0),
            "every event in the message named rank 0"
        );
        assert_eq!(
            events[1].seq, 1,
            "the reset rides the regressed message's seq"
        );
        assert_eq!(sub.publisher_restarts, 1);
        assert!(
            sub.pending_resets.is_empty(),
            "the reset is emitted once, not held after its boundary arrived"
        );
    }

    #[test]
    fn pipeline_normalizes_a_message_end_to_end() {
        let transport = MockTransport::new(vec![frame("i0", 1, STORED), frame("i0", 2, REMOVED)]);
        let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(10));
        let events = drain(&mut sub);
        assert_eq!(events.len(), 2);
        // the hash exceeds i64::MAX and must round-trip as an unsigned value
        assert!(matches!(
            events[0].event,
            Event::Stored(Stored {
                block_hash: 3_795_403_207_894_256_155,
                parent: Parent::Hash(3_974_460_646_483_626_940),
                block_size: 16,
                ..
            })
        ));
        assert!(matches!(
            events[1].event,
            Event::Removed(Removed {
                block_hash: 4_898_072_202_156_647_938,
                ..
            })
        ));
        // engine seconds became model milliseconds at the boundary
        assert!(
            events[0].event.engine_ms() > EngineMs(1.0e12),
            "at_ms must be milliseconds"
        );
        assert_eq!(events[0].source, SourceKey::from("i0"));
        assert_eq!(sub.stats().messages_seen, 2);
        assert_eq!(sub.stats().dropped, 0);
        assert_eq!(sub.stats().unknown_types, 0);
    }

    #[test]
    fn an_unmodelled_event_kind_is_skipped_and_counted_not_fatal() {
        // A future engine adds a kind. The tap must keep running and say so, not take the
        // node's telemetry down mid-upgrade.
        // [1.0, [{"type": "SomethingNew"}], 0]
        let future = "93cb3ff00000000000009181a474797065ac536f6d657468696e674e657700";
        let transport = MockTransport::new(vec![frame("i0", 1, future), frame("i0", 2, REMOVED)]);
        let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(10));
        assert_eq!(sub.poll().expect("poll"), 0); // skipped, not an error
        assert_eq!(sub.stats().unknown_types, 1); // and accounted
        assert_eq!(sub.poll().expect("poll"), 1); // the subscription survives
        assert!(matches!(
            sub.next_event(),
            Some(Sourced {
                event: Event::Removed(_),
                ..
            })
        ));
    }

    #[test]
    fn dropped_messages_are_counted_across_a_seq_gap() {
        let transport = MockTransport::new(vec![
            frame("i0", 1, REMOVED),
            frame("i0", 4, REMOVED), // 2 and 3 lost
        ]);
        let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(10));
        drain(&mut sub);
        assert_eq!(sub.stats().messages_seen, 2);
        assert_eq!(sub.stats().dropped, 2);
    }

    #[test]
    fn an_oversized_payload_is_rejected_counted_and_the_stream_survives() {
        let big = vec![0u8; super::MAX_PAYLOAD_BYTES + 1];
        let transport = MockTransport::new(vec![
            Message {
                source: SourceKey::from("i0"),
                parts: vec![b"kv".to_vec(), 1u64.to_be_bytes().to_vec(), big],
            },
            frame("i0", 2, REMOVED), // a good message behind the oversized one
        ]);
        let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(10));
        assert_eq!(sub.poll().expect("poll"), 0); // rejected, not an error
        assert_eq!(sub.stats().oversized, 1);
        let events = drain(&mut sub);
        assert_eq!(events.len(), 1); // the following message still flows
        assert_eq!(sub.stats().messages_seen, 2); // seq observed for both
    }

    #[test]
    fn capacity_refusals_drain_as_scoped_windows() {
        use crate::source::EventSource as _;
        // stored_root's run roots and derives, so with a cap of 1 every later block is
        // refused: the drain yields one scoped window per holder, drains clean, and the
        // cumulative counter feeds the heartbeat.
        let (fx, payload) = crate::testing::wire_fixture("vllm-wire", "stored_root.json");
        let blocks = fx["expect"]["events"].as_array().expect("events").len() as u64;
        let transport = MockTransport::new(vec![crate::testing::frame("i0", 1, payload)]);
        let mut sub =
            Subscriber::new(transport, &Vllm, Duration::from_millis(10)).with_resident_cap(1);
        drain(&mut sub);
        let refusals = sub.drain_identity_refusals();
        assert_eq!(refusals.len(), 1, "one window per affected holder");
        let r = &refusals[0];
        assert_eq!((r.source.as_str(), r.dp_rank, r.group_idx), ("i0", 0, 0));
        assert_eq!(
            r.refused,
            blocks - 1,
            "the first block was promised; the rest refused"
        );
        assert!(r.window_start_ms <= r.window_end_ms);
        assert!(
            r.window_start_ms > EngineMs(1.0e12),
            "engine-clock milliseconds"
        );
        assert!(
            sub.drain_identity_refusals().is_empty(),
            "drained means drained"
        );
        assert_eq!(sub.stats().identity.refused, blocks - 1);
        assert_eq!(sub.stats().identity.entries, 1);
    }

    #[test]
    fn a_malformed_frame_fails_closed() {
        // one-part message: not the 3-part envelope
        let transport = MockTransport::new(vec![Message {
            source: SourceKey::from("i0"),
            parts: vec![vec![0u8; 4]],
        }]);
        let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(10));
        assert!(matches!(sub.poll(), Err(RecvError::Frame(_))));
    }

    #[test]
    fn schema_drift_in_the_payload_fails_closed() {
        // valid frame, but the payload is msgpack for a bare string, not a batch array
        let transport = MockTransport::new(vec![frame("i0", 1, "a568656c6c6f")]); // "hello"
        let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(10));
        assert!(matches!(sub.poll(), Err(RecvError::Decode(_))));
    }
}
