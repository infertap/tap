// SPDX-License-Identifier: Apache-2.0
//! ZMQ multipart framing and per-source sequence tracking.
//!
//! Each message contains `(topic, sequence, payload)`. The sequence is an eight-byte
//! big-endian counter. [`parse_frame`] validates the envelope; [`SeqTracker`] counts
//! gaps between observed messages and detects sequence regressions.
//! These functions do not access sockets.

use std::collections::HashMap;

use crate::source::SourceKey;

/// Byte width of the publisher's big-endian sequence frame.
pub const SEQ_LEN: usize = 8;

/// A multipart message that does not match the pinned 3-part `(topic, seq, payload)` envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// The message has this many parts, not three.
    PartCount(usize),
    /// The sequence frame has this many bytes, not [`SEQ_LEN`].
    SeqWidth(usize),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartCount(n) => write!(
                f,
                "wire frame error: expected a 3-part multipart (topic, seq, payload), got {n} parts"
            ),
            Self::SeqWidth(n) => write!(
                f,
                "wire frame error: seq frame is {n} bytes, expected {SEQ_LEN}"
            ),
        }
    }
}

impl std::error::Error for FrameError {}

/// Validate the 3-part publisher envelope and return `(topic, seq, payload)`.
///
/// Return valid UTF-8 topics unchanged. Invalid UTF-8 topics become `None`.
/// The subscriber accepts all topics; this function does not interpret their names.
///
/// # Errors
/// Returns [`FrameError`] unless there are three parts and the sequence has
/// [`SEQ_LEN`] bytes.
pub fn parse_frame(parts: &[Vec<u8>]) -> Result<(Option<&str>, u64, &[u8]), FrameError> {
    if parts.len() != 3 {
        return Err(FrameError::PartCount(parts.len()));
    }
    let seq: [u8; SEQ_LEN] = parts[1]
        .as_slice()
        .try_into()
        .map_err(|_| FrameError::SeqWidth(parts[1].len()))?;
    Ok((
        std::str::from_utf8(&parts[0]).ok(),
        u64::from_be_bytes(seq),
        &parts[2],
    ))
}

/// Per-source view of the publisher sequence: how many messages have been seen, and how
/// many were dropped, inferred from gaps in the monotonic sequence.
///
/// A gap is counted only on a strict forward jump. The first message from a source, an
/// equal sequence, or a lower sequence after a publisher restart all count as no gap: a
/// reset is not a drop, and inventing drops there would raise false alarms.
#[derive(Debug, Default)]
pub struct SeqTracker {
    sources: HashMap<SourceKey, SourceSeq>,
    seen: u64,
}

/// One source's view: its last sequence, and what the gaps and the messages added up to.
#[derive(Debug, Default, Clone, Copy)]
struct SourceSeq {
    last: Option<u64>,
    dropped: u64,
    seen: u64,
}

impl SeqTracker {
    /// A tracker that has seen nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `seq` for `source` and return the number of messages missed since its previous
    /// seq (0 on the first message, an equal seq, or a reset).
    pub fn observe(&mut self, source: &SourceKey, seq: u64) -> u64 {
        self.seen += 1;
        let s = self.sources.entry(source.clone()).or_default();
        s.seen += 1;
        let gap = match s.last {
            Some(prev) if seq > prev => seq - prev - 1,
            _ => 0,
        };
        s.dropped = s.dropped.saturating_add(gap);
        s.last = Some(seq);
        gap
    }

    /// Return whether `seq` regressed for this source.
    /// A new publisher restarts its counter at zero. The subscriber uses this
    /// signal to invalidate state independently of forward-gap accounting.
    #[must_use]
    pub fn regressed(&self, source: &SourceKey, seq: u64) -> bool {
        self.sources
            .get(source)
            .and_then(|s| s.last)
            .is_some_and(|prev| seq < prev)
    }

    /// Total messages observed across all sources.
    #[must_use]
    pub fn messages_seen(&self) -> u64 {
        self.seen
    }

    /// Total messages dropped across all sources (summed seq gaps).
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.sources
            .values()
            .fold(0, |total, s| total.saturating_add(s.dropped))
    }

    /// Messages dropped for one source.
    #[must_use]
    pub fn dropped(&self, source: &SourceKey) -> u64 {
        self.sources.get(source).map_or(0, |s| s.dropped)
    }

    /// Messages observed for one source (feeds per-endpoint heartbeat stats).
    #[must_use]
    pub fn seen(&self, source: &SourceKey) -> u64 {
        self.sources.get(source).map_or(0, |s| s.seen)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{parse_frame, FrameError, SeqTracker};
    use crate::source::SourceKey;
    use std::sync::LazyLock;

    #[test]
    fn aggregate_gaps_saturate_across_sources() {
        let mut tracker = SeqTracker::new();
        for name in ["a", "b"] {
            let source = SourceKey::from(name);
            tracker.observe(&source, 0);
            tracker.observe(&source, u64::MAX);
        }
        assert_eq!(tracker.dropped_total(), u64::MAX);
    }

    static A: LazyLock<SourceKey> = LazyLock::new(|| SourceKey::from("a"));
    static B: LazyLock<SourceKey> = LazyLock::new(|| SourceKey::from("b"));

    fn parts(topic: &[u8], seq: u64, payload: &[u8]) -> Vec<Vec<u8>> {
        vec![topic.to_vec(), seq.to_be_bytes().to_vec(), payload.to_vec()]
    }

    #[test]
    fn parses_a_well_formed_frame() {
        let msg = parts(b"kv", 258, b"\x93payload");
        let (_topic, seq, payload) = parse_frame(&msg).expect("frame");
        assert_eq!(seq, 258);
        assert_eq!(payload, b"\x93payload");
    }

    #[test]
    fn wrong_part_count_is_an_error() {
        assert!(parse_frame(&[vec![0], vec![0; 8]]).is_err()); // 2 parts
        assert!(parse_frame(&[vec![0], vec![0; 8], vec![1], vec![2]]).is_err());
        // 4 parts
    }

    #[test]
    fn wrong_seq_width_is_an_error() {
        let bad = vec![b"kv".to_vec(), vec![0, 0, 1], b"payload".to_vec()]; // 3-byte seq
        let err = parse_frame(&bad).expect_err("short seq must fail");
        assert_eq!(err, FrameError::SeqWidth(3));
    }

    #[test]
    fn in_order_sequence_has_no_gaps() {
        let mut t = SeqTracker::new();
        assert_eq!(t.observe(&A, 1), 0); // first message
        assert_eq!(t.observe(&A, 2), 0);
        assert_eq!(t.observe(&A, 3), 0);
        assert_eq!(t.dropped_total(), 0);
        assert_eq!(t.messages_seen(), 3);
    }

    #[test]
    fn a_forward_jump_counts_dropped_messages() {
        let mut t = SeqTracker::new();
        t.observe(&A, 5);
        assert_eq!(t.observe(&A, 8), 2); // 6 and 7 were lost
        assert_eq!(t.dropped(&A), 2);
        assert_eq!(t.dropped_total(), 2);
    }

    #[test]
    fn a_reset_or_replay_is_not_a_drop() {
        let mut t = SeqTracker::new();
        t.observe(&A, 10);
        assert_eq!(t.observe(&A, 10), 0); // duplicate
        assert_eq!(t.observe(&A, 3), 0); // publisher restarted, counter reset
        assert_eq!(t.dropped_total(), 0);
    }

    #[test]
    fn sources_are_tracked_independently() {
        let mut t = SeqTracker::new();
        t.observe(&A, 1);
        t.observe(&B, 100);
        assert_eq!(t.observe(&A, 3), 1); // a lost one
        assert_eq!(t.observe(&B, 101), 0); // b is in order
        assert_eq!(t.dropped(&A), 1);
        assert_eq!(t.dropped(&B), 0);
    }
}
