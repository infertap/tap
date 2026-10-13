// SPDX-License-Identifier: Apache-2.0
//! Failure handling and record-stream consistency.
//!
//! Cover publisher restarts, sequence gaps, truncated writes, and resource limits.
//! Check collection behavior and the records that describe each failure.

#![allow(clippy::unwrap_used, clippy::expect_used)] // a test that cannot fail loudly is not a test

use std::collections::VecDeque;
use std::time::Duration;

use infertap::clock::EngineMs;
use infertap::compute::Transducer;
use infertap::event_model::{Event, Origin, Parent, Removed, Stored};
use infertap::record::InstanceLabel;
use infertap::sink::{GuardedSink, JsonlSink, MemorySink, Sink};
use infertap::source::vllm::Vllm;
use infertap::source::wire::{FrameTransport, Subscriber};
use infertap::source::{EventSource as _, SourceKey};
use std::sync::LazyLock;

static I0: LazyLock<InstanceLabel> = LazyLock::new(|| InstanceLabel::from("i0"));
use infertap::testing::{frame, wire_fixture, QueueTransport};
use serde_json::Value;

/// A real captured multi-event payload, from the wire corpus, so the pipeline runs on bytes
/// that came off a socket rather than on anything we constructed.
fn corpus_fixture() -> (Vec<u8>, usize) {
    let (fx, bytes) = wire_fixture("vllm-wire", "multi_event_batch.json");
    // events-per-message comes from the corpus, never a literal: the capture decides it
    let n = fx["expect"]["events"].as_array().expect("events").len();
    (bytes, n)
}

fn corpus_payload() -> Vec<u8> {
    corpus_fixture().0
}

fn drain<T: FrameTransport>(sub: &mut Subscriber<T>) -> usize {
    let mut n = 0;
    loop {
        let got = sub.poll().expect("poll");
        while sub.next_event().is_some() {
            n += 1;
        }
        if got == 0 {
            return n;
        }
    }
}

/// Engine restart or reconnect: the publisher's sequence counter resets. The tap counts no
/// false drops, since a reset is not a drop and no drop window exists, and it makes the
/// reset visible: the subscriber synthesizes one `holder_reset` ahead of the regressed
/// message's own events (contract section 2.3), so a consumer's residency model learns the
/// cache it was tracking is gone instead of folding one residency across the outage.
#[test]
fn publisher_restart_resets_seq_without_false_drops() {
    let payload = corpus_payload();
    let transport = QueueTransport(VecDeque::from(vec![
        frame("i0", 100, payload.clone()),
        frame("i0", 101, payload.clone()),
        frame("i0", 1, payload.clone()), // restart: counter reset
        frame("i0", 2, payload),
    ]));
    let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(5));
    let events = drain(&mut sub);
    // none lost, none double-counted, plus the one synthesized holder_reset
    assert_eq!(events, 4 * corpus_fixture().1 + 1);
    assert_eq!(sub.stats().messages_seen, 4);
    assert_eq!(sub.stats().dropped, 0); // a reset is not a drop
    assert_eq!(sub.restarts(&SourceKey::from("i0")), 1); // counted, and scoped to its source
}

/// Sustained burst with real losses: thousands of messages with interior gaps. Every
/// interior drop is counted exactly; the consumer's drop windows are the sequence gaps. A
/// loss at the end of the window is undetectable by gap analysis, because nothing after it
/// exposes the gap, which is why the dropped count is documented as exact for interior
/// losses and a lower bound overall. The test leaves one trailing loss out of the
/// expectation to pin that.
#[test]
fn sustained_burst_counts_every_interior_drop_exactly() {
    let payload = corpus_payload();
    let mut q = VecDeque::new();
    let mut sent = 0u64;
    let mut interior_skips = 0u64;
    for seq in 1..=10_000u64 {
        if seq % 100 == 0 {
            if seq != 10_000 {
                interior_skips += 1; // lost in flight: the SUB fell behind
            }
            // seq 10_000 is also lost, but it is the tail loss: invisible to the tracker
        } else {
            q.push_back(frame("i0", seq, payload.clone()));
            sent += 1;
        }
    }
    let mut sub = Subscriber::new(QueueTransport(q), &Vllm, Duration::from_millis(5));
    let events = drain(&mut sub);
    assert_eq!(events as u64, sent * corpus_fixture().1 as u64);
    assert_eq!(sub.stats().messages_seen, sent);
    // exact for the 99 interior gaps; the tail loss is structurally undetectable
    assert_eq!(sub.stats().dropped, interior_skips);
}

/// Truncate sample JSONL output at each byte offset. Every complete line must
/// parse as a record; only the final line may be incomplete.
#[test]
fn abrupt_termination_leaves_only_a_truncated_tail() {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut transducer = Transducer::new();
        let mut sink = GuardedSink::new(JsonlSink::new(&mut buf));
        // real-shaped hashes: above i64::MAX, which is ordinary traffic
        let base = 17_843_516_567_353_125_252_u64;
        for i in 0..40u64 {
            let at_ms = 1_785_153_670_000.0 + f64::from(u32::try_from(i).unwrap());
            let stored = Event::Stored(Stored {
                origin: Origin::default(),
                block_hash: base + i,
                parent: if i > 0 {
                    Parent::Hash(base + i - 1)
                } else {
                    Parent::Root
                },
                block_size: 16,
                tier: Some("GPU".into()),
                extra_keys: None,
                lora_id: None,
                lora_name: None,
                spec_kind: Some("full_attention".into()),
                reused: None,
                spec_sliding_window: None,
                locality: None,
                backend_id: None,
                content_id: None,
                at_ms: EngineMs(at_ms),
            });
            transducer.ingest(&I0, &stored, None, &mut sink);
            if i % 7 == 3 {
                let removed = Event::Removed(Removed {
                    origin: Origin::default(),
                    block_hash: base + i,
                    tier: Some("GPU".into()),
                    locality: None,
                    backend_id: None,
                    content_id: None,
                    at_ms: EngineMs(at_ms + 0.5),
                });
                transducer.ingest(&I0, &removed, None, &mut sink);
            }
        }
        sink.flush().expect("flush");
    }
    assert!(buf.len() > 1000);
    for cut in 0..buf.len() {
        let prefix = &buf[..cut];
        // every COMPLETE line in the prefix must be a valid record object
        let mut rest = prefix;
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            let line = &rest[..nl];
            let v: Value = serde_json::from_slice(line).expect("complete line parses");
            assert!(v.is_object());
            assert!(v.get("kind").is_some());
            rest = &rest[nl + 1..];
        }
        // `rest` is the (possibly empty) truncated tail: discarded by the consumer, by rule
    }
}

/// Each normalized event produces one record without retained compute residency state.
/// Exercise a large stream to check counts and bounded transducer state.
#[test]
fn a_pathological_stream_is_one_record_per_event_and_no_state() {
    const N: u64 = 50_000;
    let mut transducer = Transducer::new();
    let mut sink = MemorySink::new(Some(10)); // bounded sample; counts stay exact
    let base = 17_843_516_567_353_125_252_u64;
    for i in 0..N {
        transducer.ingest(
            &I0,
            &Event::Stored(Stored {
                origin: Origin::default(),
                block_hash: base + i,
                parent: Parent::Root,
                block_size: 16,
                tier: Some("GPU".into()),
                extra_keys: None,
                lora_id: None,
                lora_name: None,
                spec_kind: None,
                reused: None,
                spec_sliding_window: None,
                locality: None,
                backend_id: None,
                content_id: None,
                at_ms: EngineMs(1_785_153_670_000.0),
            }),
            None,
            &mut sink,
        );
    }
    for i in 0..N {
        transducer.ingest(
            &I0,
            &Event::Removed(Removed {
                origin: Origin::default(),
                block_hash: base + i,
                tier: Some("GPU".into()),
                locality: None,
                backend_id: None,
                content_id: None,
                at_ms: EngineMs(1_785_153_671_000.0),
            }),
            None,
            &mut sink,
        );
    }
    // one record per event, no coalescing, no synthesis, no cap to elide anything
    assert_eq!(sink.counts["store"], N);
    assert_eq!(sink.counts["evict"], N);
    assert_eq!(transducer.events_ingested(), N * 2);
    // and the sample stayed bounded: the SINK bounds memory now, not the agent
    assert_eq!(sink.records.len(), 10);
}

/// Malformed messages are counted and skipped. The next successfully decoded
/// message exposes the missing sequence through the normal gap accounting.
#[test]
fn an_undecodable_message_is_skipped_counted_and_declared_as_a_gap() {
    use infertap::source::{EventSource, SourceError};
    let payload = corpus_payload();
    let garbage = vec![0xc1_u8; 8]; // 0xc1 is msgpack's one reserved, never-valid byte
    let transport = QueueTransport(VecDeque::from(vec![
        frame("i0", 10, payload.clone()),
        frame("i0", 11, garbage),
        frame("i0", 12, payload),
    ]));
    let mut sub = Subscriber::new(transport, &Vllm, Duration::from_millis(5));
    assert!(sub.poll().is_ok(), "seq 10 decodes");
    let err = EventSource::poll(&mut sub).expect_err("seq 11 is garbage");
    assert!(
        matches!(err, SourceError::Message { .. }) && !err.is_fatal(),
        "a bad message is skippable, not fatal: {err}"
    );
    assert_eq!(sub.stats().undecodable, 1);
    assert_eq!(
        sub.stats().dropped,
        0,
        "nothing lost yet: seq 11 was simply not observed"
    );
    assert!(sub.poll().is_ok(), "seq 12 decodes");
    assert_eq!(
        sub.stats().dropped,
        1,
        "seq 12 reveals the gap where the skipped message was: declared as a drop"
    );
    assert_eq!(sub.stats().messages_seen, 2);
}
