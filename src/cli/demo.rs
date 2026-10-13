// SPDX-License-Identifier: Apache-2.0
//! `show-payload`: the built-in scenario run through the full egress chain, so the
//! emission guarantees can be checked with no engine. The diff test byte-compares it
//! against the live chain.

use infertap::clock::{EngineMs, ProducerMs};
use infertap::compute::Transducer;
use infertap::event_model::{Cleared, Event, Origin, Parent, Removed, Stored};
use infertap::lifecycle;
use infertap::pseudonym::Key;
use infertap::pseudonym::BLOCK_ID_CONTEXT;
use infertap::record::{Egress, InstanceLabel, ReuseReporting, StopReason};
use infertap::sink::GuardedSink;
use infertap::sink::JsonlSink;
use infertap::sink::PseudonymConfig;
use infertap::sink::PseudonymizingSink;
use infertap::sink::Sink as _;
use infertap::source::MAX_PAYLOAD_BYTES;
use infertap::source::{SourceKey, Sourced};
use std::process::ExitCode;

/// The built-in scenario: two instances whose events exercise every record kind in the
/// classification-neutral stream (store, evict, and clear), including a cross-instance store
/// and a re-store after eviction, so `show-payload` shows the stream an aggregator joins on.
pub(crate) fn demo_events() -> Vec<Sourced> {
    let at = |source: &str, event: Event, seq: u64| Sourced {
        source: SourceKey::from(source),
        event,
        seq,
    };
    let stored = |h: u64, at_ms: f64| {
        Event::Stored(Stored {
            origin: Origin::default(),
            block_hash: h,
            parent: Parent::Root,
            block_size: 4,
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
        })
    };
    vec![
        at("i0", stored(1, 1.0), 1),
        at("i0", stored(2, 1.1), 1),
        // chained child of block 1: its parent pointer is pseudonymized too; the edge survives
        at(
            "i0",
            Event::Stored(Stored {
                origin: Origin::default(),
                block_hash: 7,
                parent: Parent::Hash(1),
                block_size: 4,
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
                at_ms: EngineMs(1.2),
            }),
            1,
        ),
        at(
            "i1",
            Event::Stored(Stored {
                // dp_rank 1: a second data-parallel worker of the same engine, in its own
                // EngineCore process publishing on its own port at base plus global rank,
                // never a shared endpoint. Its cache is physically independent, so the same
                // hash here is real duplication.
                origin: Origin {
                    dp_rank: 1,
                    group_idx: 0,
                },
                block_hash: 42,
                parent: Parent::Root,
                block_size: 4,
                tier: Some("GPU".into()),
                // cache_salt as the engine reports it: operator-controlled text naming a
                // tenant, pseudonymized at egress like a block id.
                extra_keys: Some(vec!["tenant-example".into()]),
                lora_id: None,
                lora_name: None,
                spec_kind: Some("full_attention".into()),
                reused: None,
                spec_sliding_window: None,
                locality: None,
                backend_id: None,
                content_id: None,
                at_ms: EngineMs(2.5),
            }),
            2,
        ),
        // block 1 also on i0: the aggregator's residual join sees it
        at("i1", stored(1, 2.0), 2),
        // -> evict record
        at(
            "i0",
            Event::Removed(Removed {
                origin: Origin::default(),
                block_hash: 2,
                tier: Some("GPU".into()),
                locality: None,
                backend_id: None,
                content_id: None,
                at_ms: EngineMs(3.0),
            }),
            3,
        ),
        // re-store after evict: the aggregator's recompute join sees it
        at("i0", stored(2, 4.0), 3),
        // -> one scope-level clear record, the event as said
        at(
            "i0",
            Event::Cleared(Cleared {
                dp_rank: Some(0),
                group_idx: Some(0),
                tier: Some("GPU".into()),
                at_ms: EngineMs(5.0),
            }),
            4,
        ),
    ]
}

pub(crate) fn demo_pseudonym_config() -> PseudonymConfig {
    #[allow(clippy::expect_used)]
    // a constant the conformance vectors pin; a refusal is a build defect
    let key = Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
        .expect("test key");
    PseudonymConfig { key, epoch: 0 }
}

/// Run the sample through pseudonymization, the output guard, and JSONL encoding.
/// Return the record count and the number of values scanned for raw identifiers.
/// `show-payload` uses this pipeline; a test compares it with subscription output.
pub(crate) fn demo_chain<W: std::io::Write>(w: W) -> (u64, u64) {
    let mut transducer = Transducer::new();
    let mut sink = PseudonymizingSink::new(
        demo_pseudonym_config(),
        BLOCK_ID_CONTEXT,
        GuardedSink::new(JsonlSink::new(w)),
    );
    // The lifecycle bracket rides the same chain as every record: start, the cache events,
    // one heartbeat, and the stop record. Deterministic on purpose (fixed at_ms, rss
    // omitted): the diff-test byte-compares this output.
    sink.emit(lifecycle::start_record(
        env!("CARGO_PKG_VERSION"),
        &demo_start_config(),
        ProducerMs(0.5),
    ));
    for Sourced { source, event, seq } in demo_events() {
        transducer.ingest(&InstanceLabel::from(&source), &event, Some(seq), &mut sink);
    }
    let stats = demo_stats(&transducer, sink.scanned_total());
    let key = demo_key_identity();
    sink.emit(lifecycle::heartbeat_record(
        &stats,
        Some(&key),
        ProducerMs(5.5),
    ));
    sink.emit(lifecycle::stop_record(
        StopReason::Duration,
        &stats,
        Some(&key),
        ProducerMs(6.0),
    ));
    let scanned = sink.scanned_total();
    let written = sink.into_inner().into_inner().written();
    (written, scanned)
}

/// Derive the sample canary with the same construction used by live collection.
/// Include it in start, heartbeat, and stop records.
pub(crate) fn demo_key_identity() -> lifecycle::KeyIdentity {
    lifecycle::KeyIdentity {
        canary: infertap::pseudonym::canary(&demo_pseudonym_config().key, 0),
    }
}

pub(crate) fn demo_start_config() -> lifecycle::StartConfig {
    lifecycle::StartConfig {
        // Match the default engine's live declaration: store events include cache
        // reuse, so consumers cannot count them as new insertions.
        reuse_reporting: ReuseReporting::Unlabelled,
        endpoints: 2,
        max_payload_bytes: MAX_PAYLOAD_BYTES,
        egress: Egress::Pseudonymized,
        key_identity: Some(demo_key_identity()),
    }
}

/// Deterministic demo stats: subscriber counters zero (no transport in the demo), agent
/// counters real, rss omitted on purpose so the output is byte-stable.
pub(crate) fn demo_stats(transducer: &Transducer, scanned: u64) -> lifecycle::EnvelopeStats {
    lifecycle::EnvelopeStats {
        reuse_reporting: ReuseReporting::Unlabelled,
        msgs_seen: 0,
        dropped: 0,
        oversized: 0,
        noraw_scanned: Some(scanned),
        unknown_types: 0,
        events_ingested: transducer.events_ingested(),
        content_unresolved: 0,
        content_bridge_entries: 0,
        content_bridge_refused: 0,
        publisher_restarts: 0,
        rss_bytes: None,
        endpoints: vec![],
    }
}

pub(crate) fn show_payload() -> ExitCode {
    // Narration goes to stderr; stdout contains only the demo records.
    log::info!("INGRESS: a normalized event, as the agent holds it");
    log::info!("{:?}", demo_events()[0].event);
    log::info!("the adapter drops token ids after deriving content identity");

    log::info!("EGRESS: every record the tap would emit, post-decorator, as JSONL on stdout");
    log::info!("pseudonymized under the labeled test-only conformance key, epoch 0");
    // Use the same pseudonymization and size checks as the live output path.
    let (written, scanned) = demo_chain(std::io::stdout().lock());
    log::info!(
        "{written} records: the lifecycle bracket (agent_start / heartbeat / agent_stop) \
         plus one per cache event (store/evict/clear), each checked twice"
    );
    log::info!("the no-raw-id guard scanned {scanned} values: no unexpected raw-value matches detected (scanner exclusions apply)");
    log::info!("every demo record passed the serialized size limit");
    log::info!("present: keyed pseudonyms and epoch for block and parent ids, so prefix-chain edges survive; token counts, tiers, timestamps, provenance");
    log::info!("absent from this demo output: token ids, KV tensors, and raw block ids");
    ExitCode::SUCCESS
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {

    // The show-payload completeness diff-test, default-build half: the shown output is
    // deterministic and its pseudonyms are the pinned conformance vectors. The live-chain
    // byte-comparison is in `egress_diff`.
    #[test]
    fn shown_payload_is_deterministic_and_pinned_to_the_vectors() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        let (wa, sa) = super::demo_chain(&mut a);
        let (wb, sb) = super::demo_chain(&mut b);
        assert_eq!(a, b); // byte-identical across runs
        assert_eq!((wa, sa), (wb, sb));
        assert_eq!(wa, 11); // lifecycle bracket (3) + 6 stores + 1 evict + 1 scope-level clear
        assert!(sa > 0);
        let mut lines = a.split(|&c| c == b'\n');
        let first: serde_json::Value =
            serde_json::from_slice(lines.next().expect("a line")).expect("json line");
        // the bracket opens the stream; lifecycle records carry no ids and no epoch stamp
        assert_eq!(first["kind"], "agent_start");
        assert!(first.get("epoch").is_none());
        let second: serde_json::Value =
            serde_json::from_slice(lines.next().expect("a line")).expect("json line");
        // raw 1 under the conformance test key, epoch 0: the pinned vector
        assert_eq!(second["block_id"], "db83955d1a485fe98364fd9e47c8bc08");
        assert_eq!(second["epoch"], 0);
    }
}

// Compare sample and live sink output byte-for-byte for identical input.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod egress_diff {
    use infertap::clock::ProducerMs;
    use infertap::compute::Transducer;
    use infertap::pseudonym::BLOCK_ID_CONTEXT;
    use infertap::record::InstanceLabel;
    use infertap::sink::{GuardedSink, JsonlSink, PseudonymizingSink};
    use infertap::source::Sourced;

    #[test]
    fn shown_payload_byte_equals_the_live_chain_output() {
        use infertap::lifecycle;
        use infertap::sink::Sink as _;

        let mut shown = Vec::new();
        super::demo_chain(&mut shown);

        let mut live = Vec::new();
        {
            let mut transducer = Transducer::new();
            let guarded = GuardedSink::new(JsonlSink::new(&mut live));
            let mut sink = crate::cli::output::EgressSink::Pseudonymized(PseudonymizingSink::new(
                super::demo_pseudonym_config(),
                BLOCK_ID_CONTEXT,
                guarded,
            ));
            // the identical bracket, through the live wrapper
            sink.emit(lifecycle::start_record(
                env!("CARGO_PKG_VERSION"),
                &super::demo_start_config(),
                ProducerMs(0.5),
            ));
            for Sourced { source, event, seq } in super::demo_events() {
                transducer.ingest(&InstanceLabel::from(&source), &event, Some(seq), &mut sink);
            }
            let scanned = match &sink {
                crate::cli::output::EgressSink::Pseudonymized(p) => p.scanned_total(),
                crate::cli::output::EgressSink::Raw(_) => unreachable!(),
            };
            let stats = super::demo_stats(&transducer, scanned);
            let key = super::demo_key_identity();
            sink.emit(lifecycle::heartbeat_record(
                &stats,
                Some(&key),
                ProducerMs(5.5),
            ));
            sink.emit(lifecycle::stop_record(
                infertap::record::StopReason::Duration,
                &stats,
                Some(&key),
                ProducerMs(6.0),
            ));
        }
        for line in std::str::from_utf8(&live).expect("utf8").lines() {
            let value = serde_json::from_str(line).expect("record JSON");
            inference_events::Record::decode(value).expect("live egress conforms to shared model");
        }
        assert_eq!(shown, live);
    }
}
