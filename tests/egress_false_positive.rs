// SPDX-License-Identifier: Apache-2.0
//! Exercise the production egress chain with raw IDs and strings that also occur in
//! unmapped fields. Legitimate coincidences must not abort emission; unexpected new
//! occurrences remain failures.

#![allow(clippy::unwrap_used, clippy::expect_used)] // a test that cannot fail loudly is not a test

use infertap::clock::EngineMs;
use infertap::compute::Transducer;
use infertap::event_model::{Event, Origin, Parent, Stored};
use infertap::pseudonym::{Key, BLOCK_ID_CONTEXT};
use infertap::record::InstanceLabel;
use infertap::sink::{GuardedSink, MemorySink, PseudonymConfig, PseudonymizingSink};

fn chain() -> PseudonymizingSink<GuardedSink<MemorySink, infertap::record::Pseudonymized>> {
    let key = Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
        .expect("test key");
    PseudonymizingSink::new(
        PseudonymConfig { key, epoch: 0 },
        BLOCK_ID_CONTEXT,
        GuardedSink::new(MemorySink::new(None)),
    )
}

fn store(block_hash: u64, origin: Origin) -> Event {
    Event::Stored(Stored {
        origin,
        block_hash,
        parent: Parent::Root,
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
        at_ms: EngineMs(1_785_153_670_000.0),
    })
}

/// Small synthetic IDs can equal counters such as epoch, rank, and token count.
/// These coincidences must not trigger the raw-value guard.
#[test]
fn synthetic_sequential_ids_do_not_abort_the_chain() {
    let mut sink = chain();
    let mut transducer = Transducer::new();
    for h in 0..64u64 {
        transducer.ingest(
            &InstanceLabel::from("i0"),
            &store(h, Origin::default()),
            None,
            &mut sink,
        );
    }
    // and with non-zero scope values, so dp_rank/group_idx take other small numbers too
    for h in 0..64u64 {
        let origin = Origin {
            dp_rank: 3,
            group_idx: 1,
        };
        transducer.ingest(
            &InstanceLabel::from("i1"),
            &store(h, origin),
            None,
            &mut sink,
        );
    }
    assert_eq!(sink.into_inner().into_inner().records.len(), 128);
}

/// A salt may equal public record vocabulary. Preexisting occurrences outside
/// the mapped field must not trigger the raw-value guard.
#[test]
fn a_salt_equal_to_record_vocabulary_does_not_abort_the_chain() {
    use infertap::sink::Sink as _;
    for salt in ["store", "GPU", "full_attention", "i0"] {
        let mut sink = chain();
        let mut transducer = Transducer::new();
        let mut event = store(12_955_354_968_414_679_641, Origin::default());
        if let Event::Stored(stored) = &mut event {
            stored.extra_keys = Some(vec![salt.into()]);
        }
        transducer.ingest(&InstanceLabel::from("i0"), &event, None, &mut sink);
        sink.flush().expect("flush");
        let records = sink.into_inner().into_inner().records;
        // and the coincidence bought no weakening: the salt itself still left pseudonymized
        let emitted = records[0]["extra_keys"][0].as_str().expect("mapped");
        assert_eq!(emitted.len(), 32, "salt {salt:?} left the host unmapped");
        assert_ne!(emitted, salt);
    }
}

/// The narrowing must not have bought that pass by weakening the guard. A realistic
/// fingerprint surviving into the output is still an abort.
/// Every `_id` field the transducer emits leaves the chain as a pseudonym or is exempt by
/// name with a reason. The classification is the type: an identity is an `Id`, mapped by
/// the pseudonymizing sink, or it is not an identity. This test reads the emitted JSON so
/// a field that is neither would show up as a decimal in the clear.
#[test]
fn every_id_field_the_transducer_emits_leaves_as_a_pseudonym_or_is_exempt() {
    /// Fields ending in `_id` that carry no content fingerprint, with the reason.
    const EXEMPT: &[&str] = &[
        "instance_id", // the tap's own label for the engine
        "lora_id",     // the engine's small integer adapter index, not a hash
    ];
    let mut sink = chain();
    let mut transducer = Transducer::new();
    transducer.ingest(
        &InstanceLabel::from("i0"),
        &Event::Stored(Stored {
            origin: Origin {
                dp_rank: 1,
                group_idx: 2,
            },
            block_hash: 12_955_354_968_414_679_641,
            parent: Parent::Hash(10_841_253_731_892_115_301),
            block_size: 16,
            tier: Some("GPU".into()),
            extra_keys: Some(vec!["tenant-AAAA".into()]),
            lora_id: Some(7),
            lora_name: Some("customer-adapter".into()),
            spec_kind: Some("full_attention".into()),
            reused: None,
            spec_sliding_window: None,
            locality: Some("LOCAL".into()),
            backend_id: None,
            content_id: Some(16_032_592_773_066_255_487),
            at_ms: EngineMs(1_785_153_670_000.0),
        }),
        None,
        &mut sink,
    );
    let records = sink.into_inner().into_inner().records;
    let obj = records
        .first()
        .expect("one record")
        .as_object()
        .expect("object");
    for (k, v) in obj {
        if !k.ends_with("_id") || EXEMPT.contains(&k.as_str()) {
            continue;
        }
        assert!(
            v.as_str().is_some_and(|s| s.len() == 32),
            "{k} left the chain as {v}, neither a pseudonym nor exempt"
        );
    }
}
