// SPDX-License-Identifier: Apache-2.0
//! Replay a captured prefix chain with a simulated message gap and publisher restart.
//! Use the production adapter, transducer, pseudonymizer, and record guard. An in-memory
//! transport and sink make the demonstration work without a GPU, sockets, or output files.
//!
//! ```sh
//! cargo run --locked --quiet --example replay_corpus
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)] // a test that cannot fail loudly is not a test

use std::time::Duration;

use infertap::compute::Transducer;
use infertap::pseudonym::{Key, BLOCK_ID_CONTEXT};
use infertap::record::InstanceLabel;
use infertap::sink::{GuardedSink, MemorySink, PseudonymConfig, PseudonymizingSink};
use infertap::source::vllm::Vllm;
use infertap::source::wire::Subscriber;
use infertap::source::{EventSource, SourceKey, Sourced};
use infertap::testing::{frame, wire_fixture, QueueTransport};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Historical captured bytes, independent of the current engine support baseline.
    // Reusing this rooted run lets the demo check identity continuity after a reset.
    let (fixture, payload) = wire_fixture("vllm-wire", "stored_root.json");
    let expected = fixture["expect"]["events"]
        .as_array()
        .expect("fixture events");

    // The payload is captured; the sequence numbers and their faults are constructed.
    let transport = QueueTransport::new(vec![
        frame("engine-a", 10, payload.clone()),
        frame("engine-a", 12, payload.clone()),
        frame("engine-a", 1, payload),
    ]);
    let mut source = Subscriber::new(transport, &Vllm, Duration::from_millis(1));

    // Public test material, never a key for live collection.
    let key = Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")?;
    let mut transducer = Transducer::new();
    let mut sink = PseudonymizingSink::new(
        PseudonymConfig { key, epoch: 0 },
        BLOCK_ID_CONTEXT,
        GuardedSink::new(MemorySink::new(None)),
    );
    loop {
        let buffered = EventSource::poll(&mut source)?;
        while let Some(Sourced { source, event, seq }) = source.next_event() {
            transducer.ingest(&InstanceLabel::from(&source), &event, Some(seq), &mut sink);
        }
        if buffered == 0 {
            break;
        }
    }

    let records = sink.into_inner().into_inner();
    let stats = EventSource::stats(&source);
    let restarts = source.restarts(&SourceKey::from("engine-a"));
    let stores: Vec<_> = records
        .records
        .iter()
        .filter(|r| r["kind"] == "store")
        .collect();
    let first = stores[0];
    let child = stores[1];
    let after_restart = stores[expected.len() * 2];

    // Expectations come from the input scenario and the independent fixture contract.
    // Fail the command if a future change stops demonstrating the stated behavior.
    assert_eq!(stats.messages_seen, 3);
    assert_eq!(stats.dropped, 1);
    assert_eq!(restarts, 1);
    assert_eq!(stores.len(), expected.len() * 3);
    assert_eq!(records.counts["holder_reset"], 1);
    assert_eq!(child["parent_id"], first["block_id"]);
    assert_eq!(after_restart["block_id"], first["block_id"]);
    assert!(first["content_id"].is_string());
    assert_eq!(after_restart["content_id"], first["content_id"]);
    for (store, raw) in stores.iter().zip(expected.iter().cycle()) {
        assert_ne!(store["block_id"], raw["block_hash"].to_string());
        assert_ne!(store["content_id"], raw["content_id"]);
        assert!(store.get("token_ids").is_none());
    }

    println!("Replay a captured vLLM prefix chain (no GPU or network)");
    println!("Input: the same captured payload at simulated sequences 10, 12, 1");
    println!("Messages received: {}", stats.messages_seen);
    println!(
        "Missing messages between observed sequences: {}",
        stats.dropped
    );
    println!("Publisher resets: {restarts}");
    println!(
        "Output: {} stores, {} holder reset",
        stores.len(),
        records.counts["holder_reset"]
    );
    println!(
        "Checks passed: pseudonymized identities, parent links, identity continuity after reset"
    );
    println!(
        "\nFirst store (public test key):\n{}",
        serde_json::to_string_pretty(first)?
    );
    println!(
        "\nThis demo uses in-memory transport and output. Live collection writes Parquet segments."
    );
    Ok(())
}
