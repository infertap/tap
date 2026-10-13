// SPDX-License-Identifier: Apache-2.0
//! The lifecycle builders reproduce `conformance/lifecycle/records.json` byte-for-byte, and
//! lifecycle records behave correctly through the full egress chain: no ids, so the
//! pseudonymizer maps nothing and stamps no epoch, the no-raw-id scan finds nothing
//! scannable to flag, and the metadata guard passes them. A graceful stream ends with the
//! `agent_stop` record as its last complete line.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

use infertap::clock::{EngineMs, ProducerMs};
use infertap::lifecycle::{
    heartbeat_record, identity_refused_record, start_record, stop_record, EndpointStats,
    EnvelopeStats, KeyIdentity, StartConfig,
};
use infertap::pseudonym::{self, Key, BLOCK_ID_CONTEXT};
use infertap::record::InstanceLabel;
use infertap::record::Record;
use infertap::sink::{
    GuardedSink, JsonlSink, MemorySink, PseudonymConfig, PseudonymizingSink, Sink,
};
use infertap::source::{IdentityRefusal, SourceKey};
use serde_json::Value;

/// One corpus case, built through the shipped builders: the canary is computed from the
/// key rather than read out of the fixture, so the two canary implementations meet.
fn build_case(case: &Value, version: &str, key_epoch: u32) -> Record {
    let at_ms = ProducerMs(case["at_ms"].as_f64().expect("at_ms"));
    let expect = case.get("expect").unwrap_or(case);
    let key = expect.get("canary").is_some().then(|| KeyIdentity {
        canary: pseudonym::canary(&conformance_key(), key_epoch),
    });
    match case["kind"].as_str().expect("kind") {
        "agent_start" => {
            let cfg = &case["config"];
            start_record(
                version,
                &StartConfig {
                    reuse_reporting: serde_json::from_value(cfg["reuse_reporting"].clone())
                        .expect("a pinned reuse_reporting"),
                    endpoints: usize::try_from(cfg["endpoint_count"].as_u64().unwrap()).unwrap(),
                    max_payload_bytes: usize::try_from(cfg["max_payload_bytes"].as_u64().unwrap())
                        .unwrap(),
                    egress: serde_json::from_value(cfg["egress"].clone()).expect("a pinned egress"),
                    key_identity: key.clone(),
                },
                at_ms,
            )
        }
        "heartbeat" => heartbeat_record(&stats_from(&case["stats"]), key.as_ref(), at_ms),
        "agent_stop" => stop_record(
            serde_json::from_value(case["reason"].clone()).expect("a pinned stop reason"),
            &stats_from(&case["stats"]),
            key.as_ref(),
            at_ms,
        ),
        other => panic!("unknown corpus kind {other:?}"),
    }
}

fn corpus() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/lifecycle/records.json");
    serde_json::from_str(&fs::read_to_string(path).expect("read")).expect("json")
}

fn stats_from(v: &Value) -> EnvelopeStats {
    EnvelopeStats {
        msgs_seen: v["msgs_seen"].as_u64().expect("msgs_seen"),
        dropped: v["dropped"].as_u64().expect("dropped"),
        oversized: v["oversized"].as_u64().expect("oversized"),
        noraw_scanned: v.get("noraw_scanned").and_then(Value::as_u64),
        unknown_types: v["unknown_types"].as_u64().expect("unknown_types"),
        events_ingested: v["events_ingested"].as_u64().expect("events_ingested"),
        // Required, not optional: these are completeness figures, and a fixture that let them
        // default to zero would pin "reports nothing" as indistinguishable from "nothing to
        // report", which is the confusion the always-present rule exists to prevent.
        content_unresolved: v["content_unresolved"]
            .as_u64()
            .expect("content_unresolved"),
        content_bridge_entries: v["content_bridge_entries"]
            .as_u64()
            .expect("content_bridge_entries"),
        content_bridge_refused: v["content_bridge_evicted"]
            .as_u64()
            .expect("content_bridge_evicted"),
        publisher_restarts: v["publisher_restarts"]
            .as_u64()
            .expect("publisher_restarts"),
        rss_bytes: v.get("rss_bytes").and_then(Value::as_u64),
        // Not optional here even though the field is optional on the wire: the fixture
        // declares it, and a harness that defaulted a missing declaration would pin the
        // reading -- absence means "none" -- that spec 2.3 forbids.
        reuse_reporting: serde_json::from_value(v["reuse_reporting"].clone())
            .expect("a pinned reuse_reporting"),
        endpoints: v["endpoints"]
            .as_array()
            .expect("endpoints")
            .iter()
            .map(|e| EndpointStats {
                source: e["source"].as_str().expect("source").into(),
                endpoint: e["endpoint"].as_str().expect("endpoint").to_string(),
                msgs_seen: e["msgs_seen"].as_u64().expect("msgs_seen"),
                dropped: e["dropped"].as_u64().expect("dropped"),
                last_msg_at_ms: e
                    .get("last_msg_at_ms")
                    .and_then(Value::as_f64)
                    .map(ProducerMs),
                topic: e.get("topic").and_then(Value::as_str).map(str::to_string),
                // Absent on records that predate per-endpoint scoping, and kept absent so
                // the round-trip never invents a measured zero.
                publisher_restarts: e.get("publisher_restarts").and_then(Value::as_u64),
            })
            .collect(),
    }
}

#[test]
fn reproduces_every_corpus_case() {
    let c = corpus();
    let version = c["agent_version"].as_str().expect("agent_version");
    let cases = c["cases"].as_array().expect("cases");
    assert_eq!(cases.len(), 3, "all three kinds pinned");
    // The epoch every canary in this corpus is derived under. Lifecycle records other than
    // `agent_start` carry no epoch of their own, so it is read once from the config that does.
    let key_epoch = u32::try_from(
        cases
            .iter()
            .find_map(|c| c["config"]["key_epoch"].as_u64())
            .expect("some case pins the key epoch"),
    )
    .unwrap();
    for case in cases {
        // Computed from the key here, not read out of the fixture.
        //
        // There are two implementations of the canary: this one, which ships, and the Python
        // in the spec repository's `tools/gen-lifecycle-corpus.py` that wrote the fixture. A
        // disagreement between them must be a test failure. Reading the fixture's canary and
        // handing it back to the builder would defeat that: the value would match itself, and
        // `pseudonym::canary` could return a constant and still pass. One canary per case, the
        // key in force; a fixture claiming another key-identity shape fails the comparison.
        let built = build_case(case, version, key_epoch);
        assert_eq!(
            built.to_value(),
            case["expect"],
            "case {:?} not reproduced",
            case["kind"]
        );
    }
}

/// The capacity signal reproduces its corpus case and rides the chain like everything else:
/// no ids to map, so no epoch stamp. It is instance-scoped by design, unlike the lifecycle
/// bracket, because it declares a loss for one observed holder over one engine-clock window.
#[test]
fn reproduces_the_capacity_case_and_it_passes_the_chain() {
    let c = corpus();
    let cases = c["capacity_cases"].as_array().expect("capacity_cases");
    assert!(!cases.is_empty(), "the capacity signal is pinned");
    for case in cases {
        let r = &case["refusal"];
        // The refusal names the source key; the record carries the label it rolls up to.
        // In the corpus the two are one name, as on a single-rank engine.
        let instance = r["instance_id"].as_str().expect("instance_id");
        let built = identity_refused_record(
            &IdentityRefusal {
                source: SourceKey::from(instance),
                dp_rank: u32::try_from(r["dp_rank"].as_u64().expect("dp_rank")).unwrap(),
                group_idx: u32::try_from(r["group_idx"].as_u64().expect("group_idx")).unwrap(),
                window_start_ms: EngineMs(r["window_start_ms"].as_f64().expect("window_start_ms")),
                window_end_ms: EngineMs(r["window_end_ms"].as_f64().expect("window_end_ms")),
                refused: r["refused"].as_u64().expect("refused"),
            },
            &InstanceLabel::from(instance),
            ProducerMs(case["at_ms"].as_f64().expect("at_ms")),
        );
        assert_eq!(
            built.to_value(),
            case["expect"],
            "identity_refused not reproduced"
        );
        let mut sink = test_chain();
        sink.emit(built);
        let records = sink.into_inner().into_inner().records;
        assert!(
            records[0].get("epoch").is_none(),
            "no ids, so no epoch stamp"
        );
        assert_eq!(records[0]["instance_id"], "i0", "holder-scoped, by design");
    }
}

/// The agent version and the key epoch the corpus pins.
fn corpus_version_and_epoch(c: &Value) -> (&str, u32) {
    let version = c["agent_version"].as_str().expect("agent_version");
    let key_epoch = u32::try_from(
        c["cases"]
            .as_array()
            .expect("cases")
            .iter()
            .find_map(|c| c["config"]["key_epoch"].as_u64())
            .expect("some case pins the key epoch"),
    )
    .unwrap();
    (version, key_epoch)
}

/// The key the spec repository's `tools/gen-lifecycle-corpus.py` generates the corpus under:
/// a 32-byte HMAC key followed by the 32-byte canary nonce. Named once so the Rust and
/// Python sides cannot drift apart on the input while agreeing on the construction.
fn conformance_key() -> Key {
    Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
        .expect("key")
}

fn test_chain() -> PseudonymizingSink<GuardedSink<MemorySink, infertap::record::Pseudonymized>> {
    let key = conformance_key();
    PseudonymizingSink::new(
        PseudonymConfig { key, epoch: 0 },
        BLOCK_ID_CONTEXT,
        GuardedSink::new(MemorySink::new(None)),
    )
}

#[test]
fn lifecycle_records_pass_the_chain_unmapped() {
    let c = corpus();
    let (version, key_epoch) = corpus_version_and_epoch(&c);
    let mut sink = test_chain();
    for case in c["cases"].as_array().expect("cases") {
        sink.emit(build_case(case, version, key_epoch));
    }
    sink.emit(heartbeat_record(
        &stats_from(&c["cases"][1]["stats"]),
        None,
        ProducerMs(1_785_153_730_000.0),
    ));
    let scanned = sink.scanned_total();
    assert!(scanned > 0, "the no-raw-id scan covered every field");
    let records = sink.into_inner().into_inner().records;
    assert_eq!(records.len(), 4);
    for r in &records {
        // no ids to map: no epoch stamp, and block_id/parent_id never appear
        assert!(r.get("epoch").is_none());
        assert!(r.get("block_id").is_none());
        assert!(r.get("parent_id").is_none());
        assert!(r.get("instance_id").is_none()); // agent-scoped, by contract
    }
}

#[test]
fn a_graceful_stream_ends_with_the_stop_record() {
    let c = corpus();
    let (version, key_epoch) = corpus_version_and_epoch(&c);
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut sink = GuardedSink::new(JsonlSink::new(&mut buf));
        for case in c["cases"].as_array().expect("cases") {
            sink.emit(build_case(case, version, key_epoch));
        }
        sink.flush().expect("flush");
    }
    let text = String::from_utf8(buf).expect("utf8");
    let last = text.lines().last().expect("a line");
    let v: Value = serde_json::from_str(last).expect("last line parses");
    assert_eq!(v["kind"], "agent_stop");
    assert_eq!(v["reason"], "signal");
}
