// SPDX-License-Identifier: Apache-2.0
//! Compare transducer output with the pinned telemetry corpus.
//!
//! Fixtures cover field mapping, omitted values, identity scope, and record order.
//! Expected records are generated upstream from the contract independently of this
//! implementation.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

use infertap::clock::{EngineMs, ProducerMs};
use infertap::compute::Transducer;
use infertap::event_model::{Cleared, Event, HolderReset, Origin, Parent, Removed, Stored, Term};
use infertap::record::InstanceLabel;
use infertap::sink::MemorySink;
use serde_json::{json, Value};

/// The fixture's three-state parent: a named hash, an explicit unknown, or a root by absence.
fn parent_of(v: &Value) -> Parent {
    if let Some(h) = v.get("parent_hash").and_then(Value::as_u64) {
        Parent::Hash(h)
    } else if v.get("parent_unknown").and_then(Value::as_bool) == Some(true) {
        Parent::Unknown
    } else {
        Parent::Root
    }
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/telemetry")
}

fn parse_event(v: &Value) -> (String, Event, Option<u64>) {
    let source = v["source"].as_str().expect("source").to_string();
    let at_ms = v["at_ms"].as_f64().expect("at_ms");
    let engine_ms = EngineMs(at_ms);
    let tier = v.get("medium").and_then(Value::as_str).map(Term::from);
    let u32_of =
        |k: &str| u32::try_from(v.get(k).and_then(Value::as_u64).unwrap_or(0)).expect("fits u32");
    let origin = Origin {
        dp_rank: u32_of("dp_rank"),
        group_idx: u32_of("group_idx"),
    };
    let opt_str = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
    let event = match v["type"].as_str().expect("type") {
        "Stored" => Event::Stored(Stored {
            origin,
            block_hash: v["block_hash"].as_u64().expect("block_hash"),
            parent: parent_of(v),
            block_size: v["block_size"].as_u64().expect("block_size"),
            tier,
            extra_keys: v.get("extra_keys").and_then(Value::as_array).map(|a| {
                a.iter()
                    .map(|x| x.as_str().expect("extra key").to_string())
                    .collect()
            }),
            lora_id: v.get("lora_id").and_then(Value::as_u64),
            lora_name: opt_str("lora_name"),
            spec_kind: opt_str("spec_kind").map(Term::from),
            spec_sliding_window: v.get("spec_sliding_window").and_then(Value::as_i64),
            reused: v.get("reused").and_then(Value::as_bool),
            locality: opt_str("locality").map(Term::from),
            backend_id: None,
            content_id: v
                .get("content_id")
                .and_then(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| value.as_u64().map(|n| n.to_string()))
                })
                .map(|value| value.parse().expect("content identity")),
            at_ms: engine_ms,
        }),
        "Removed" => Event::Removed(Removed {
            origin,
            block_hash: v["block_hash"].as_u64().expect("block_hash"),
            tier,
            locality: opt_str("locality").map(Term::from),
            backend_id: None,
            content_id: v
                .get("content_id")
                .and_then(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| value.as_u64().map(|n| n.to_string()))
                })
                .map(|value| value.parse().expect("content identity")),
            at_ms: engine_ms,
        }),
        // The clear's scope is read as declared: a fixture that omits a scope key states
        // that the wire named no value, and the record must omit it too rather than claim
        // zero.
        "Cleared" => Event::Cleared(Cleared {
            dp_rank: v
                .get("dp_rank")
                .and_then(Value::as_u64)
                .map(|d| u32::try_from(d).expect("fits u32")),
            group_idx: v
                .get("group_idx")
                .and_then(Value::as_u64)
                .map(|g| u32::try_from(g).expect("fits u32")),
            tier,
            at_ms: engine_ms,
        }),
        // Synthesized by the subscriber in live runs; a fixture states it directly so the
        // corpus pins the record's shape. The fixture's `at_ms` is the producer clock at
        // detection, `boundary_ms` the engine clock (contract sections 2.3 and 2.6).
        "HolderReset" => Event::HolderReset(HolderReset {
            boundary_ms: EngineMs(v["boundary_ms"].as_f64().expect("boundary_ms")),
            detected_at_ms: ProducerMs(at_ms),
            dp_rank: v
                .get("dp_rank")
                .and_then(Value::as_u64)
                .map(|d| u32::try_from(d).expect("fits u32")),
        }),
        other => panic!("unknown event type {other:?}"),
    };
    (source, event, v.get("seq").and_then(Value::as_u64))
}

fn run_fixture(fx: &Value) -> Value {
    let mut transducer = Transducer::new();
    let mut sink = MemorySink::new(None);
    for ev in fx["events"].as_array().expect("events") {
        let (source, event, seq) = parse_event(ev);
        // The telemetry corpus states seq per event; a fixture without one exercises
        // the transport-has-no-sequence path.
        transducer.ingest(&InstanceLabel::from(source), &event, seq, &mut sink);
    }
    json!({
        "record_counts": serde_json::to_value(&sink.counts).expect("counts"),
        "records": Value::Array(sink.records.clone()),
    })
}

#[test]
fn reproduces_every_fixture() {
    let dir = fixtures_dir();
    let mut count = 0;
    for entry in fs::read_dir(&dir).expect("fixtures dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let fx: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read")).expect("json");
        assert_eq!(
            run_fixture(&fx),
            fx["expect"],
            "fixture {:?} not reproduced",
            path.file_name().unwrap()
        );
        count += 1;
    }
    assert!(count >= 5, "expected >=5 fixtures, found {count}");
}
