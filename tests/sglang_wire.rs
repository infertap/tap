// SPDX-License-Identifier: Apache-2.0
//! Reproduce the vendored `SGLang` wire corpus at the framing and normalization boundary,
//! under the adapter's `SGLang` profile. Valid fixtures use captured payloads with
//! independently derived expected events. Malformed fixtures assert the error stage and
//! message. The fixture reader is shared with `tests/vllm_wire.rs`; the profile is what
//! differs.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

use infertap::clock::EngineMs;
use infertap::event_model::{Cleared, Event, HolderReset, Origin, Parent, Removed, Stored, Term};
use infertap::source::sglang::Sglang;
use infertap::source::wire::content::ContentBridge;
use infertap::source::wire::{decode_payload, normalize_batch};
use serde_json::Value;

const PROFILE: Sglang = Sglang;

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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/sglang-wire")
}

fn opt_str(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

fn content_id_of(v: &Value) -> Option<u128> {
    v.get("content_id")
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| value.as_u64().map(|n| n.to_string()))
        })
        .map(|value| value.parse().expect("content identity"))
}

fn expected_event(v: &Value) -> Event {
    let origin = || Origin {
        dp_rank: u32::try_from(v["dp_rank"].as_u64().expect("dp_rank")).expect("dp_rank fits"),
        group_idx: u32::try_from(v["group_idx"].as_u64().expect("group_idx"))
            .expect("group_idx fits"),
    };
    let at_ms = EngineMs(v["at_ms"].as_f64().expect("at_ms"));
    let tier = v.get("medium").and_then(Value::as_str).map(Term::from);
    match v["type"].as_str().expect("type") {
        "Stored" => Event::Stored(Stored {
            origin: origin(),
            block_hash: v["block_hash"].as_u64().expect("block_hash"),
            parent: parent_of(v),
            block_size: v["block_size"].as_u64().expect("block_size"),
            tier,
            extra_keys: v.get("extra_keys").and_then(Value::as_array).map(|a| {
                a.iter()
                    .map(|x| x.as_str().expect("extra key string").to_string())
                    .collect()
            }),
            lora_id: v.get("lora_id").and_then(Value::as_u64),
            lora_name: opt_str(v, "lora_name"),
            spec_kind: opt_str(v, "spec_kind").map(Term::from),
            spec_sliding_window: v.get("spec_sliding_window").and_then(Value::as_i64),
            reused: v.get("reused").and_then(Value::as_bool),
            locality: opt_str(v, "locality").map(Term::from),
            backend_id: None,
            content_id: content_id_of(v),
            at_ms,
        }),
        "Removed" => Event::Removed(Removed {
            origin: origin(),
            block_hash: v["block_hash"].as_u64().expect("block_hash"),
            tier,
            locality: opt_str(v, "locality").map(Term::from),
            backend_id: None,
            content_id: content_id_of(v),
            at_ms,
        }),
        "Cleared" => Event::Cleared(Cleared {
            dp_rank: v
                .get("dp_rank")
                .and_then(Value::as_u64)
                .map(|d| u32::try_from(d).expect("dp_rank fits")),
            group_idx: v
                .get("group_idx")
                .and_then(Value::as_u64)
                .map(|g| u32::try_from(g).expect("group_idx fits")),
            tier,
            at_ms,
        }),
        other => panic!("unknown expected type {other:?}"),
    }
}

/// See `tests/vllm_wire.rs`: the corpus pins microseconds, every other field stays exact.
fn canon(e: &Event) -> Event {
    let r = |ms: EngineMs| EngineMs((ms.0 * 1e3).round() / 1e3);
    match e.clone() {
        Event::Stored(s) => Event::Stored(Stored {
            at_ms: r(s.at_ms),
            ..s
        }),
        Event::Removed(s) => Event::Removed(Removed {
            at_ms: r(s.at_ms),
            ..s
        }),
        Event::Cleared(s) => Event::Cleared(Cleared {
            at_ms: r(s.at_ms),
            ..s
        }),
        Event::HolderReset(s) => Event::HolderReset(HolderReset {
            boundary_ms: r(s.boundary_ms),
            ..s
        }),
    }
}

fn check_valid(name: &str, bytes: &[u8], expect: &Value) {
    let value =
        decode_payload(bytes).unwrap_or_else(|e| panic!("{name}: decode should succeed: {e}"));
    let got = normalize_batch(&PROFILE, &value, &mut ContentBridge::default())
        .unwrap_or_else(|e| panic!("{name}: normalize should succeed: {e}"));
    let want: Vec<Event> = expect["events"]
        .as_array()
        .expect("events")
        .iter()
        .map(expected_event)
        .collect();
    assert_eq!(got.events.len(), want.len(), "{name}: event count mismatch");
    for (i, (a, b)) in got.events.iter().zip(want.iter()).enumerate() {
        assert_eq!(canon(a), canon(b), "{name}: event {i} mismatch");
    }
    let want_unknown = expect["unknown_types"].as_u64().expect("unknown_types");
    assert_eq!(
        got.unknown_types, want_unknown,
        "{name}: unknown_types mismatch"
    );
    // The corpus is this engine's own vocabulary: nothing in it is drift under its profile.
    assert!(
        got.unknown_fields.is_empty(),
        "{name}: captured fields read as drift: {:?}",
        got.unknown_fields
    );
}

fn check_malformed(name: &str, bytes: &[u8], err: &Value) {
    let stage = err["stage"].as_str().expect("stage");
    let contains = err.get("contains").and_then(Value::as_str).unwrap_or("");
    let msg = match stage {
        "decode" => decode_payload(bytes)
            .expect_err(&format!("{name}: expected a decode-stage error"))
            .to_string(),
        "normalize" => {
            let value = decode_payload(bytes)
                .unwrap_or_else(|e| panic!("{name}: decode should succeed: {e}"));
            normalize_batch(&PROFILE, &value, &mut ContentBridge::default())
                .expect_err(&format!("{name}: expected a normalize-stage error"))
                .to_string()
        }
        "any" => match decode_payload(bytes) {
            Err(e) => e.to_string(),
            Ok(value) => normalize_batch(&PROFILE, &value, &mut ContentBridge::default())
                .expect_err(&format!("{name}: expected an error at some stage"))
                .to_string(),
        },
        other => panic!("{name}: unknown stage {other:?}"),
    };
    assert!(
        msg.contains(contains),
        "{name}: error {msg:?} does not contain {contains:?}"
    );
}

#[test]
fn reproduces_sglang_wire_corpus() {
    let (mut valid, mut malformed) = (0, 0);
    for entry in fs::read_dir(fixtures_dir()).expect("fixtures dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|e| e != "json")
            || path.file_name().is_some_and(|n| n == "manifest.json")
        {
            continue;
        }
        let fx: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read")).expect("json");
        let name = fx["name"].as_str().expect("name");
        assert!(
            fx["source"].is_string(),
            "{name}: a fixture names its source"
        );
        assert!(
            fx["producer"]["sglang"].is_string(),
            "{name}: the corpus names its engine version"
        );
        let bytes = hex::decode(fx["wire_hex"].as_str().expect("wire_hex")).expect("hex");
        let expect = &fx["expect"];
        if expect.get("events").is_some() {
            check_valid(name, &bytes, expect);
            valid += 1;
        } else {
            check_malformed(name, &bytes, &expect["error"]);
            malformed += 1;
        }
    }
    assert!(valid >= 12, "expected >=12 valid fixtures, found {valid}");
    assert!(
        malformed >= 12,
        "expected >=12 malformed fixtures, found {malformed}"
    );
}

fn load_fixture(name: &str) -> (Value, Vec<u8>) {
    infertap::testing::wire_fixture("sglang-wire", name)
}

/// A captured clear releases the bridge population. The vLLM corpus pins this from a
/// constructed clear; here the clear came off the wire.
#[test]
fn a_captured_clear_leaves_the_bridge_population_at_the_resident_set() {
    let mut bridge = ContentBridge::default();
    let (fx, bytes) = load_fixture("stored_root.json");
    let stored = fx["expect"]["events"].as_array().expect("events").len();
    let value = decode_payload(&bytes).expect("decode");
    normalize_batch(&PROFILE, &value, &mut bridge).expect("normalize");
    assert_eq!(bridge.len(), stored, "every rooted store is an entry");
    let (_, bytes) = load_fixture("cleared_all.json");
    let value = decode_payload(&bytes).expect("decode");
    normalize_batch(&PROFILE, &value, &mut bridge).expect("normalize");
    assert!(bridge.is_empty(), "a clear empties its scope");
    assert_eq!(
        bridge.unscoped_clears(),
        1,
        "a fieldless clear is unscoped over the group dimension"
    );
}

/// The salted fixture's first block carries the salt as its extra keys and derives; the
/// vLLM corpus's salted fixture carries the same salt in the same place.
#[test]
fn the_captured_salt_enters_on_the_first_block_and_derives() {
    let (fx, bytes) = load_fixture("stored_salted.json");
    let value = decode_payload(&bytes).expect("decode");
    let got = normalize_batch(&PROFILE, &value, &mut ContentBridge::default()).expect("normalize");
    let first = got
        .events
        .iter()
        .find_map(|e| match e {
            Event::Stored(s) if s.parent == Parent::Root => Some(s),
            _ => None,
        })
        .expect("a rooted store");
    let salt = fx["expect"]["events"][0]["extra_keys"][0]
        .as_str()
        .expect("the fixture's salt");
    assert_eq!(first.extra_keys.as_deref(), Some(&[salt.to_string()][..]));
    assert!(first.content_id.is_some(), "a rooted salted run derives");
    for e in &got.events {
        if let Event::Stored(s) = e {
            if s.parent != Parent::Root {
                assert_eq!(s.extra_keys, None, "no later block carries the salt");
            }
        }
    }
}

/// A negative wire hash is the same 64 bits unsigned: it lands above `i64::MAX`.
#[test]
fn negative_hashes_round_trip_as_unsigned_values() {
    let (_, bytes) = load_fixture("hash_negative.json");
    let value = decode_payload(&bytes).expect("decode");
    let got = normalize_batch(&PROFILE, &value, &mut ContentBridge::default()).expect("normalize");
    let big = got.events.iter().any(|e| match e {
        Event::Stored(Stored { block_hash, .. }) | Event::Removed(Removed { block_hash, .. }) => {
            *block_hash > i64::MAX as u64
        }
        Event::Cleared(_) | Event::HolderReset(_) => false,
    });
    assert!(big, "a negative wire hash reads as a value above i64::MAX");
}

/// Engine timestamps are seconds on this wire as on vLLM's.
#[test]
fn timestamps_are_milliseconds_at_wall_clock_scale() {
    let (_, bytes) = load_fixture("stored_single.json");
    let value = decode_payload(&bytes).expect("decode");
    let got = normalize_batch(&PROFILE, &value, &mut ContentBridge::default()).expect("normalize");
    let EngineMs(at_ms) = got.events.first().expect("an event").engine_ms();
    assert!(
        (1e12..1e13).contains(&at_ms),
        "at_ms {at_ms} is not wall-clock milliseconds"
    );
}
