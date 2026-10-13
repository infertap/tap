// SPDX-License-Identifier: Apache-2.0
//! Reproduce the vendored vLLM wire corpus at the framing and normalization boundary.
//! Valid fixtures use captured payloads with independently derived expected events.
//! Malformed fixtures assert the error stage and message.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

use infertap::clock::EngineMs;
use infertap::event_model::{Cleared, Event, HolderReset, Origin, Parent, Removed, Stored, Term};
use infertap::source::vllm::Vllm;
use infertap::source::wire::content::ContentBridge;
use infertap::source::wire::{decode_payload, normalize_batch};
use serde_json::Value;

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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/vllm-wire")
}

fn opt_str(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

fn expected_event(v: &Value) -> Event {
    // Block events always carry both scope dimensions; only a Cleared may omit them, and
    // reads them itself as declared-or-absent.
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
            content_id: v
                .get("content_id")
                .and_then(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| value.as_u64().map(|n| n.to_string()))
                })
                .map(|value| value.parse().expect("content identity")),
            at_ms,
        }),
        "Removed" => Event::Removed(Removed {
            origin: origin(),
            block_hash: v["block_hash"].as_u64().expect("block_hash"),
            tier,
            locality: opt_str(v, "locality").map(Term::from),
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
            at_ms,
        }),
        // The clear's scope is declared, never defaulted: a fixture that omits a scope key
        // states that the wire named no value and the clear is global over that dimension.
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

/// `at_ms` is the one float in the model, and in a corpus it crosses a JSON boundary. Python
/// writes a shortest-round-trip literal; `serde_json`'s parser lands within 1 ULP of the
/// correctly-rounded value (measured: exactly 1 ULP low at wall-clock magnitudes). Bit equality
/// is therefore not a property a corpus can pin, so the corpus pins microseconds -- four orders
/// finer than any timestamp an engine produces -- and every other field stays exact.
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
        // Synthesized by the subscriber, never decoded from the wire. No corpus event can
        // carry it, but the engine instant rounds like every other for symmetry.
        Event::HolderReset(s) => Event::HolderReset(HolderReset {
            boundary_ms: r(s.boundary_ms),
            ..s
        }),
    }
}

fn check_valid(name: &str, bytes: &[u8], expect: &Value) {
    let value =
        decode_payload(bytes).unwrap_or_else(|e| panic!("{name}: decode should succeed: {e}"));
    let got = normalize_batch(&Vllm, &value, &mut ContentBridge::default())
        .unwrap_or_else(|e| panic!("{name}: normalize should succeed: {e}"));

    let want: Vec<Event> = expect["events"]
        .as_array()
        .expect("events")
        .iter()
        .map(expected_event)
        .collect();
    let mine: Vec<Event> = got.events;
    assert_eq!(mine.len(), want.len(), "{name}: event count mismatch");
    for (i, (a, b)) in mine.iter().zip(want.iter()).enumerate() {
        assert_eq!(canon(a), canon(b), "{name}: event {i} mismatch");
    }
    // Growth is tolerated but never silent: the corpus pins how many events were skipped.
    let want_unknown = expect["unknown_types"].as_u64().expect("unknown_types");
    assert_eq!(
        got.unknown_types, want_unknown,
        "{name}: unknown_types mismatch"
    );
}

fn check_malformed(name: &str, bytes: &[u8], err: &Value) {
    let stage = err["stage"].as_str().expect("stage");
    let contains = err.get("contains").and_then(Value::as_str).unwrap_or("");
    // `decode` = not valid msgpack; `normalize` = valid msgpack, invalid schema; `any` = either.
    // In every case: fail closed, no events.
    let msg = match stage {
        "decode" => decode_payload(bytes)
            .expect_err(&format!("{name}: expected a decode-stage error"))
            .to_string(),
        "normalize" => {
            let value = decode_payload(bytes)
                .unwrap_or_else(|e| panic!("{name}: decode should succeed: {e}"));
            normalize_batch(&Vllm, &value, &mut ContentBridge::default())
                .expect_err(&format!("{name}: expected a normalize-stage error"))
                .to_string()
        }
        "any" => match decode_payload(bytes) {
            Err(e) => e.to_string(),
            Ok(value) => normalize_batch(&Vllm, &value, &mut ContentBridge::default())
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
fn reproduces_vllm_wire_corpus() {
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
        // The fixture names its source for the reader; the adapter no longer needs it,
        // since the subscription a batch arrived on is the caller's knowledge.
        assert!(
            fx["source"].is_string(),
            "{name}: a fixture names its source"
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
    infertap::testing::wire_fixture("vllm-wire", name)
}

/// Carry one bridge across store and clear payloads, as the live pipeline does.
/// Rank-scoped and fully unscoped clears remove the stored entries. Both fixtures omit
/// group and tier, so both increment the undeclared-scope counter.
#[test]
fn a_clear_leaves_the_bridge_population_at_the_resident_set() {
    let mut bridge = ContentBridge::default();
    let (fx, bytes) = load_fixture("stored_root.json");
    let stored = fx["expect"]["events"].as_array().expect("events").len();
    let value = decode_payload(&bytes).expect("decode");
    normalize_batch(&Vllm, &value, &mut bridge).expect("normalize");
    assert_eq!(bridge.len(), stored, "every rooted store is an entry");

    let (_, bytes) = load_fixture("cleared_all.json");
    let value = decode_payload(&bytes).expect("decode");
    normalize_batch(&Vllm, &value, &mut bridge).expect("normalize");
    assert!(
        bridge.is_empty(),
        "a scoped clear empties its scope: population is the resident set, never tombstones"
    );
    // AllBlocksCleared carries no group or tier fields. Only the batch's rank
    // scopes these fixtures. Both clears release state across groups and tiers
    // and increment the undeclared-scope counter.
    assert_eq!(
        bridge.unscoped_clears(),
        1,
        "a clear declaring only dp_rank is unscoped over the group dimension"
    );

    // Rebuild, then clear with NO declared scope at all -- not even dp_rank.
    let (_, bytes) = load_fixture("stored_root.json");
    let value = decode_payload(&bytes).expect("decode");
    normalize_batch(&Vllm, &value, &mut bridge).expect("normalize");
    assert_eq!(bridge.len(), stored);
    let (_, bytes) = load_fixture("cleared_scope_undeclared.json");
    let value = decode_payload(&bytes).expect("decode");
    normalize_batch(&Vllm, &value, &mut bridge).expect("normalize");
    assert!(
        bridge.is_empty(),
        "an undeclared scope releases globally, never as zero"
    );
    assert_eq!(
        bridge.unscoped_clears(),
        2,
        "and the assumed scope is counted, once per clear"
    );
}

/// Engine hashes above `i64::MAX` must retain their unsigned value.
#[test]
fn hashes_above_i64_max_round_trip() {
    let fx: Value = serde_json::from_str(
        &fs::read_to_string(fixtures_dir().join("hash_above_i64_max.json")).expect("read"),
    )
    .expect("json");
    let bytes = hex::decode(fx["wire_hex"].as_str().expect("wire_hex")).expect("hex");
    let value = decode_payload(&bytes).expect("decode");
    let got = normalize_batch(&Vllm, &value, &mut ContentBridge::default()).expect("normalize");
    let big = got.events.iter().any(|e| match e {
        Event::Stored(Stored { block_hash, .. }) | Event::Removed(Removed { block_hash, .. }) => {
            *block_hash > i64::MAX as u64
        }
        Event::Cleared(_) | Event::HolderReset(_) => false,
    });
    assert!(big, "fixture must carry a hash above i64::MAX");
}

/// Convert engine timestamps from seconds to milliseconds at realistic wall-clock magnitudes.
#[test]
fn timestamps_are_milliseconds_at_wall_clock_scale() {
    let fx: Value = serde_json::from_str(
        &fs::read_to_string(fixtures_dir().join("stored_single.json")).expect("read"),
    )
    .expect("json");
    let bytes = hex::decode(fx["wire_hex"].as_str().expect("wire_hex")).expect("hex");
    let value = decode_payload(&bytes).expect("decode");
    let got = normalize_batch(&Vllm, &value, &mut ContentBridge::default()).expect("normalize");
    let EngineMs(at_ms) = got.events.first().expect("an event").engine_ms();
    // 1e12 ms is 2001; 1e13 is 2286. Real wall-clock milliseconds sit between.
    assert!(
        (1e12..1e13).contains(&at_ms),
        "at_ms {at_ms} is not wall-clock milliseconds"
    );
}

/// Reject token/hash geometry that exceeds the supplied token span.
/// A block-size product overflow must not pass the contiguity check or reach
/// per-block slicing.
#[test]
fn a_block_size_that_overflows_or_exceeds_the_token_span_cannot_panic() {
    use serde_json::json;
    let batch = |block_size: u64, tokens: u32| {
        json!([
            1.0,
            [{
                "type": "BlockStored",
                "block_hashes": [1, 2, 3, 4],
                "parent_block_hash": null,
                "token_ids": (0..tokens).collect::<Vec<u32>>(),
                "block_size": block_size,
                "medium": "GPU"
            }],
            0
        ])
    };
    // Beyond the span: corruption, refused.
    let err = normalize_batch(
        &Vllm,
        &batch((1u64 << 62) + 4, 16),
        &mut ContentBridge::default(),
    )
    .expect_err("a block size past the token span is refused");
    assert!(err.to_string().contains("exceeds"), "{err}");
    // Exactly the span, four blocks of four: derives as before.
    let ok = normalize_batch(&Vllm, &batch(4, 16), &mut ContentBridge::default())
        .expect("a well-formed run normalizes");
    assert_eq!(ok.events.len(), 4);
    // An empty token array with a huge block size is the tokenless offload shape, and the
    // product overflow must read as "not contiguous", not as a match.
    let ok = normalize_batch(
        &Vllm,
        &batch(u64::MAX / 2, 0),
        &mut ContentBridge::default(),
    )
    .expect("a tokenless store normalizes");
    assert_eq!(ok.events.len(), 4);
    assert!(ok.events.iter().all(|e| matches!(
        e,
        Event::Stored(Stored {
            parent: Parent::Unknown,
            ..
        })
    )));
}

/// A token id that is not one (negative, past `u32`, a string) makes the run's tokens
/// unusable: the store still normalizes, because its metadata is sound, but no identity is
/// derived from a list with a hole in it, and the run is not mistaken for the tokenless
/// offload shape that resolves through the bridge.
#[test]
fn an_unusable_token_id_derives_no_identity_and_is_not_tokenless() {
    use serde_json::json;
    let mut bridge = ContentBridge::default();
    // The block is stored once from GPU with sound tokens, so the bridge holds its id.
    let sound = json!([1.0, [{"type": "BlockStored", "block_hashes": [9], "block_size": 2,
        "token_ids": [1, 2], "parent_block_hash": null, "medium": "GPU"}], 0]);
    let first = normalize_batch(&Vllm, &sound, &mut bridge).expect("sound");
    let Event::Stored(Stored {
        content_id: Some(id),
        ..
    }) = &first.events[0]
    else {
        panic!("a sound run derives: {first:?}");
    };
    for bad in [json!(-1), json!(1u64 << 40), json!("7")] {
        let corrupt = json!([2.0, [{"type": "BlockStored", "block_hashes": [9], "block_size": 2,
            "token_ids": [1, bad], "parent_block_hash": null, "medium": "GPU"}], 0]);
        let out = normalize_batch(&Vllm, &corrupt, &mut bridge).expect("the store normalizes");
        let Event::Stored(Stored { content_id, .. }) = &out.events[0] else {
            panic!("{out:?}");
        };
        assert_eq!(*content_id, None, "no identity from a list with a hole");
        assert_ne!(
            content_id.as_ref(),
            Some(id),
            "and not the tokenless resolve"
        );
    }
}

/// A scope value that is present but not a u32 is corruption, not scope 0: coercing it
/// would merge a distinct cache into rank or group 0. Absent stays 0.
#[test]
fn an_out_of_range_scope_value_is_refused_not_coerced_to_zero() {
    use serde_json::json;
    let ev = |group: serde_json::Value| json!([1.0, [{"type": "BlockRemoved", "block_hashes": [1], "medium": "GPU", "group_idx": group}], 0]);
    for bad in [json!(-1), json!(1u64 << 32), json!("0")] {
        let err =
            normalize_batch(&Vllm, &ev(bad), &mut ContentBridge::default()).expect_err("refused");
        assert!(err.to_string().contains("group_idx"), "{err}");
    }
    let bad_dp = json!([1.0, [{"type": "BlockRemoved", "block_hashes": [1]}], -7]);
    assert!(normalize_batch(&Vllm, &bad_dp, &mut ContentBridge::default()).is_err());
    let ok = normalize_batch(&Vllm, &ev(json!(null)), &mut ContentBridge::default())
        .expect("absent is scope 0");
    assert_eq!(ok.events.len(), 1);
}

/// A field this build does not read is reported, never an error: the wire grows between
/// engine versions, and a renamed field the tap depends on must be visible as drift.
#[test]
fn an_unmodelled_field_is_named_as_drift() {
    use serde_json::json;
    let batch = json!([1.0, [
        {"type": "BlockRemoved", "block_hashes": [1], "medium": "GPU", "future_owner": "OWNED"},
        {"type": "BlockStored", "block_hashes": [2], "block_size": 1, "token_ids": [7], "parent_block_hash": null, "medium": "GPU", "future_owner": "OWNED", "future": 1}
    ], 0]);
    let out = normalize_batch(&Vllm, &batch, &mut ContentBridge::default()).expect("tolerated");
    assert_eq!(out.events.len(), 2);
    let drift: Vec<&str> = out.unknown_fields.iter().map(String::as_str).collect();
    assert_eq!(
        drift,
        [
            "BlockRemoved.future_owner",
            "BlockStored.future",
            "BlockStored.future_owner"
        ]
    );
}
