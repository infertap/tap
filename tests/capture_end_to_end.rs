// SPDX-License-Identifier: Apache-2.0
//! Replay captured wire payloads through the adapter and transducer to emitted records.
//! Properties check parent-chain consistency and content identity across the full path.

#![allow(clippy::unwrap_used, clippy::expect_used)] // a test that cannot fail loudly is not a test

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use infertap::compute::Transducer;
use infertap::record::InstanceLabel;
use infertap::sink::MemorySink;
use infertap::source::vllm::Vllm;
use infertap::source::wire::content::ContentBridge;
use infertap::source::wire::{decode_payload, normalize_batch};
use serde_json::Value;

fn capture_files() -> Vec<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("capture");
    let mut out: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| {
            panic!(
                "read {}: {e}\nthe capture is not in the tree; run scripts/fetch-capture.sh \
                 once (it downloads the corpus tag's release asset and verifies its digest)",
                dir.display()
            )
        })
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            // the capture directory also holds reports, manifests and inspection dumps; only
            // the payload logs carry wire bytes
            p.extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("jsonl"))
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("capture-"))
        })
        .collect();
    assert!(!out.is_empty(), "no capture files under {}", dir.display());
    out.sort();
    out
}

/// Every record the capture produces, through the real adapter and the real transducer.
fn records_from_capture() -> Vec<Value> {
    let mut transducer = Transducer::new();
    let mut sink = MemorySink::new(None);
    for path in capture_files() {
        // One bridge for the whole file, carried across payloads in arrival order. A fresh
        // bridge per payload would resolve nothing beyond prefix roots, which is exactly the
        // degraded state a tap sits in immediately after a restart.
        let mut bridge = ContentBridge::default();
        // Each capture line is one observed ZMQ payload, with its bytes as hex. The source id
        // is the capture file, which is what "one engine instance" meant during the run.
        let source = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("capture file stem");
        for line in fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let rec: Value = serde_json::from_str(line).expect("capture line is JSON");
            let Some(hex) = rec.get("payload_hex").and_then(Value::as_str) else {
                continue;
            };
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("payload hex"))
                .collect();
            let batch = decode_payload(&bytes).expect("captured payload decodes");
            let normalized =
                normalize_batch(&Vllm, &batch, &mut bridge).expect("captured batch normalizes");
            for event in &normalized.events {
                transducer.ingest(&InstanceLabel::from(source), event, None, &mut sink);
            }
        }
    }
    sink.records.clone()
}

/// Share of `n` in `total`, for the failure message only.
fn percent(n: usize, total: usize) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let (n, total) = (
        u32::try_from(n).unwrap_or(u32::MAX),
        u32::try_from(total).unwrap_or(u32::MAX),
    );
    100.0 * f64::from(n) / f64::from(total)
}

/// A block hash is content-addressed: it commits to its parent, its tokens, and its extra
/// keys, so a given block id has exactly one parent everywhere. The prefix chain rests on
/// that property, and a consumer's dedup metric reads the chain as truth.
///
/// Two distinct parents for one block id is therefore a reader defect, not unusual data.
/// The defect this pins is giving every block in a stored run the run's parent instead of
/// its predecessor.
#[test]
fn every_block_in_the_capture_claims_exactly_one_parent() {
    let mut parents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut with_parent = 0usize;
    for r in records_from_capture() {
        if r.get("kind").and_then(Value::as_str) != Some("store") {
            continue;
        }
        let Some(block) = r.get("block_id") else {
            continue;
        };
        if let Some(parent) = r.get("parent_id") {
            with_parent += 1;
            parents
                .entry(block.to_string())
                .or_default()
                .insert(parent.to_string());
        }
    }

    // Never vacuous: if the capture stopped carrying chained stores, this test must fail
    // loudly rather than pass over an empty set.
    assert!(
        with_parent > 10_000,
        "only {with_parent} chained stores in the capture: the corpus or the adapter changed \
         shape, and this property is no longer being exercised"
    );

    let conflicted: Vec<(&String, &BTreeSet<String>)> =
        parents.iter().filter(|(_, ps)| ps.len() > 1).collect();
    assert!(
        conflicted.is_empty(),
        "{} of {} distinct blocks claim more than one parent ({:.1}%). A content-addressed \
         block has exactly one parent, so this is a READER defect, not odd traffic. First few: \
         {:?}",
        conflicted.len(),
        parents.len(),
        percent(conflicted.len(), parents.len()),
        conflicted.iter().take(3).collect::<Vec<_>>()
    );
}

/// The run's shape, asserted where it is actually observable.
///
/// `BlockStored` carries a contiguous run of blocks whose tokens span the whole run, so
/// `len(token_ids) == len(block_hashes) * block_size` holds whenever no block was skipped.
/// vLLM skips null/masked blocks for sliding-window and Mamba models, and when it does, list
/// order stops implying parenthood, which is the one case the chaining rule must not guess at.
///
/// This capture has zero skips (1,215 of 1,215 events exact), so the guard is unexercised here
/// and the fixture corpus carries the constructed case. Pinned anyway: if a future capture
/// contains skips, the assumption behind the chain changes and this says so.
#[test]
fn the_capture_contains_no_skipped_blocks_within_a_run() {
    let mut exact = 0usize;
    let mut skipped = 0usize;
    for path in capture_files() {
        for line in fs::read_to_string(&path)
            .expect("read capture")
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let rec: Value = serde_json::from_str(line).expect("capture line is JSON");
            let Some(hex) = rec.get("payload_hex").and_then(Value::as_str) else {
                continue;
            };
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("payload hex"))
                .collect();
            let batch = decode_payload(&bytes).expect("decodes");
            let Some(events) = batch.get(1).and_then(Value::as_array) else {
                continue;
            };
            for ev in events {
                if ev.get("type").and_then(Value::as_str) != Some("BlockStored") {
                    continue;
                }
                let hashes = ev
                    .get("block_hashes")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                let tokens = ev
                    .get("token_ids")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                let bs = ev
                    .get("block_size")
                    .and_then(Value::as_u64)
                    .and_then(|b| usize::try_from(b).ok())
                    .expect("block_size");
                if tokens == hashes * bs {
                    exact += 1;
                } else {
                    skipped += 1;
                }
            }
        }
    }
    assert!(exact > 1_000, "only {exact} store events seen");
    assert_eq!(
        skipped, 0,
        "{skipped} events carry fewer hashes than their token span implies: blocks were \
         skipped, so list order no longer implies parenthood in this capture"
    );
}

/// Replay captured traffic with original and bijectively relabeled engine hashes.
/// Engine IDs must differ while content IDs remain identical for the same inputs.
#[test]
fn content_identity_survives_a_change_of_the_engine_hash_space() {
    /// A bijection on u64, standing in for a different `NONE_HASH`.
    fn relabel(h: u64) -> u64 {
        h ^ 0x5DEE_CE66_D0BA_DBAD
    }

    fn remap(batch: &mut Value) {
        let Some(events) = batch.get_mut(1).and_then(Value::as_array_mut) else {
            return;
        };
        for ev in events {
            for key in ["block_hashes"] {
                if let Some(a) = ev.get_mut(key).and_then(Value::as_array_mut) {
                    for h in a.iter_mut() {
                        if let Some(v) = h.as_u64() {
                            *h = Value::from(relabel(v));
                        }
                    }
                }
            }
            if let Some(v) = ev.get("parent_block_hash").and_then(Value::as_u64) {
                ev["parent_block_hash"] = Value::from(relabel(v));
            }
        }
    }

    let ids = |relabelled: bool| -> (BTreeSet<String>, BTreeSet<String>) {
        let (mut engine, mut content) = (BTreeSet::new(), BTreeSet::new());
        for path in capture_files() {
            let mut bridge = ContentBridge::default();
            let mut transducer = Transducer::new();
            let mut sink = MemorySink::new(None);
            let source = path.file_stem().and_then(|s| s.to_str()).expect("stem");
            for line in fs::read_to_string(&path)
                .expect("read capture")
                .lines()
                .filter(|l| !l.trim().is_empty())
            {
                let rec: Value = serde_json::from_str(line).expect("json");
                let Some(hex) = rec.get("payload_hex").and_then(Value::as_str) else {
                    continue;
                };
                let bytes: Vec<u8> = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
                    .collect();
                let mut batch = decode_payload(&bytes).expect("decode");
                if relabelled {
                    remap(&mut batch);
                }
                let n = normalize_batch(&Vllm, &batch, &mut bridge).expect("normalize");
                for ev in &n.events {
                    transducer.ingest(&InstanceLabel::from(source), ev, None, &mut sink);
                }
            }
            for r in &sink.records {
                if r.get("kind").and_then(Value::as_str) != Some("store") {
                    continue;
                }
                // Identities are decimal strings on the wire (contract section 2.2); the set
                // comparison below needs only equality, which strings carry exactly.
                if let Some(b) = r.get("block_id").and_then(Value::as_str) {
                    engine.insert(b.to_string());
                }
                if let Some(c) = r.get("content_id").and_then(Value::as_str) {
                    content.insert(c.to_string());
                }
            }
        }
        (engine, content)
    };

    let (engine_a, content_a) = ids(false);
    let (engine_b, content_b) = ids(true);
    assert!(!content_a.is_empty(), "no content ids derived at all");
    assert_eq!(
        engine_a.intersection(&engine_b).count(),
        0,
        "the relabelling must move every engine id, or this proves nothing"
    );
    assert_eq!(
        content_a, content_b,
        "content identity moved when the engine's hash space did: it is not content-addressed, \
         and cross-node duplication would still read as zero"
    );
}
