// SPDX-License-Identifier: Apache-2.0
//! The egress pseudonymization reproduces `conformance/pseudonym/vectors.json` exactly: every
//! pinned (context, epoch, `raw_id`) vector under the corpus's labeled test-only key, plus the
//! chained-row property: a record's `parent_id` maps under the same construction as
//! `block_id`, so the prefix edge survives pseudonymization.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

use infertap::pseudonym::{pseudonym, Key, BLOCK_ID_CONTEXT, CONTENT_ID_CONTEXT};
use infertap::sink::{MemorySink, PseudonymConfig, PseudonymizingSink, Sink};
use serde_json::{json, Value};

fn corpus() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/pseudonym/vectors.json");
    serde_json::from_str(&fs::read_to_string(path).expect("read")).expect("json")
}

fn corpus_key(c: &Value) -> Key {
    Key::from_hex_or_base64(c["test_key_hex"].as_str().expect("test_key_hex")).expect("key")
}

#[test]
fn reproduces_every_vector() {
    let c = corpus();
    let key = corpus_key(&c);
    let vectors = c["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 10, "expected >=10 vectors");
    for v in vectors {
        let context = v["context"].as_str().expect("context");
        let epoch = u32::try_from(v["epoch"].as_u64().expect("epoch")).expect("epoch fits u32");
        let raw = v["raw_id"]
            .as_str()
            .map_or_else(|| v["raw_id"].to_string(), str::to_owned);
        let actual = if context == CONTENT_ID_CONTEXT {
            infertap::pseudonym::pseudonym_content(&key, epoch, raw.parse().expect("u128"))
        } else {
            pseudonym(&key, context, epoch, raw.parse().expect("u64"))
        };
        assert_eq!(
            actual,
            v["pseudonym"].as_str().expect("pseudonym"),
            "vector (context={context}, epoch={epoch}, raw_id={raw}) not reproduced"
        );
    }
}

#[test]
fn the_chained_row_edge_survives_the_sink() {
    let c = corpus();
    let mut s = PseudonymizingSink::new(
        PseudonymConfig {
            key: corpus_key(&c),
            epoch: 0,
        },
        BLOCK_ID_CONTEXT,
        MemorySink::new(None),
    );
    // The corpus's chained row: child raw 7 whose parent is raw 1.
    let mut child = infertap::testing::store(7);
    child.parent_id = Some(infertap::record::Id(1));
    s.emit(infertap::record::Record::Store(child));
    s.emit(infertap::testing::store_record(1));
    let rs = s.into_inner().records;
    // The edge survives: the child's parent pseudonym IS the parent's block pseudonym, and
    // both ends match the pinned vectors.
    assert_eq!(rs[0]["parent_id"], rs[1]["block_id"]);
    assert_eq!(rs[0]["block_id"], json!("a11dadf39bcf1cfbe1169b3201b0c56c"));
    assert_eq!(
        rs[0]["parent_id"],
        json!("db83955d1a485fe98364fd9e47c8bc08")
    );
}

/// Check production context constants against the pinned corpus.
///
/// Vector tests supply context strings directly, so they do not detect a changed
/// production constant. Check each identity domain used by the egress mapper.
#[test]
fn the_context_constants_match_the_pinned_corpus() {
    let c = corpus();
    let contexts: Vec<&str> = c["vectors"]
        .as_array()
        .expect("vectors")
        .iter()
        .filter_map(|v| v["context"].as_str())
        .collect();
    for expected in [BLOCK_ID_CONTEXT, CONTENT_ID_CONTEXT] {
        assert!(
            contexts.contains(&expected),
            "the corpus pins no vectors for {expected:?}: either the constant moved without \
             the corpus, or a purpose is shipping unpinned"
        );
    }
    assert_ne!(
        BLOCK_ID_CONTEXT, CONTENT_ID_CONTEXT,
        "the two identity spaces share a context, so one value would map to one pseudonym in \
         both and a consumer joining across them would find spurious matches"
    );
}

#[test]
fn backend_identity_is_pseudonymized_once() {
    use infertap::pseudonym::{pseudonym_str, BACKEND_ID_CONTEXT};
    use infertap::record::{OperatorText, Record};
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/pseudonym/backend.json");
    let corpus: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(corpus["context"], BACKEND_ID_CONTEXT);
    for vector in corpus["vectors"].as_array().unwrap() {
        let raw = vector["raw"].as_str().unwrap();
        let epoch = u32::try_from(vector["epoch"].as_u64().unwrap()).unwrap();
        let key = corpus_key(&corpus);
        assert_eq!(
            pseudonym_str(&key, BACKEND_ID_CONTEXT, epoch, raw),
            vector["pseudonym"].as_str().unwrap()
        );
        let mut sink = PseudonymizingSink::new(
            PseudonymConfig { key, epoch },
            BLOCK_ID_CONTEXT,
            MemorySink::new(None),
        );
        let mut store = infertap::testing::store(7);
        store.backend_id = Some(OperatorText(raw.into()));
        let evict = infertap::record::Evict {
            instance_id: store.instance_id.clone(),
            at_ms: store.at_ms,
            seq: store.seq,
            dp_rank: store.dp_rank,
            group_idx: store.group_idx,
            block_id: store.block_id,
            tier: store.tier.clone(),
            locality: None,
            backend_id: store.backend_id.clone(),
            content_id: None,
            epoch: None,
        };
        sink.emit(Record::Store(store));
        sink.emit(Record::Evict(evict));
        let records = sink.into_inner().records;
        assert_eq!(records[0]["backend_id"], vector["pseudonym"]);
        assert_eq!(records[1]["backend_id"], vector["pseudonym"]);
    }
}
