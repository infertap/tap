// SPDX-License-Identifier: Apache-2.0
//! Independent upstream vectors for the full-width content chain and its pseudonyms.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use infertap::pseudonym::{pseudonym_content, Key};
use infertap::source::wire::content::{chain, BlockInput};
use serde_json::Value;

#[test]
fn reproduces_full_width_content_and_pseudonym_vectors() {
    let corpus: Value =
        serde_json::from_str(include_str!("../conformance/content/vectors.json")).unwrap();
    let key = Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
        .unwrap();
    for vector in corpus["vectors"].as_array().unwrap() {
        let parent = vector["parent"].as_str().unwrap().parse().unwrap();
        let tokens: Vec<u32> = serde_json::from_value(vector["tokens"].clone()).unwrap();
        let extra_keys: Option<Vec<String>> =
            serde_json::from_value(vector["extra_keys"].clone()).unwrap();
        let actual = chain(
            parent,
            &BlockInput {
                tokens: &tokens,
                extra_keys: extra_keys.as_deref(),
            },
        );
        assert_eq!(actual.to_string(), vector["content_id"].as_str().unwrap());
        assert_eq!(
            pseudonym_content(
                &key,
                u32::try_from(vector["epoch"].as_u64().unwrap()).unwrap(),
                actual
            ),
            vector["pseudonym"].as_str().unwrap()
        );
    }
}

#[test]
fn content_pseudonyms_use_the_high_bits_and_keep_the_wire_width() {
    let key = Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
        .unwrap();
    let low = pseudonym_content(&key, 0, 7);
    let high = pseudonym_content(&key, 0, (1u128 << 100) | 7);
    assert_ne!(low, high);
    assert_eq!(high.len(), 32);
}

#[test]
fn scanner_detects_a_full_width_raw_identity_in_nested_output() {
    let raw = (1u128 << 100) | 7;
    let value = serde_json::json!({"unexpected": {"identity": raw.to_string()}});
    assert!(infertap::sink::assert_no_raw(&value, &[raw], &[], &[]).is_err());
}
