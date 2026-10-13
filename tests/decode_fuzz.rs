// SPDX-License-Identifier: Apache-2.0
//! Deterministic decoder mutations seeded from the wire corpus.
//!
//! Truncate each payload and mutate each byte. Exercise `MessagePack` length prefixes,
//! multipart arity, and sequence width. Inputs must return a result without panicking;
//! large declared lengths must not trigger proportional preallocation.
//! Fixed mutation patterns make failures reproducible from the corpus.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

use infertap::source::vllm::Vllm;
use infertap::source::wire::content::ContentBridge;
use infertap::source::wire::framing::parse_frame;
use infertap::source::wire::{decode_payload, normalize_batch};
use serde_json::Value;

fn corpus_payloads() -> Vec<Vec<u8>> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/vllm-wire");
    let mut payloads = Vec::new();
    for entry in fs::read_dir(&dir).expect("corpus dir") {
        let path = entry.expect("entry").path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let fx: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read")).expect("json");
        if let Some(hex_str) = fx.get("wire_hex").and_then(Value::as_str) {
            payloads.push(hex::decode(hex_str).expect("wire_hex"));
        }
    }
    assert!(payloads.len() >= 15, "corpus should seed >=15 payloads");
    payloads
}

/// Decode + normalize must return, never panic. The Ok/Err split is the conformance suite's
/// job; here only totality is asserted.
fn must_return(bytes: &[u8]) {
    if let Ok(value) = decode_payload(bytes) {
        let _ = normalize_batch(&Vllm, &value, &mut ContentBridge::default());
    }
}

#[test]
fn every_truncation_of_every_corpus_payload_returns() {
    for payload in corpus_payloads() {
        for cut in 0..payload.len() {
            must_return(&payload[..cut]);
        }
    }
}

#[test]
fn every_single_byte_mutation_of_every_corpus_payload_returns() {
    // Three fixed patterns per position: bit-flip all, flip the top bit (msgpack format
    // family boundary), flip the low bit (length/value off-by-one).
    for payload in corpus_payloads() {
        for i in 0..payload.len() {
            for mask in [0xFF, 0x80, 0x01] {
                let mut mutated = payload.clone();
                mutated[i] ^= mask;
                must_return(&mutated);
            }
        }
    }
}

#[test]
fn length_prefix_bombs_error_without_exhausting_memory() {
    // Claimed-huge collections on tiny buffers: str32 / bin32 / array32 / map32 with a
    // 0xFFFF_FFFF length prefix and (almost) no actual data behind it. Each must come back
    // as a decode error, promptly and without the claimed allocation.
    let bombs: Vec<Vec<u8>> = vec![
        vec![0xdb, 0xFF, 0xFF, 0xFF, 0xFF, b'x'], // str32, claims 4 GiB
        vec![0xc6, 0xFF, 0xFF, 0xFF, 0xFF, 0x00], // bin32, claims 4 GiB
        vec![0xdd, 0xFF, 0xFF, 0xFF, 0xFF, 0xc0], // array32, claims 2^32-1 elements
        vec![0xdf, 0xFF, 0xFF, 0xFF, 0xFF, 0xc0, 0xc0], // map32, claims 2^32-1 pairs
        // nested: an array32 bomb inside a plausible batch shape [ts, <bomb>]
        {
            let mut v = vec![0x92, 0xcb];
            v.extend_from_slice(&1.0f64.to_be_bytes());
            v.extend_from_slice(&[0xdd, 0xFF, 0xFF, 0xFF, 0xFF]);
            v
        },
    ];
    for bomb in &bombs {
        assert!(decode_payload(bomb).is_err(), "bomb {bomb:02x?} must error");
    }
}

#[test]
fn frame_layer_survives_arity_and_width_mutations() {
    let payload = corpus_payloads().pop().expect("one payload");
    let good = vec![b"kv".to_vec(), 7u64.to_be_bytes().to_vec(), payload];
    assert!(parse_frame(&good).is_ok());
    // every arity from 0..=6 except 3 must error, never panic
    for n in [0usize, 1, 2, 4, 5, 6] {
        let parts: Vec<Vec<u8>> = (0..n).map(|_| good[1].clone()).collect();
        assert!(parse_frame(&parts).is_err(), "arity {n} must error");
    }
    // every wrong seq width must error, never panic
    for w in [0usize, 1, 4, 7, 9, 16] {
        let parts = vec![good[0].clone(), vec![0u8; w], good[2].clone()];
        assert!(parse_frame(&parts).is_err(), "seq width {w} must error");
    }
    // truncating the payload part is a decode-layer concern; the frame accepts it whole
    let parts = vec![good[0].clone(), good[1].clone(), Vec::new()];
    assert!(parse_frame(&parts).is_ok()); // empty payload is framed fine; decode rejects it
}
