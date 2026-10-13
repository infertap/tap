// SPDX-License-Identifier: Apache-2.0
//! Constructed vLLM offload payloads exercise identity and residency rules.
//! Placeholder events carry no tokens. Self-describing offload events may omit
//! extra keys, so only complete GPU announcements establish content identity.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use infertap::event_model::{Event, Removed, Stored};
use infertap::source::vllm::Vllm;
use infertap::source::wire::content::ContentBridge;
use infertap::source::wire::normalize_batch;
use serde_json::{json, Value};

const TOKENS: [u32; 16] = [7; 16];
const HASH: u64 = 11_400_714_819_323_198_485;

fn batch(events: &[Value]) -> Value {
    json!([1_785_153_670.0, events, 0])
}

/// The GPU-tier store: the full payload, tokens included.
fn gpu_store() -> Value {
    json!({
        "type": "BlockStored",
        "block_hashes": [HASH],
        "parent_block_hash": null,
        "token_ids": TOKENS,
        "block_size": 16,
        "medium": "GPU",
        "group_idx": 0,
    })
}

/// The offload placeholder, exactly as `_placeholder_stored` builds it.
fn offload_store(tier: &str) -> Value {
    json!({
        "type": "BlockStored",
        "block_hashes": [HASH],
        "parent_block_hash": null,
        "token_ids": [],
        "block_size": 0,
        "medium": tier,
        "group_idx": 0,
        "locality": "LOCAL",
    })
}

fn removed(tier: &str) -> Value {
    json!({
        "type": "BlockRemoved",
        "block_hashes": [HASH],
        "medium": tier,
        "group_idx": 0,
    })
}

fn drive(events: &[Value], bridge: &mut ContentBridge) -> Vec<Event> {
    let b = batch(events);
    normalize_batch(&Vllm, &b, bridge)
        .expect("constructed batch normalizes")
        .events
}

fn content_id(e: &Event) -> Option<u128> {
    match e {
        Event::Stored(Stored { content_id, .. }) | Event::Removed(Removed { content_id, .. }) => {
            *content_id
        }
        Event::Cleared(_) | Event::HolderReset(_) => None,
    }
}

#[test]
fn one_block_on_two_tiers_is_one_identity_on_every_record() {
    let mut bridge = ContentBridge::new(64);
    let events = drive(
        &[
            gpu_store(),
            offload_store("CPU"),
            removed("GPU"),
            removed("CPU"),
        ],
        &mut bridge,
    );
    assert_eq!(events.len(), 4);

    let derived = content_id(&events[0]).expect("the GPU store derives an identity from tokens");
    for (i, e) in events.iter().enumerate() {
        assert_eq!(
            content_id(e),
            Some(derived),
            "record {i} disagrees: content identity is tier-invariant"
        );
    }
    assert_eq!(
        bridge.unresolved(),
        0,
        "nothing was omitted, so nothing is counted"
    );
    assert_eq!(bridge.conflicts(), 0);
}

#[test]
fn the_offload_store_states_what_the_wire_said_about_tokens_and_parent() {
    // Resolving content identity does not establish ancestry. The placeholder
    // keeps parent_unknown and preserves the wire's zero token count.
    let mut bridge = ContentBridge::new(64);
    let events = drive(&[gpu_store(), offload_store("CPU")], &mut bridge);
    let Event::Stored(stored) = &events[1] else {
        panic!("expected a Stored");
    };
    assert_eq!(stored.parent, infertap::event_model::Parent::Unknown);
    assert_eq!(stored.block_size, 0);
    assert_eq!(stored.tier.as_deref(), Some("CPU"));
    assert_eq!(stored.locality.as_deref(), Some("LOCAL"));
}

#[test]
fn an_offload_store_for_a_block_never_seen_stored_is_a_counted_omission() {
    // Without a prior GPU store, the offload placeholder has no content identity.
    // Count the unresolved identity and retain the event.
    let mut bridge = ContentBridge::new(64);
    let events = drive(&[offload_store("CPU")], &mut bridge);
    assert_eq!(content_id(&events[0]), None);
    assert_eq!(bridge.unresolved(), 1);
}

#[test]
fn a_skipped_block_run_is_not_rescued_by_the_bridge_and_every_block_counts() {
    // Tokens present but not lining up with the hashes is the engine skipping blocks
    // (sliding-window and Mamba groups). The chunk-to-hash mapping is unknown, so the bridge
    // cannot establish their mapping from the hash-to-token ratio.
    //
    // Count one omission for every emitted block record.
    let mut bridge = ContentBridge::new(64);
    let events = drive(
        &[json!({
            "type": "BlockStored",
            "block_hashes": [HASH, HASH ^ 1, HASH ^ 2],
            "parent_block_hash": null,
            // three hashes, but a token span that does not divide by them: a block was skipped
            "token_ids": TOKENS,
            "block_size": 16,
            "medium": "GPU",
            "group_idx": 0,
        })],
        &mut bridge,
    );
    assert_eq!(events.len(), 3);
    for e in &events {
        assert_eq!(content_id(e), None);
    }
    assert_eq!(
        bridge.unresolved(),
        3,
        "one per record that left without an id, not one per lookup"
    );
}

#[test]
fn duplicate_announcements_survive_until_the_last_remove() {
    let mut bridge = ContentBridge::new(64);
    let events = drive(
        &[
            gpu_store(),
            offload_store("CPU"),
            offload_store("CPU"),
            removed("GPU"),
            removed("CPU"),
            removed("CPU"),
        ],
        &mut bridge,
    );
    let expected = content_id(&events[0]).unwrap();
    assert!(events.iter().all(|e| content_id(e) == Some(expected)));
    assert!(bridge.is_empty());
    assert_eq!(bridge.unresolved(), 0);
}

fn described_offload() -> Value {
    let mut event = gpu_store();
    event["medium"] = json!("CPU");
    event["extra_keys"] = Value::Null;
    event
}

#[test]
fn offload_without_extra_keys_reuses_the_complete_gpu_identity() {
    let mut gpu = gpu_store();
    gpu["extra_keys"] = json!([["adapter-v1", "cache-salt"]]);
    let mut bridge = ContentBridge::new(64);
    let events = drive(
        &[gpu, described_offload(), removed("GPU"), removed("CPU")],
        &mut bridge,
    );
    let expected = content_id(&events[0]).unwrap();
    assert!(events.iter().all(|e| content_id(e) == Some(expected)));
    assert_eq!(bridge.conflicts(), 0);
}

#[test]
fn cold_offload_does_not_establish_an_identity_from_incomplete_inputs() {
    let mut bridge = ContentBridge::new(64);
    let events = drive(&[described_offload(), removed("CPU")], &mut bridge);
    assert!(events.iter().all(|e| content_id(e).is_none()));
    assert_eq!(bridge.unresolved(), 2);
}

#[test]
fn contradictory_complete_stores_invalidate_identity_until_release() {
    let mut conflicting = gpu_store();
    conflicting["token_ids"] = json!([8; 16].to_vec());
    let mut bridge = ContentBridge::new(64);
    let events = drive(
        &[gpu_store(), conflicting, removed("GPU"), removed("GPU")],
        &mut bridge,
    );
    assert!(content_id(&events[0]).is_some());
    assert!(events[1..].iter().all(|e| content_id(e).is_none()));
    assert_eq!(bridge.conflicts(), 1);
    assert_eq!(bridge.unresolved(), 3);
    assert!(bridge.is_empty());
}

#[test]
fn backend_residencies_are_independent() {
    let mut a = offload_store("STORAGE");
    a["ownership"] = json!("cache-a");
    let mut b = offload_store("STORAGE");
    b["ownership"] = json!("cache-b");
    let mut evict_a = removed("STORAGE");
    evict_a["ownership"] = json!("cache-a");
    let mut evict_b = removed("STORAGE");
    evict_b["ownership"] = json!("cache-b");
    let mut bridge = ContentBridge::new(64);
    let events = drive(
        &[
            gpu_store(),
            a,
            b,
            removed("GPU"),
            evict_a.clone(),
            evict_a,
            evict_b,
        ],
        &mut bridge,
    );
    let expected = content_id(&events[0]).unwrap();
    assert!(events.iter().all(|e| content_id(e) == Some(expected)));
    assert!(bridge.is_empty());
    let Event::Stored(stored) = &events[1] else {
        panic!("store")
    };
    assert_eq!(stored.backend_id.as_deref(), Some("cache-a"));
    let Event::Removed(removed) = &events[6] else {
        panic!("remove")
    };
    assert_eq!(removed.backend_id.as_deref(), Some("cache-b"));
}

#[test]
fn ownership_is_modeled_and_malformed_values_are_refused() {
    let mut event = gpu_store();
    event["ownership"] = json!("cache-a");
    let out = normalize_batch(&Vllm, &batch(&[event.clone()]), &mut ContentBridge::new(4)).unwrap();
    assert!(out.unknown_fields.is_empty());
    event["ownership"] = json!(42);
    assert!(normalize_batch(&Vllm, &batch(&[event]), &mut ContentBridge::new(4)).is_err());
}
