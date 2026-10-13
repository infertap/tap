// SPDX-License-Identifier: Apache-2.0
//! `SGLang`'s profile of the shared wire: `ZmqEventPublisher` at 0.5.20, captured behind
//! `conformance/sglang-wire`. It speaks vLLM's wire format with four differences: the
//! cache salt rides the event rather than per-block `extra_keys`, `lora_name` is absent,
//! the `medium` vocabulary is its own, and block hashes are signed 64-bit integers.

use serde_json::Value;

use crate::event_model::Parent;
use crate::record::ReuseReporting;
use crate::source::wire::adapter::names::{field, ALL_BLOCKS_CLEARED, BLOCK_REMOVED, BLOCK_STORED};
use crate::source::wire::{unsigned, DecodeError, Fields, Profile};

/// The `SGLang` profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Sglang;

/// `SGLang` v0.5.20, `srt/disaggregation/kv_events.py`: `BlockStored` carries the salt and
/// the session at event level and has no `extra_keys`, `lora_name`, group, spec, locality
/// or ownership fields; `BlockRemoved` carries hashes and medium; `AllBlocksCleared`
/// carries nothing.
const SGLANG_KNOWN_FIELDS: &[(&str, &[&str])] = &[
    (
        BLOCK_STORED,
        &[
            field::TYPE,
            field::BLOCK_HASHES,
            field::PARENT_BLOCK_HASH,
            field::TOKEN_IDS,
            field::BLOCK_SIZE,
            field::LORA_ID,
            field::MEDIUM,
            field::CACHE_SALT,
            field::SESSION_ID,
        ],
    ),
    (
        BLOCK_REMOVED,
        &[field::TYPE, field::BLOCK_HASHES, field::MEDIUM],
    ),
    (ALL_BLOCKS_CLEARED, &[field::TYPE]),
];

impl Profile for Sglang {
    fn known_fields(&self, kind: &str) -> Option<&'static [&'static str]> {
        SGLANG_KNOWN_FIELDS
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, fields)| *fields)
    }

    /// The radix cache records a store only where it creates a node
    /// (`mem_cache/radix_cache.py`, the new-node branch of `_insert_helper`, at v0.5.20).
    /// A prefix hit walks existing nodes and records nothing; a node split records
    /// nothing; the hierarchical cache records a host store on write-through and a GPU
    /// store on load-back, each a block entering that tier. Every store is an insertion
    /// into its tier. Confirmed by the capture behind `conformance/sglang-wire`.
    fn reuse_reporting(&self) -> ReuseReporting {
        ReuseReporting::None
    }

    /// `hash_str_to_int64` folds the leading 64 bits of the engine's SHA-256 into a
    /// signed 64-bit integer, so a negative value is the same 64 bits read as two's
    /// complement. The unsigned value is the block identity, and the hash space stays
    /// one `u64`.
    fn block_hash(&self, value: &Value, what: &'static str) -> Result<u64, DecodeError> {
        match value.as_i64() {
            #[allow(clippy::cast_sign_loss)] // the reinterpretation is the point
            Some(signed) if signed < 0 => Ok(signed as u64),
            _ => unsigned(value, what),
        }
    }

    /// `cache_salt` arrives once per event, as a string. vLLM folds the request's salt
    /// into the first block's `extra_keys` and nothing into later blocks: the salt enters
    /// the hash chain where the chain begins, and every descendant inherits it through
    /// its parent. This profile folds it into the same place, the first block of a rooted
    /// run, so the same content under the same salt yields the same identity on both
    /// engines. An absent or empty salt is no salt; the engine itself folds
    /// `cache_salt or None` into its key.
    fn extra_keys(&self, event: &Fields, i: usize, event_parent: Parent) -> Option<Vec<String>> {
        if i != 0 || event_parent != Parent::Root {
            return None;
        }
        event
            .get(field::CACHE_SALT)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|salt| vec![salt.to_owned()])
    }

    /// The recorder announces every tier through one path, with the same tokens and
    /// salt as the GPU store, so every tier derives.
    fn derives_on_offload_tiers(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    //! The `SGLang` profile's wire differences, each beside the vLLM form it must agree
    //! with. Payloads are the shapes `SGLang` v0.5.20 encodes
    //! (`srt/disaggregation/kv_events.py`, `srt/mem_cache/events.py`).

    use super::Sglang;
    use crate::event_model::{Event, Parent, Stored};
    use crate::source::vllm::Vllm;
    use crate::source::wire::content::ContentBridge;
    use crate::source::wire::{normalize_batch, Normalized};
    use serde_json::{json, Value};

    fn stored(out: &Normalized) -> Vec<&Stored> {
        out.events
            .iter()
            .map(|e| match e {
                Event::Stored(s) => s,
                other => panic!("expected a store: {other:?}"),
            })
            .collect()
    }

    fn sglang(batch: &Value) -> Normalized {
        normalize_batch(&Sglang, batch, &mut ContentBridge::default()).expect("normalizes")
    }

    /// A rooted two-page `SGLang` run, salted or not, as the recorder coalesces it.
    fn sglang_run(salt: Option<&str>) -> Value {
        let mut ev = json!({
            "type": "BlockStored", "block_hashes": [11, 12], "parent_block_hash": null,
            "token_ids": [1, 2, 3, 4, 5, 6, 7, 8], "block_size": 4, "lora_id": null,
            "medium": "GPU"
        });
        if let Some(s) = salt {
            ev["cache_salt"] = json!(s);
        }
        json!([1.5, [ev], 0])
    }

    /// The same run as vLLM publishes it: the salt in the first block's `extra_keys`.
    fn vllm_run(salt: Option<&str>) -> Value {
        let extra = salt.map_or(json!([null, null]), |s| json!([[s], null]));
        json!([1.5, [{
            "type": "BlockStored", "block_hashes": [11, 12], "parent_block_hash": null,
            "token_ids": [1, 2, 3, 4, 5, 6, 7, 8], "block_size": 4, "lora_id": null,
            "medium": "GPU", "lora_name": null, "extra_keys": extra, "group_idx": 0,
            "kv_cache_spec_kind": "full_attention"
        }], 0])
    }

    fn ids(out: &Normalized) -> Vec<Option<u128>> {
        stored(out).iter().map(|s| s.content_id).collect()
    }

    #[test]
    fn an_event_level_salt_enters_the_chain_where_vllm_puts_it() {
        let salted = sglang(&sglang_run(Some("tenant-AAAA")));
        let plain = sglang(&sglang_run(None));
        assert_eq!(
            stored(&salted)[0].extra_keys.as_deref(),
            Some(&["tenant-AAAA".to_string()][..]),
            "the salt rides the first block of a rooted run"
        );
        assert_eq!(
            stored(&salted)[1].extra_keys,
            None,
            "and no later block: descendants inherit it through their parent"
        );
        assert!(ids(&salted).iter().all(Option::is_some));
        assert_ne!(ids(&salted), ids(&plain), "a salt partitions the cache");
        assert_ne!(
            ids(&salted)[1],
            ids(&plain)[1],
            "including every block below the salted root"
        );
        let vllm_salted = normalize_batch(
            &Vllm,
            &vllm_run(Some("tenant-AAAA")),
            &mut ContentBridge::default(),
        )
        .expect("normalizes");
        assert_eq!(
            ids(&salted),
            ids(&vllm_salted),
            "the SGLang form and vLLM's per-block form are one identity"
        );
        assert_eq!(
            stored(&salted)
                .iter()
                .map(|s| s.extra_keys.clone())
                .collect::<Vec<_>>(),
            stored(&vllm_salted)
                .iter()
                .map(|s| s.extra_keys.clone())
                .collect::<Vec<_>>(),
            "and one label"
        );
        let vllm_plain = normalize_batch(&Vllm, &vllm_run(None), &mut ContentBridge::default())
            .expect("normalizes");
        assert_eq!(ids(&plain), ids(&vllm_plain));
    }

    #[test]
    fn an_empty_or_absent_salt_is_no_salt() {
        let empty = sglang(&sglang_run(Some("")));
        let plain = sglang(&sglang_run(None));
        assert_eq!(stored(&empty)[0].extra_keys, None);
        assert_eq!(ids(&empty), ids(&plain));
    }

    #[test]
    fn a_salt_on_a_run_with_a_named_parent_is_not_folded() {
        // vLLM folds the salt only where the chain begins; a run continuing a chain
        // carries none, and the `SGLang` profile matches that shape.
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [12], "parent_block_hash": 11,
            "token_ids": [5, 6, 7, 8], "block_size": 4, "lora_id": null, "medium": "GPU",
            "cache_salt": "tenant-AAAA"
        }], 0]);
        let out = sglang(&batch);
        assert_eq!(stored(&out)[0].extra_keys, None);
        assert_eq!(stored(&out)[0].parent, Parent::Hash(11));
    }

    #[test]
    fn lora_id_is_carried_and_no_name_is_synthesized() {
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [11], "parent_block_hash": null,
            "token_ids": [1, 2, 3, 4], "block_size": 4, "lora_id": 7, "medium": "GPU"
        }], 0]);
        let out = sglang(&batch);
        assert_eq!(stored(&out)[0].lora_id, Some(7));
        assert_eq!(stored(&out)[0].lora_name, None);
        assert!(out.unknown_fields.is_empty());
    }

    #[test]
    fn the_sglang_vocabulary_is_known_on_its_own_wire_and_drift_on_vllms() {
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [11], "parent_block_hash": null,
            "token_ids": [1, 2, 3, 4], "block_size": 4, "lora_id": null, "medium": "GPU",
            "cache_salt": "tenant-AAAA", "session_id": "session-1"
        }], 0]);
        let out = sglang(&batch);
        assert!(out.unknown_fields.is_empty(), "{:?}", out.unknown_fields);
        let out =
            normalize_batch(&Vllm, &batch, &mut ContentBridge::default()).expect("normalizes");
        let drift: Vec<&str> = out.unknown_fields.iter().map(String::as_str).collect();
        assert_eq!(drift, ["BlockStored.cache_salt", "BlockStored.session_id"]);
        // And vLLM's own vocabulary is drift under the `SGLang` profile.
        let out = sglang(&vllm_run(None));
        let drift: Vec<&str> = out.unknown_fields.iter().map(String::as_str).collect();
        assert_eq!(
            drift,
            [
                "BlockStored.extra_keys",
                "BlockStored.group_idx",
                "BlockStored.kv_cache_spec_kind",
                "BlockStored.lora_name"
            ]
        );
    }

    #[test]
    fn the_session_is_never_carried() {
        // The event model has no session field; this pins that the store still
        // normalizes with one present and that nothing of it lands on the event.
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [11], "parent_block_hash": null,
            "token_ids": [1, 2, 3, 4], "block_size": 4, "lora_id": null, "medium": "GPU",
            "session_id": "session-private-canary"
        }], 0]);
        let out = sglang(&batch);
        assert!(!format!("{:?}", out.events).contains("session-private-canary"));
    }

    #[test]
    fn media_pass_through_as_terms_and_every_tier_derives_the_same_identity() {
        let mut bridge = ContentBridge::default();
        let gpu = json!([1.0, [{
            "type": "BlockStored", "block_hashes": [11], "parent_block_hash": null,
            "token_ids": [1, 2, 3, 4], "block_size": 4, "lora_id": null, "medium": "GPU"
        }], 0]);
        let first = normalize_batch(&Sglang, &gpu, &mut bridge).expect("normalizes");
        let id = stored(&first)[0]
            .content_id
            .expect("a rooted GPU store derives");
        for medium in ["CPU_PINNED", "DISK", "EXTERNAL"] {
            // The hierarchical cache's write-through: the same hashes, tokens and salt
            // on another tier. It derives, agrees with the GPU identity, and resolves
            // on its own against a fresh bridge.
            let tiered = json!([2.0, [{
                "type": "BlockStored", "block_hashes": [11], "parent_block_hash": null,
                "token_ids": [1, 2, 3, 4], "block_size": 4, "lora_id": null, "medium": medium
            }], 0]);
            let out = normalize_batch(&Sglang, &tiered, &mut bridge).expect("normalizes");
            let s = stored(&out)[0];
            assert_eq!(s.tier.as_deref(), Some(medium), "the term is not remapped");
            assert_eq!(s.content_id, Some(id));
            assert_eq!(bridge.conflicts(), 0);
            let alone = normalize_batch(&Sglang, &tiered, &mut ContentBridge::default())
                .expect("normalizes");
            assert_eq!(
                stored(&alone)[0].content_id,
                Some(id),
                "derived, not resolved"
            );
        }
        let removed =
            json!([3.0, [{"type": "BlockRemoved", "block_hashes": [11], "medium": "EXTERNAL"}], 0]);
        let out = normalize_batch(&Sglang, &removed, &mut bridge).expect("normalizes");
        let Event::Removed(r) = &out.events[0] else {
            panic!("{out:?}")
        };
        assert_eq!(r.tier.as_deref(), Some("EXTERNAL"));
        assert_eq!(r.content_id, Some(id));
        assert_eq!(r.origin.group_idx, 0, "no group on the wire is group 0");
    }

    #[test]
    fn the_batch_rank_is_attn_dp_rank_and_a_fieldless_clear_is_scoped_by_it_alone() {
        let batch = json!([1.5, [
            {"type": "BlockRemoved", "block_hashes": [11], "medium": "GPU"},
            {"type": "AllBlocksCleared"}
        ], 3]);
        let out = sglang(&batch);
        let Event::Removed(r) = &out.events[0] else {
            panic!("{out:?}")
        };
        assert_eq!(r.origin.dp_rank, 3);
        let Event::Cleared(c) = &out.events[1] else {
            panic!("{out:?}")
        };
        assert_eq!(
            (c.dp_rank, c.group_idx, c.tier.as_deref()),
            (Some(3), None, None)
        );
        assert!(out.unknown_fields.is_empty());
    }

    #[test]
    fn a_negative_hash_is_the_unsigned_hash_in_twos_complement() {
        // SGLang's hash_str_to_int64 maps the leading 64 bits of SHA-256 into i64, so
        // about half of all hashes arrive negative. The profile restores the u64; the
        // vLLM profile keeps refusing a negative hash as corruption.
        let mut bridge = ContentBridge::default();
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [-1, -9_223_372_036_854_775_808_i64],
            "parent_block_hash": -2, "token_ids": [1, 2, 3, 4, 5, 6, 7, 8], "block_size": 4,
            "lora_id": null, "medium": "GPU"
        }], 0]);
        let out = normalize_batch(&Sglang, &batch, &mut bridge).expect("normalizes");
        let s = stored(&out);
        assert_eq!(s[0].block_hash, u64::MAX);
        assert_eq!(s[0].parent, Parent::Hash(u64::MAX - 1));
        assert_eq!(s[1].block_hash, 1 << 63);
        assert_eq!(s[1].parent, Parent::Hash(u64::MAX));
        let removed =
            json!([2.0, [{"type": "BlockRemoved", "block_hashes": [-1], "medium": "GPU"}], 0]);
        let out = normalize_batch(&Sglang, &removed, &mut bridge).expect("normalizes");
        let Event::Removed(r) = &out.events[0] else {
            panic!("{out:?}")
        };
        assert_eq!(r.block_hash, u64::MAX);
        assert!(
            normalize_batch(&Vllm, &batch, &mut ContentBridge::default()).is_err(),
            "vLLM's hashes are unsigned; a negative one is refused"
        );
    }

    #[test]
    fn a_run_whose_block_size_is_not_the_page_size_is_still_contiguous() {
        // The recorder sets block_size to the page's own token count, so the hash count
        // times block_size always covers the tokens, whatever the page size.
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [12], "parent_block_hash": 11,
            "token_ids": [9, 10, 11], "block_size": 3, "lora_id": null, "medium": "GPU"
        }], 0]);
        let out = sglang(&batch);
        assert_eq!(stored(&out)[0].parent, Parent::Hash(11));
        assert_eq!(stored(&out)[0].block_size, 3);
    }

    #[test]
    fn bigram_token_pairs_normalize_without_an_identity() {
        // EAGLE-style keys expose (t_i, t_i+1) pairs as token_ids. Not token ids: the
        // store is kept, no identity is derived, and the omission is counted.
        let mut bridge = ContentBridge::default();
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [11], "parent_block_hash": null,
            "token_ids": [[1, 2], [2, 3]], "block_size": 2, "lora_id": null, "medium": "GPU"
        }], 0]);
        let out = normalize_batch(&Sglang, &batch, &mut bridge).expect("normalizes");
        assert_eq!(stored(&out)[0].content_id, None);
        assert_eq!(bridge.unresolved(), 1);
    }
}
