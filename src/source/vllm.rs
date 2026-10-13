// SPDX-License-Identifier: Apache-2.0
//! vLLM's profile of the shared wire: `ZmqEventPublisher` at 0.26.0 and later, the
//! reference producer's capture behind `conformance/vllm-wire`.

use serde_json::Value;

use crate::event_model::Parent;
use crate::record::ReuseReporting;
use crate::source::wire::adapter::names::{field, ALL_BLOCKS_CLEARED, BLOCK_REMOVED, BLOCK_STORED};
use crate::source::wire::{unsigned, DecodeError, Fields, Profile};

/// The vLLM profile. Hashes are unsigned; the cache salt rides the first block's
/// `extra_keys`; offload announcements may omit extra keys, so only GPU stores derive;
/// a store announces an insertion or a reuse in one shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Vllm;

/// vLLM 0.26.0 through 0.30.0: every field the capture showed, plus the `ownership`
/// upstream added after it.
const VLLM_KNOWN_FIELDS: &[(&str, &[&str])] = &[
    (
        BLOCK_STORED,
        &[
            field::TYPE,
            field::BLOCK_HASHES,
            field::PARENT_BLOCK_HASH,
            field::TOKEN_IDS,
            field::BLOCK_SIZE,
            field::LORA_ID,
            field::LORA_NAME,
            field::MEDIUM,
            field::EXTRA_KEYS,
            field::GROUP_IDX,
            field::KV_CACHE_SPEC_KIND,
            field::KV_CACHE_SPEC_SLIDING_WINDOW,
            field::LOCALITY,
            field::OWNERSHIP,
        ],
    ),
    (
        BLOCK_REMOVED,
        &[
            field::TYPE,
            field::BLOCK_HASHES,
            field::MEDIUM,
            field::GROUP_IDX,
            field::LOCALITY,
            field::OWNERSHIP,
        ],
    ),
    (
        ALL_BLOCKS_CLEARED,
        &[field::TYPE, field::GROUP_IDX, field::MEDIUM],
    ),
];

impl Profile for Vllm {
    fn known_fields(&self, kind: &str) -> Option<&'static [&'static str]> {
        VLLM_KNOWN_FIELDS
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, fields)| *fields)
    }

    /// The publisher uses the same store event for insertion and reuse. These wire
    /// events do not provide a reliable reuse label.
    fn reuse_reporting(&self) -> ReuseReporting {
        ReuseReporting::Unlabelled
    }

    /// Hashes are unsigned 64-bit values; a negative one is corruption.
    fn block_hash(&self, value: &Value, what: &'static str) -> Result<u64, DecodeError> {
        unsigned(value, what)
    }

    /// The `extra_keys` entry for one block: `null`, or the list of values the engine
    /// folded into that block's hash. `cache_salt` arrives here as a string, on the
    /// block where the chain begins; a `LoRA` id or a multimodal hash arrives as a
    /// number. A value that is not a string rides as its compact JSON text, so every
    /// entry folds into the content identity and maps at egress the same way, and the
    /// same wire value always yields the same text.
    fn extra_keys(&self, event: &Fields, i: usize, _event_parent: Parent) -> Option<Vec<String>> {
        let entry = event.get(field::EXTRA_KEYS)?.as_array()?.get(i)?;
        let list = entry.as_array()?;
        Some(
            list.iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect(),
        )
    }

    /// Offload publishers may omit extra keys even with tokens present, so an offload
    /// store reuses the identity the GPU store established.
    fn derives_on_offload_tiers(&self) -> bool {
        false
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    //! The vLLM profile's own rules, beside the `SGLang` forms they differ from. The
    //! vendored `vllm-wire` corpus pins the whole mapping; these pin the profile's part.

    use super::Vllm;
    use crate::event_model::{Event, Stored};
    use crate::source::wire::content::ContentBridge;
    use crate::source::wire::{normalize_batch, Normalized, Profile as _};
    use serde_json::json;

    fn stored(out: &Normalized) -> Vec<&Stored> {
        out.events
            .iter()
            .map(|e| match e {
                Event::Stored(s) => s,
                other => panic!("expected a store: {other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_negative_hash_is_corruption_on_this_wire() {
        let err = Vllm
            .block_hash(&json!(-1), "block hash")
            .expect_err("refused");
        assert!(err.to_string().contains("unsigned integer"), "{err}");
        assert_eq!(
            Vllm.block_hash(&json!(u64::MAX), "block hash").unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn extra_keys_are_read_per_block_and_non_strings_ride_as_json_text() {
        // The salt on block 0 and a LoRA id folded as a number on block 1: each block's
        // entry is its own, and a number becomes its compact JSON text.
        let batch = json!([1.5, [{
            "type": "BlockStored", "block_hashes": [11, 12], "parent_block_hash": null,
            "token_ids": [1, 2, 3, 4, 5, 6, 7, 8], "block_size": 4, "lora_id": null,
            "medium": "GPU", "extra_keys": [["tenant-AAAA"], [7]]
        }], 0]);
        let out =
            normalize_batch(&Vllm, &batch, &mut ContentBridge::default()).expect("normalizes");
        let s = stored(&out);
        assert_eq!(
            s[0].extra_keys.as_deref(),
            Some(&["tenant-AAAA".to_string()][..])
        );
        assert_eq!(s[1].extra_keys.as_deref(), Some(&["7".to_string()][..]));
    }

    #[test]
    fn an_offload_store_reuses_the_gpu_identity_rather_than_deriving() {
        // The offload connector may omit extra keys with tokens present, so a CPU
        // announcement never derives: it resolves what the GPU store promised, and on
        // a fresh bridge it resolves nothing.
        let mut bridge = ContentBridge::default();
        let ev = |medium: &str| {
            json!([1.0, [{
                "type": "BlockStored", "block_hashes": [11], "parent_block_hash": null,
                "token_ids": [1, 2, 3, 4], "block_size": 4, "lora_id": null, "medium": medium,
                "extra_keys": [["tenant-AAAA"]]
            }], 0])
        };
        let gpu = normalize_batch(&Vllm, &ev("GPU"), &mut bridge).expect("normalizes");
        let id = stored(&gpu)[0].content_id.expect("derives");
        let cpu = normalize_batch(&Vllm, &ev("CPU"), &mut bridge).expect("normalizes");
        assert_eq!(stored(&cpu)[0].content_id, Some(id));
        let alone =
            normalize_batch(&Vllm, &ev("CPU"), &mut ContentBridge::default()).expect("normalizes");
        assert_eq!(stored(&alone)[0].content_id, None);
        assert!(!Vllm.derives_on_offload_tiers());
    }
}
