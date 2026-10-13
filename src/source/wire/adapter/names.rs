// SPDX-License-Identifier: Apache-2.0
//! Event names and field vocabulary shared by decoding and drift detection. The engine
//! modules pick from this vocabulary to declare which fields their wire carries.

pub(crate) const BLOCK_STORED: &str = "BlockStored";
pub(crate) const BLOCK_REMOVED: &str = "BlockRemoved";
pub(crate) const ALL_BLOCKS_CLEARED: &str = "AllBlocksCleared";

pub(crate) mod field {
    pub const TYPE: &str = "type";
    pub const BLOCK_HASHES: &str = "block_hashes";
    pub const PARENT_BLOCK_HASH: &str = "parent_block_hash";
    pub const TOKEN_IDS: &str = "token_ids";
    pub const BLOCK_SIZE: &str = "block_size";
    pub const LORA_ID: &str = "lora_id";
    pub const LORA_NAME: &str = "lora_name";
    pub const MEDIUM: &str = "medium";
    pub const EXTRA_KEYS: &str = "extra_keys";
    pub const GROUP_IDX: &str = "group_idx";
    pub const KV_CACHE_SPEC_KIND: &str = "kv_cache_spec_kind";
    pub const KV_CACHE_SPEC_SLIDING_WINDOW: &str = "kv_cache_spec_sliding_window";
    pub const LOCALITY: &str = "locality";
    pub const OWNERSHIP: &str = "ownership";
    /// `SGLang`: the request's cache salt, once per event rather than per block.
    pub const CACHE_SALT: &str = "cache_salt";
    /// `SGLang`: the session the store is attributed to. Read for drift, never carried.
    pub const SESSION_ID: &str = "session_id";
}
