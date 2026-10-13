// SPDX-License-Identifier: Apache-2.0
//! Internal normalized cache events.
//!
//! Engine hashes are unsigned 64-bit values scoped by [`Origin`]. Content identities
//! retain 128 bits. Engine timestamps are converted to milliseconds at ingest.
//! Adapters discard token IDs before constructing these events.

use crate::clock::{EngineMs, ProducerMs};

shared_text! {
    /// A term from the engine's vocabulary: a cache tier, a KV cache spec kind, a locality.
    /// Carried as the wire spelled it. Shared by reference count, because a store names it
    /// once for every block in a run and each record repeats it.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    Term
}

impl std::ops::Deref for Term {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Scope of an engine block hash.
///
/// Data-parallel ranks hold independent caches. KV groups distinguish cache groups
/// within a worker. Include both dimensions when indexing engine hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct Origin {
    /// Data-parallel rank of the emitting worker.
    pub dp_rank: u32,
    /// KV cache group index within that worker.
    pub group_idx: u32,
}

/// A block's prefix parent as determined by the engine event.
///
/// An absent parent identifies a root only when the event establishes that state.
/// Use [`Parent::Unknown`] when skipped blocks prevent deriving the parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Parent {
    /// The event carried no parent: this block begins a chain.
    Root,
    /// The event named it.
    Hash(u64),
    /// The event does not establish a parent, for example because hashes were skipped
    /// within the token span.
    Unknown,
}

/// A normalized cache event. Each variant wraps its own struct so constructors, matches,
/// and tests name the fields once. Every variant carries an engine-clock instant.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A full block was (re)computed and inserted into the cache.
    Stored(Stored),
    /// A block was evicted/dropped from a tier.
    Removed(Removed),
    /// A tier was fully cleared (flush, restart, session close).
    Cleared(Cleared),
    /// The producer observed this source's publisher restart.
    HolderReset(HolderReset),
}

impl Event {
    /// The engine-clock instant a reader orders the event at. For a
    /// [`HolderReset`] that is its `boundary_ms`; its producer-clock detection instant is
    /// a different domain and has its own field.
    #[must_use]
    pub const fn engine_ms(&self) -> EngineMs {
        match self {
            Self::Stored(e) => e.at_ms,
            Self::Removed(e) => e.at_ms,
            Self::Cleared(e) => e.at_ms,
            Self::HolderReset(e) => e.boundary_ms,
        }
    }
}

/// A full block was (re)computed and inserted into the cache.
#[derive(Debug, Clone, PartialEq)]
pub struct Stored {
    /// The scope this block's hash is unique within.
    pub origin: Origin,
    /// The engine's hash for the block; an identity only within `origin`.
    pub block_hash: u64,
    /// The prefix parent, per [`Parent`]'s three states.
    ///
    /// For a contiguous stored run, the event names the first block's parent.
    /// Each later block's parent is its predecessor. Skipped hashes prevent this mapping.
    pub parent: Parent,
    /// Tokens in the block, as the engine declared. A count; the adapter refuses a
    /// value the contract's `int64` column cannot hold.
    pub block_size: u64,
    /// The cache tier the block was stored to, as the engine named it; `None` where the wire named none.
    pub tier: Option<Term>,
    /// Extra keys supplied for this block, including an optional cache salt.
    /// Preserve absence on later blocks rather than propagating keys along the chain.
    /// The pseudonymizer maps these operator-controlled strings before output.
    pub extra_keys: Option<Vec<String>>,
    /// `LoRA` adapter this block belongs to, where the engine reports one.
    pub lora_id: Option<u64>,
    /// `LoRA` adapter name, where the engine reports one.
    pub lora_name: Option<String>,
    /// The engine's KV cache spec for the group (e.g. `full_attention`). Names what
    /// `Origin::group_idx` indexes; carried so a consumer can tell groups apart by kind
    /// rather than by position.
    pub spec_kind: Option<Term>,
    /// Whether this store reports reuse rather than a new insertion.
    /// The vLLM adapter leaves this absent because the observed event shape does not
    /// reliably distinguish those cases. Consumers must follow `reuse_reporting`.
    pub reused: Option<bool>,
    /// The group's sliding-window size, where the engine declares one.
    ///
    /// Sliding-window groups can omit masked hashes while retaining the full token
    /// span. Such a run has unresolved parent mapping. Omitted when undeclared.
    pub spec_sliding_window: Option<i64>,
    /// Locality relative to the publisher, such as `LOCAL` or `REMOTE`.
    /// Preserve the engine's declaration. Remote storage may be shared by publishers;
    /// consumers must account for that when calculating residency.
    pub locality: Option<Term>,
    /// Engine-emitted backend identity within this publisher and tier.
    pub backend_id: Option<String>,
    /// Content-derived identity, separate from the engine's block-hash identity space.
    /// Comparisons require compatible model, tokenizer, block boundaries, and extra keys.
    ///
    /// Absent when parent history, contiguous token mapping, or retained identity state
    /// is unavailable. The adapter counts omissions and never substitutes an engine hash.
    pub content_id: Option<u128>,
    /// When the engine stored it.
    pub at_ms: EngineMs,
}

/// A block was evicted/dropped from a tier.
#[derive(Debug, Clone, PartialEq)]
pub struct Removed {
    /// The scope this block's hash is unique within.
    pub origin: Origin,
    /// The engine's hash for the removed block.
    pub block_hash: u64,
    /// The tier the block was removed from; `None` where the wire named none.
    pub tier: Option<Term>,
    /// See [`Stored::locality`]. Present on removes too, so a consumer can close the
    /// interval it opened on the side it opened it.
    pub locality: Option<Term>,
    /// Engine-emitted backend identity within this publisher and tier.
    pub backend_id: Option<String>,
    /// See [`Stored::content_id`]. A remove carries no tokens, so this is resolved through
    /// the bridge built at store time, and is `None` when that lookup misses.
    pub content_id: Option<u128>,
    /// When the engine removed it.
    pub at_ms: EngineMs,
}

/// A tier was fully cleared (flush, restart, session close).
///
/// An absent rank, group, or tier makes the clear global over that dimension.
/// Preserve the engine's declared scope rather than defaulting absent fields to zero.
#[derive(Debug, Clone, PartialEq)]
pub struct Cleared {
    /// Data-parallel rank the clear applies to, or global across ranks when undeclared.
    pub dp_rank: Option<u32>,
    /// KV cache group the clear applies to, or global across groups when undeclared.
    pub group_idx: Option<u32>,
    /// The tier cleared, or global across tiers when undeclared.
    pub tier: Option<Term>,
    /// When the engine cleared it.
    pub at_ms: EngineMs,
}

/// A publisher sequence regression observed by the subscriber.
/// The subscriber emits this event to invalidate retained state for the source.
/// It is synthesized from sequence tracking, not decoded from an engine event.
#[derive(Debug, Clone, PartialEq)]
pub struct HolderReset {
    /// The earliest event instant in the regressed message: where a reader orders the
    /// reset. Ties resolve closes-before-stores, so a store at this exact instant belongs
    /// to the fresh cache and opens a new residency.
    pub boundary_ms: EngineMs,
    /// When the producer detected the regression.
    pub detected_at_ms: ProducerMs,
    /// The worker whose publisher regressed, where the regressed message named exactly
    /// one; absent means the reset covers every rank the source hosts.
    pub dp_rank: Option<u32>,
}
