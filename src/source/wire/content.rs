// SPDX-License-Identifier: Apache-2.0
//! Content-derived block identities and bounded engine-hash lookup state.
//!
//! Each identity hashes a fixed domain tag, the parent's content identity, token IDs,
//! and optional extra keys. The root is fixed. Engine hash values and hash settings
//! do not enter this calculation. Equal inputs produce equal identities; comparing
//! their meaning also requires compatible model, tokenizer, and cache configuration.
//!
//! The bridge maps `(dp_rank, group_idx, engine_hash)` to content identity within one
//! source. Stores resolve the leading parent once, then derive the rest of the run.
//! Removes resolve through this map because they carry no tokens.
//!
//! Entries count store announcements by tier and backend. A scoped clear releases
//! that tier across its scope. An omitted scope dimension matches every value.
//! Entries disappear when their last residency count reaches zero. Publisher sequence resets
//! clear the source's bridge. Missing state leaves content identity unresolved.
//!
//! Per-origin and aggregate caps bound retained entries. The pseudonymizer maps these
//! keyless identities before output, unless the operator explicitly selects raw mode.

use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::clock::EngineMs;
use crate::event_model::Origin;

/// Domain separation, versioned like the pseudonymizer's contexts: a construction change is a
/// new tag, never a silent reinterpretation of the same one.
const TAG: &[u8] = b"infertap:content-key:v2\x00";

/// Fixed root for the content-identity chain.
pub const ROOT: u128 = 0;

/// Default per-origin resident-block cap. [`budgeted_cap`] also limits the aggregate
/// allocation across sources and origins to a share of the configured RSS budget.
pub const DEFAULT_RESIDENT_CAP: usize = 1_000_000;

/// Aggregate entry bound per source, expressed as a multiple of its per-origin cap.
/// Wire-controlled origins cannot increase total retained entries beyond this bound.
pub const ORIGINS_PER_SOURCE_BOUND: usize = 4;

/// Refusal windows kept per drain, one per origin. Past this many distinct refusing
/// origins in one interval, which only a publisher inventing origins can reach, further
/// refusals are counted in the aggregate `refused` figure and not windowed, so the map
/// of windows cannot grow with a hostile origin space.
const MAX_REFUSAL_WINDOWS: usize = 1024;

/// Maximum byte length of a retained tier or backend name.
const MAX_SCOPE_NAME_BYTES: usize = 4096;

/// Conservative allocation budget per entry, including hash-table growth and the
/// per-origin count table. Residency counters have fixed storage.
pub const BRIDGE_ENTRY_BYTES: u64 = 512;

/// Clamp the per-origin cap so estimated bridge storage across all sources fits a
/// quarter of the RSS budget. Return at least one entry per origin. A zero RSS bound
/// disables this clamp; the caller reports any reduction of the declared cap.
#[must_use]
pub fn budgeted_cap(declared: usize, sources: usize, max_rss_bytes: u64) -> usize {
    if max_rss_bytes == 0 {
        return declared.max(1);
    }
    let budget = max_rss_bytes / 4;
    let origins = sources.max(1).saturating_mul(ORIGINS_PER_SOURCE_BOUND) as u64;
    let per = budget / (BRIDGE_ENTRY_BYTES * origins);
    let per = usize::try_from(per).unwrap_or(usize::MAX);
    declared.min(per).max(1)
}

/// Maximum distinct residency scopes retained by one source.
const MAX_SCOPES: usize = 32;

/// One block's content identity, and the ingredients a caller must supply.
#[derive(Debug)]
pub struct BlockInput<'a> {
    /// The block's token ids.
    pub tokens: &'a [u32],
    /// The engine's `extra_keys` for this block, where the wire carried one.
    pub extra_keys: Option<&'a [String]>,
}

/// Derive a block's content identity from its parent's identity and block inputs.
///
/// The contract uses the first 128 bits of SHA-256. This is a probabilistic identity:
/// collisions are possible and are not guaranteed to be detected. Under a uniform
/// hash assumption, the probability of at least one collision among N distinct inputs
/// is approximately `1 - exp(-N*(N-1)/2^129)`. Pseudonymization does not increase the
/// underlying identity's entropy. Changing its width requires a contract revision.
///
/// # Panics
/// Panics if SHA-256 produces fewer than sixteen bytes, contrary to its fixed output size.
#[must_use]
pub fn chain(parent: u128, block: &BlockInput) -> u128 {
    let mut h = Sha256::new();
    h.update(TAG);
    h.update(parent.to_be_bytes());
    // Length-prefixed, fixed-width: no delimiter can collide and nothing needs canonicalizing,
    // the same discipline the pseudonymizer's canonical input uses.
    h.update(
        u32::try_from(block.tokens.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for t in block.tokens {
        h.update(t.to_be_bytes());
    }
    match block.extra_keys {
        // Absent and empty differ: the engine folds an absent extra-keys entry and an empty
        // one into different hashes, and a block with a cache_salt must never collide with
        // the same tokens without one.
        None => h.update([0u8]),
        Some(keys) => {
            h.update([1u8]);
            h.update(u32::try_from(keys.len()).unwrap_or(u32::MAX).to_be_bytes());
            for k in keys {
                h.update(u32::try_from(k.len()).unwrap_or(u32::MAX).to_be_bytes());
                h.update(k.as_bytes());
            }
        }
    }
    let d = h.finalize();
    let mut head = [0u8; 16];
    head.copy_from_slice(&d[..16]);
    u128::from_be_bytes(head)
}

/// The root of a fresh chain: a block the engine reported with no parent.
#[must_use]
pub fn chain_from_root(block: &BlockInput) -> u128 {
    chain(ROOT, block)
}

/// A cache location within one publisher, rank, and KV group.
#[derive(Debug, Clone, Copy)]
pub struct ResidencyScope<'a> {
    tier: Option<&'a str>,
    backend: Option<&'a str>,
}

impl<'a> From<Option<&'a str>> for ResidencyScope<'a> {
    fn from(tier: Option<&'a str>) -> Self {
        Self {
            tier,
            backend: None,
        }
    }
}

impl<'a> From<(Option<&'a str>, Option<&'a str>)> for ResidencyScope<'a> {
    fn from((tier, backend): (Option<&'a str>, Option<&'a str>)) -> Self {
        Self { tier, backend }
    }
}

/// An identity and bounded counts of outstanding store announcements.
#[derive(Debug)]
struct Entry {
    content: Option<u128>,
    conflicted: bool,
    resident: [u32; MAX_SCOPES],
}

/// The refusals one origin accumulated since the last drain: how many stores were denied an
/// identity promise at the cap, over which engine-clock window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RefusalWindow {
    /// The first refused store since the last drain.
    pub first_at_ms: EngineMs,
    /// The most recent refused store.
    pub last_at_ms: EngineMs,
    /// Stores refused in the window.
    pub refused: u64,
}

/// Bounded engine-hash lookup state, isolated by publisher, rank, and KV group.
/// Each entry counts outstanding store announcements by tier and backend. Missing
/// events or reuse announcements can leave counts above actual residency; entry caps
/// bound that state. Capacity refusals are counted and windowed per origin.
#[derive(Debug)]
pub struct ContentBridge {
    map: HashMap<(Origin, u64), Entry>,
    /// Entries the whole map may hold, over every origin.
    total_cap: usize,
    /// Entries currently held per origin: the denominator of the per-origin cap check.
    /// Kept beside the map (incremented on insert, decremented wherever an entry leaves)
    /// because deriving it would scan the whole map on every store.
    resident_counts: HashMap<Origin, usize>,
    /// Interned tier and backend pairs, indexed by the entry counters.
    scopes: Vec<(Option<String>, Option<String>)>,
    cap: usize,
    unresolved: u64,
    conflicts: u64,
    refused: u64,
    unscoped_clears: u64,
    refusals: HashMap<Origin, RefusalWindow>,
}

impl ContentBridge {
    /// A bridge holding at most `resident_cap` entries per origin, and four caps in total.
    #[must_use]
    pub fn new(resident_cap: usize) -> Self {
        let cap = resident_cap.max(1);
        Self {
            map: HashMap::new(),
            total_cap: cap.saturating_mul(ORIGINS_PER_SOURCE_BOUND),
            resident_counts: HashMap::new(),
            scopes: Vec::new(),
            cap,
            unresolved: 0,
            conflicts: 0,
            refused: 0,
            unscoped_clears: 0,
            refusals: HashMap::new(),
        }
    }

    /// Intern a residency scope without merging distinct scopes at capacity.
    fn scope_index(&mut self, scope: ResidencyScope<'_>) -> Option<usize> {
        if let Some(index) = self.known_scope_index(scope) {
            return Some(index);
        }
        if self.scopes.len() == MAX_SCOPES
            || [scope.tier, scope.backend]
                .iter()
                .flatten()
                .any(|t| t.len() > MAX_SCOPE_NAME_BYTES)
        {
            return None;
        }
        self.scopes.push((
            scope.tier.map(str::to_owned),
            scope.backend.map(str::to_owned),
        ));
        Some(self.scopes.len() - 1)
    }

    fn known_scope_index(&self, scope: ResidencyScope<'_>) -> Option<usize> {
        self.scopes
            .iter()
            .position(|(t, b)| t.as_deref() == scope.tier && b.as_deref() == scope.backend)
    }

    /// Count a store and return its accepted identity. A contradiction invalidates
    /// the identity until all announcements are removed or the scope is cleared.
    pub fn store<'a>(
        &mut self,
        origin: Origin,
        engine_hash: u64,
        scope: impl Into<ResidencyScope<'a>>,
        content: Option<u128>,
        at_ms: EngineMs,
    ) -> Option<u128> {
        let Some(index) = self.scope_index(scope.into()) else {
            self.refuse(origin, at_ms);
            return None;
        };
        if let Some(e) = self.map.get_mut(&(origin, engine_hash)) {
            // Saturation is sticky until clear: decrements cannot reconstruct the
            // number of stores lost to overflow.
            let overflow = e.resident[index] == u32::MAX - 1;
            e.resident[index] = e.resident[index].saturating_add(1);
            if !e.conflicted {
                if e.content.zip(content).is_some_and(|(a, b)| a != b) {
                    e.conflicted = true;
                    e.content = None;
                    self.conflicts += 1;
                } else if e.content.is_none() {
                    e.content = content;
                }
            }
            let accepted = e.content;
            if overflow {
                self.refuse(origin, at_ms);
            }
            return accepted;
        }
        let at_origin_cap = self.resident_counts.get(&origin).copied().unwrap_or(0) >= self.cap;
        if at_origin_cap || self.map.len() >= self.total_cap {
            self.refuse(origin, at_ms);
            return content;
        }
        *self.resident_counts.entry(origin).or_insert(0) += 1;
        let mut resident = [0; MAX_SCOPES];
        resident[index] = 1;
        self.map.insert(
            (origin, engine_hash),
            Entry {
                content,
                conflicted: false,
                resident,
            },
        );
        content
    }

    /// Count one refused promise, windowed under its origin while the window roster has
    /// room; past [`MAX_REFUSAL_WINDOWS`] origins the refusal counts and is not windowed.
    fn refuse(&mut self, origin: Origin, at_ms: EngineMs) {
        self.refused += 1;
        if let Some(w) = self.refusals.get_mut(&origin) {
            w.last_at_ms = at_ms;
            w.refused += 1;
        } else if self.refusals.len() < MAX_REFUSAL_WINDOWS {
            self.refusals.insert(
                origin,
                RefusalWindow {
                    first_at_ms: at_ms,
                    last_at_ms: at_ms,
                    refused: 1,
                },
            );
        }
    }

    /// Look up a retained content identity without changing residency or counters.
    /// Missing history and capacity refusals can leave the identity absent. The caller
    /// counts each emitted omission through [`Self::note_unidentified`], because one
    /// lookup can seed several records.
    #[must_use]
    pub fn resolve(&self, origin: Origin, engine_hash: u64) -> Option<u128> {
        self.map.get(&(origin, engine_hash)).and_then(|e| e.content)
    }

    /// Count one emitted block record without a content identity, regardless of whether
    /// it resulted from missing state, incomplete inputs, or a contradiction.
    pub const fn note_unidentified(&mut self) {
        self.unresolved += 1;
    }

    /// Resolve an eviction and decrement its residency scope. An eviction with no
    /// tier or backend releases the entry. Saturated counts remain until clear.
    /// An unknown scope releases nothing. The caller counts omitted identities.
    pub fn remove<'a>(
        &mut self,
        origin: Origin,
        engine_hash: u64,
        scope: impl Into<ResidencyScope<'a>>,
    ) -> Option<u128> {
        let scope = scope.into();
        let index = self.known_scope_index(scope);
        let e = self.map.get_mut(&(origin, engine_hash))?;
        let content = e.content;
        if scope.tier.is_none() && scope.backend.is_none() {
            e.resident.fill(0);
        } else if let Some(index) = index {
            if e.resident[index] != u32::MAX {
                e.resident[index] = e.resident[index].saturating_sub(1);
            }
        }
        if e.resident.iter().all(|n| *n == 0) {
            self.map.remove(&(origin, engine_hash));
            Self::forget_one(&mut self.resident_counts, origin);
        }
        content
    }

    /// One entry left the map: its origin holds one fewer promise. An origin at zero is
    /// dropped so a long-lived tap does not accumulate a row per origin ever seen.
    fn forget_one(counts: &mut HashMap<Origin, usize>, origin: Origin) {
        if let Some(n) = counts.get_mut(&origin) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                counts.remove(&origin);
            }
        }
    }

    /// Release all announcements in the declared rank, group, and tier scope.
    /// Missing clear dimensions cover all values, including all backends.
    pub fn clear_scope(
        &mut self,
        dp_rank: Option<u32>,
        group_idx: Option<u32>,
        tier: Option<&str>,
    ) {
        if dp_rank.is_none() || group_idx.is_none() {
            self.unscoped_clears += 1;
        }
        let matching: [bool; MAX_SCOPES] = std::array::from_fn(|i| {
            tier.is_none()
                || self
                    .scopes
                    .get(i)
                    .is_some_and(|(t, _)| t.as_deref() == tier)
        });
        let counts = &mut self.resident_counts;
        self.map.retain(|(o, _), e| {
            if dp_rank.is_some_and(|d| d != o.dp_rank)
                || group_idx.is_some_and(|g| g != o.group_idx)
            {
                return true;
            }
            for (count, matches) in e.resident.iter_mut().zip(matching) {
                if matches {
                    *count = 0;
                }
            }
            if e.resident.iter().any(|n| *n != 0) {
                return true;
            }
            Self::forget_one(counts, *o);
            false
        });
    }

    /// Clear resident identities when the publisher sequence resets.
    /// Subsequent stores repopulate the bridge; unresolved parents remain counted.
    pub fn clear(&mut self) {
        self.map.clear();
        self.resident_counts.clear();
        self.scopes.clear();
    }

    /// Take the accumulated per-origin refusal windows, in origin order, leaving none.
    /// The caller turns each into one scoped `identity_refused` record, so sustained
    /// overflow costs one record per holder per drain rather than one per store.
    pub fn take_refusals(&mut self) -> Vec<(Origin, RefusalWindow)> {
        let mut windows: Vec<_> = std::mem::take(&mut self.refusals).into_iter().collect();
        windows.sort_unstable_by_key(|(origin, _)| *origin);
        windows
    }

    /// Live entries across every origin.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the bridge holds no entry.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Stores whose retained state exceeded an entry, scope, or counter limit.
    #[must_use]
    pub const fn refused(&self) -> u64 {
        self.refused
    }

    /// Conflicting content derivations for an engine hash already held in this origin.
    /// The bridge invalidates the conflicting entry. The closing report includes this
    /// adapter-local counter; it is not a record-stream field.
    #[must_use]
    pub const fn conflicts(&self) -> u64 {
        self.conflicts
    }

    /// Records that left egress with no `content_id`: the declared loss of this mechanism.
    #[must_use]
    pub const fn unresolved(&self) -> u64 {
        self.unresolved
    }

    /// Clears applied with an undeclared scope dimension, released as global rather than
    /// guessed as zero.
    #[must_use]
    pub const fn unscoped_clears(&self) -> u64 {
        self.unscoped_clears
    }
}

impl Default for ContentBridge {
    fn default() -> Self {
        Self::new(DEFAULT_RESIDENT_CAP)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn blk(tokens: &[u32]) -> BlockInput<'_> {
        BlockInput {
            tokens,
            extra_keys: None,
        }
    }

    #[test]
    fn the_same_content_yields_the_same_identity() {
        assert_eq!(
            chain_from_root(&blk(&[1, 2, 3])),
            chain_from_root(&blk(&[1, 2, 3]))
        );
    }

    #[test]
    fn a_different_prefix_yields_a_different_identity() {
        // identity commits to the ancestry, not only to this block's tokens
        let a = chain(chain_from_root(&blk(&[1, 2])), &blk(&[9, 9]));
        let b = chain(chain_from_root(&blk(&[3, 4])), &blk(&[9, 9]));
        assert_ne!(
            a, b,
            "identical tokens under different prefixes must differ"
        );
    }

    #[test]
    fn token_order_is_part_of_the_identity() {
        assert_ne!(
            chain_from_root(&blk(&[1, 2])),
            chain_from_root(&blk(&[2, 1]))
        );
    }

    #[test]
    fn absent_and_empty_extra_keys_are_different() {
        // a salted block must never collide with the same tokens unsalted
        let none = chain(
            0,
            &BlockInput {
                tokens: &[1],
                extra_keys: None,
            },
        );
        let empty = chain(
            0,
            &BlockInput {
                tokens: &[1],
                extra_keys: Some(&[]),
            },
        );
        assert_ne!(none, empty);
    }

    #[test]
    fn a_cache_salt_changes_the_identity() {
        let plain = chain(
            0,
            &BlockInput {
                tokens: &[1],
                extra_keys: None,
            },
        );
        let salted = chain(
            0,
            &BlockInput {
                tokens: &[1],
                extra_keys: Some(&["tenant-AAAA".to_string()]),
            },
        );
        assert_ne!(
            plain, salted,
            "cache_salt partitions the cache; identity must follow"
        );
    }

    #[test]
    fn a_salt_on_the_root_block_partitions_the_whole_chain() {
        // The salt enters where the chain begins and nowhere else: vLLM folds it into
        // the first block's extra keys, and the `SGLang` profile folds its event-level
        // salt into the same place. Every descendant differs through its parent, so one
        // fold at the root separates two tenants' whole chains.
        let child = |root: u128| {
            chain(
                root,
                &BlockInput {
                    tokens: &[2],
                    extra_keys: None,
                },
            )
        };
        let plain_root = chain_from_root(&BlockInput {
            tokens: &[1],
            extra_keys: None,
        });
        let salted_root = chain_from_root(&BlockInput {
            tokens: &[1],
            extra_keys: Some(&["tenant-AAAA".to_string()]),
        });
        assert_ne!(plain_root, salted_root);
        assert_ne!(child(plain_root), child(salted_root));
        // A second salt is a third chain.
        let other_root = chain_from_root(&BlockInput {
            tokens: &[1],
            extra_keys: Some(&["tenant-BBBB".to_string()]),
        });
        assert_ne!(child(salted_root), child(other_root));
    }

    #[test]
    fn extra_key_boundaries_cannot_be_shifted() {
        // length-prefixing is what stops ["ab","c"] and ["a","bc"] colliding
        let ab_c = chain(
            0,
            &BlockInput {
                tokens: &[1],
                extra_keys: Some(&["ab".to_string(), "c".to_string()]),
            },
        );
        let a_bc = chain(
            0,
            &BlockInput {
                tokens: &[1],
                extra_keys: Some(&["a".to_string(), "bc".to_string()]),
            },
        );
        assert_ne!(ab_c, a_bc);
    }

    #[test]
    fn the_root_is_fixed_so_two_processes_agree() {
        // The property the engine's own root lacks. Nothing here reads a clock, an address,
        // or an environment: the same tokens give the same id in any process.
        assert_eq!(
            chain_from_root(&blk(&[7, 7, 7])),
            chain(ROOT, &blk(&[7, 7, 7]))
        );
    }

    fn o(dp: u32) -> Origin {
        Origin {
            dp_rank: dp,
            group_idx: 0,
        }
    }

    const GPU: Option<&str> = Some("GPU");
    const CPU: Option<&str> = Some("CPU");

    fn stored(b: &mut ContentBridge, origin: Origin, hash: u64, tier: Option<&str>, content: u128) {
        b.store(origin, hash, tier, Some(content), EngineMs(1.0));
    }

    #[test]
    fn a_stored_block_resolves_and_an_unseen_one_does_not() {
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        assert_eq!(b.resolve(o(0), 111), Some(999));
        assert_eq!(b.resolve(o(0), 222), None);
    }

    #[test]
    fn lookups_count_nothing_and_omissions_count_one_each() {
        // Contract section 3.1 counts omissions, records leaving with no content_id, not
        // lookups. The units differ: one lookup seeds a whole run, so counting lookups
        // would under-report a run of N by N-1, and a run whose shape denies it a seed
        // performs no lookup at all while emitting nothing identifiable.
        let mut b = ContentBridge::new(16);
        assert_eq!(b.resolve(o(0), 222), None);
        assert_eq!(b.remove(o(0), 222, GPU), None);
        assert_eq!(b.unresolved(), 0, "a lookup is not an omission");
        b.note_unidentified();
        b.note_unidentified();
        assert_eq!(b.unresolved(), 2, "one per record that left without an id");
    }

    #[test]
    fn contradictions_invalidate_both_tiers() {
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        assert_eq!(b.store(o(0), 111, CPU, Some(555), EngineMs(2.0)), None);
        assert_eq!(b.conflicts(), 1);
        assert_eq!(b.remove(o(0), 111, GPU), None);
        assert_eq!(b.remove(o(0), 111, CPU), None);
        assert!(b.is_empty());
    }

    #[test]
    fn a_re_store_agreeing_with_itself_is_not_a_conflict() {
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        stored(&mut b, o(0), 111, GPU, 999);
        b.store(o(0), 111, CPU, None, EngineMs(2.0));
        assert_eq!(b.conflicts(), 0);
    }

    #[test]
    fn holders_do_not_share_entries() {
        // Two data-parallel ranks can hold the same engine hash for the same prefix, and
        // they are different caches evicted independently.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 900);
        assert_eq!(b.resolve(o(1), 111), None, "rank 1 never stored this block");
    }

    #[test]
    fn a_remove_resolves_and_the_entry_dies_with_it() {
        // Removing the last resident tier releases the bridge entry.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        assert_eq!(b.remove(o(0), 111, GPU), Some(999));
        assert!(b.is_empty(), "the resolved remove is the entry's death");
        assert_eq!(b.remove(o(0), 111, GPU), None, "and a second remove misses");
    }

    #[test]
    fn repeated_stores_require_matching_removes() {
        // Distinct cache copies may share the same engine hash.
        let mut b = ContentBridge::new(16);
        for _ in 0..100 {
            stored(&mut b, o(0), 7, GPU, 70);
        }
        assert_eq!(b.len(), 1);
        for _ in 0..100 {
            assert_eq!(b.remove(o(0), 7, GPU), Some(70));
        }
        assert!(b.is_empty());
    }

    #[test]
    fn an_offloaded_copy_survives_the_gpu_evict() {
        // The reason releases are tier-refcounted: a GPU evict with a CPU copy resident must
        // not orphan the CPU tier's later remove.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        stored(&mut b, o(0), 111, CPU, 999);
        assert_eq!(b.remove(o(0), 111, GPU), Some(999));
        assert_eq!(b.len(), 1, "the CPU copy is still resident");
        assert_eq!(b.remove(o(0), 111, CPU), Some(999));
        assert!(b.is_empty(), "the last tier's remove is the release");
        assert_eq!(b.unresolved(), 0);
    }

    #[test]
    fn a_remove_naming_no_tier_releases_the_whole_entry() {
        // An undeclared dimension is unbounded: the block left, the wire did not say from
        // where, and holding residency the engine no longer has would present as working
        // while resolving nothing.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        stored(&mut b, o(0), 111, CPU, 999);
        assert_eq!(b.remove(o(0), 111, None), Some(999));
        assert!(b.is_empty());
    }

    #[test]
    fn a_remove_naming_an_unstored_tier_resolves_but_releases_nothing() {
        // A resolved identity can be returned even if this tier has no announcement to release.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        assert_eq!(b.remove(o(0), 111, CPU), Some(999));
        assert_eq!(b.len(), 1, "GPU residency stands until GPU's own remove");
    }

    #[test]
    fn a_store_that_derived_nothing_still_marks_residency_but_promises_nothing() {
        let mut b = ContentBridge::new(16);
        // No entry exists and no content was derived: nothing a remove could resolve.
        b.store(o(0), 5, GPU, None, EngineMs(1.0));
        assert_eq!(b.resolve(o(0), 5), None);
        assert_eq!(b.remove(o(0), 5, GPU), None);
        assert!(b.is_empty());
        // An entry exists: the re-store marks the tier even without a fresh derivation,
        // because the block is resident and its remove is coming.
        stored(&mut b, o(0), 111, GPU, 999);
        b.store(o(0), 111, CPU, None, EngineMs(1.0));
        assert_eq!(b.remove(o(0), 111, GPU), Some(999));
        assert_eq!(
            b.remove(o(0), 111, CPU),
            Some(999),
            "CPU residency was marked"
        );
        assert!(b.is_empty());
    }

    #[test]
    fn a_scoped_clear_leaves_the_population_at_the_new_resident_set() {
        // Clear, then assert that the map population equals the resident set.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 1, GPU, 10);
        stored(&mut b, o(0), 2, GPU, 20);
        stored(&mut b, o(1), 3, GPU, 30); // another rank: outside the clear's scope
        b.clear_scope(Some(0), Some(0), GPU);
        assert_eq!(b.len(), 1, "rank 0 is empty; rank 1 is untouched");
        assert_eq!(b.resolve(o(1), 3), Some(30));
        assert_eq!(b.resolve(o(0), 1), None, "cleared, so a miss, and counted");
        // Recurring content re-stores after the clear and overwrites nothing stale: the
        // entry it makes is the resident set.
        stored(&mut b, o(0), 1, GPU, 10);
        assert_eq!(b.len(), 2);
        assert_eq!(b.remove(o(0), 1, GPU), Some(10));
    }

    #[test]
    fn a_clear_is_a_bulk_evict_for_one_tier_not_a_third_removal_rule() {
        // A GPU clear with a CPU copy resident must release GPU's residency, not the entry.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        stored(&mut b, o(0), 111, CPU, 999);
        b.clear_scope(Some(0), Some(0), GPU);
        assert_eq!(b.len(), 1, "the CPU copy is still resident");
        assert_eq!(b.remove(o(0), 111, CPU), Some(999));
        assert!(b.is_empty());
        assert_eq!(b.unresolved(), 0);
    }

    #[test]
    fn an_unscoped_clear_releases_across_all_origins_and_is_counted() {
        // A scope the wire did not declare is global for the named tier, never zero.
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 1, GPU, 10);
        stored(&mut b, o(1), 2, GPU, 20);
        stored(&mut b, o(1), 3, CPU, 30); // another tier: outside the clear
        b.clear_scope(None, None, GPU);
        assert_eq!(b.len(), 1, "every origin's GPU residency released");
        assert_eq!(b.resolve(o(1), 3), Some(30));
        assert_eq!(b.unscoped_clears(), 1, "the assumed scope is counted");
    }

    #[test]
    fn a_clear_naming_no_tier_releases_every_tier() {
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 1, GPU, 10);
        stored(&mut b, o(0), 1, CPU, 10);
        stored(&mut b, o(0), 2, None, 20); // an undeclared-tier engine, cleared like any other
        b.clear_scope(Some(0), Some(0), None);
        assert!(b.is_empty());
    }

    #[test]
    fn a_clear_naming_an_unstored_tier_releases_nothing() {
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 1, GPU, 10);
        b.clear_scope(Some(0), Some(0), Some("DISK"));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn at_the_cap_new_promises_are_refused_and_old_ones_are_kept() {
        // The cap refuses new promises rather than breaking existing ones, so entries still
        // die when the engine evicts the block, and the loss lands where it is declared.
        let mut b = ContentBridge::new(2);
        stored(&mut b, o(0), 1, GPU, 10);
        stored(&mut b, o(0), 2, GPU, 20);
        b.store(o(0), 3, GPU, Some(30), EngineMs(5.0));
        assert_eq!(b.len(), 2);
        assert_eq!(b.refused(), 1);
        assert_eq!(
            b.resolve(o(0), 1),
            Some(10),
            "existing promises still resolve"
        );
        assert_eq!(
            b.remove(o(0), 3, GPU),
            None,
            "the refused block resolves to nothing"
        );
        // a re-store and a tier update of existing entries are not new promises
        stored(&mut b, o(0), 1, GPU, 10);
        b.store(o(0), 2, CPU, None, EngineMs(6.0));
        assert_eq!(b.len(), 2);
        assert_eq!(b.refused(), 1);
        // Both outstanding stores must be removed to free the slot.
        assert_eq!(b.remove(o(0), 1, GPU), Some(10));
        assert_eq!(b.remove(o(0), 1, GPU), Some(10));
        b.store(o(0), 3, GPU, Some(30), EngineMs(7.0));
        assert_eq!(b.resolve(o(0), 3), Some(30));
    }

    #[test]
    fn the_budgeted_cap_pairs_with_the_guard_it_must_fit_under() {
        // Per-origin semantics multiply the ceiling by the origin count, so the enforced
        // cap derives from the RSS bound: a quarter of it, divided over origins times
        // entry bytes. The declared cap survives only where it fits. Exact values, so the
        // budget arithmetic cannot drift.
        const GUARD: u64 = 768 * 1024 * 1024;
        assert_eq!(budgeted_cap(1_000_000, 1, GUARD), 98_304);
        assert_eq!(budgeted_cap(1_000_000, 8, GUARD), 12_288);
        assert_eq!(budgeted_cap(1_000_000, 27, GUARD), 3_640);
        assert_eq!(
            budgeted_cap(2_000, 27, GUARD),
            2_000,
            "a declaration under the budget is honored"
        );
        assert_eq!(
            budgeted_cap(1_000_000, 27, 0),
            1_000_000,
            "no bound, no clamp"
        );
    }

    #[test]
    fn the_bridge_total_is_bounded_against_a_publisher_that_invents_origins() {
        // Origins come off the wire. A per-origin cap alone lets a publisher that varies
        // group_idx per store allocate origins × cap; the total holds the whole map at
        // ORIGINS_PER_SOURCE_BOUND caps' worth, and every store past it is a counted,
        // windowed refusal like any other.
        let mut b = ContentBridge::new(2);
        let total = 2 * ORIGINS_PER_SOURCE_BOUND;
        let total_u32 = u32::try_from(total).expect("small");
        for g in 0..(total_u32 + 3) {
            let origin = Origin {
                dp_rank: 0,
                group_idx: g,
            };
            b.store(
                origin,
                u64::from(g) + 1,
                GPU,
                Some(100 + u128::from(g)),
                EngineMs(1.0),
            );
        }
        assert_eq!(b.len(), total, "the map stops at the total bound");
        assert_eq!(b.refused(), 3, "every store past it is refused and counted");
        let windows = b.take_refusals();
        assert_eq!(windows.len(), 3, "each refusing origin gets its own window");
        // Four origins at the per-origin cap exactly fill the aggregate bound.
        let mut b = ContentBridge::new(2);
        for g in 0..u32::try_from(ORIGINS_PER_SOURCE_BOUND).expect("small") {
            for h in 0..2u64 {
                b.store(
                    Origin {
                        dp_rank: 0,
                        group_idx: g,
                    },
                    h + 1,
                    GPU,
                    Some(u128::from(h)),
                    EngineMs(1.0),
                );
            }
        }
        assert_eq!(b.refused(), 0);
    }

    #[test]
    fn refusal_windows_stop_growing_past_the_roster_but_refusals_still_count() {
        let mut b = ContentBridge::new(1);
        // Fill the total so every later store is refused, from a fresh origin each time.
        for g in 0..u32::try_from(ORIGINS_PER_SOURCE_BOUND).expect("small") {
            b.store(
                Origin {
                    dp_rank: 0,
                    group_idx: g,
                },
                1,
                GPU,
                Some(1),
                EngineMs(1.0),
            );
        }
        let flood = u32::try_from(MAX_REFUSAL_WINDOWS).expect("small") + 50;
        for g in 0..flood {
            b.store(
                Origin {
                    dp_rank: 1,
                    group_idx: g,
                },
                1,
                GPU,
                Some(1),
                EngineMs(2.0),
            );
        }
        assert_eq!(b.refused(), u64::from(flood));
        assert_eq!(b.take_refusals().len(), MAX_REFUSAL_WINDOWS);
    }

    #[test]
    fn each_origin_gets_its_whole_declared_capacity() {
        // The cap is per origin, as `--resident-blocks` declares it. A whole-map cap would
        // give N origins about 1/N each: stores past the shared cap would be refused an
        // identity, and their removes would then resolve to nothing.
        let mut b = ContentBridge::new(1);
        stored(&mut b, o(0), 1, GPU, 10);
        stored(&mut b, o(1), 2, GPU, 20);
        assert_eq!(
            b.refused(),
            0,
            "a second origin is not the first one's overflow"
        );
        assert_eq!(b.resolve(o(1), 2), Some(20));
        // an evict frees the origin's own slot, and only that origin's
        assert_eq!(b.remove(o(0), 1, GPU), Some(10));
        stored(&mut b, o(0), 3, GPU, 30);
        b.store(o(1), 4, GPU, Some(40), EngineMs(9.0));
        assert_eq!(b.refused(), 1, "o(1) is still at its own cap");
    }

    #[test]
    fn refusals_are_windowed_per_origin_and_drained() {
        let mut b = ContentBridge::new(1);
        stored(&mut b, o(0), 1, GPU, 10);
        stored(&mut b, o(1), 9, GPU, 90);
        b.store(o(0), 2, GPU, Some(20), EngineMs(100.0));
        b.store(o(0), 3, GPU, Some(30), EngineMs(200.0));
        b.store(o(1), 4, GPU, Some(40), EngineMs(150.0));
        let windows = b.take_refusals();
        assert_eq!(
            windows,
            vec![
                (
                    o(0),
                    RefusalWindow {
                        first_at_ms: EngineMs(100.0),
                        last_at_ms: EngineMs(200.0),
                        refused: 2
                    }
                ),
                (
                    o(1),
                    RefusalWindow {
                        first_at_ms: EngineMs(150.0),
                        last_at_ms: EngineMs(150.0),
                        refused: 1
                    }
                ),
            ],
            "one window per origin, in origin order"
        );
        assert!(b.take_refusals().is_empty(), "drained means drained");
        assert_eq!(b.refused(), 3, "the cumulative counter survives the drain");
    }

    #[test]
    fn a_publisher_restart_empties_the_bridge() {
        let mut b = ContentBridge::new(16);
        stored(&mut b, o(0), 111, GPU, 999);
        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.resolve(o(0), 111), None);
    }

    #[test]
    fn overflow_is_sticky_and_a_clear_releases_it() {
        let mut b = ContentBridge::new(1);
        stored(&mut b, o(0), 1, GPU, 10);
        b.map.get_mut(&(o(0), 1)).unwrap().resident[0] = u32::MAX - 1;
        stored(&mut b, o(0), 1, GPU, 10);
        assert_eq!(b.refused(), 1);
        for _ in 0..3 {
            assert_eq!(b.remove(o(0), 1, GPU), Some(10));
        }
        assert_eq!(b.len(), 1);
        b.clear_scope(Some(0), Some(0), GPU);
        assert!(b.is_empty());
    }

    #[test]
    fn scope_capacity_never_aliases_distinct_backends() {
        let mut b = ContentBridge::new(1);
        for i in 0..MAX_SCOPES {
            b.store(
                o(0),
                1,
                (GPU, Some(i.to_string().as_str())),
                Some(10),
                EngineMs(1.0),
            );
        }
        assert_eq!(
            b.store(o(0), 1, (GPU, Some("overflow")), Some(10), EngineMs(2.0)),
            None
        );
        assert_eq!(b.scopes.len(), MAX_SCOPES);
        assert_eq!(b.refused(), 1);
        b.remove(o(0), 1, (GPU, Some("overflow")));
        for i in 0..MAX_SCOPES {
            assert_eq!(
                b.remove(o(0), 1, (GPU, Some(i.to_string().as_str()))),
                Some(10)
            );
        }
        assert!(b.is_empty());
    }

    #[test]
    fn tier_clear_releases_every_backend_but_preserves_other_tiers() {
        let mut b = ContentBridge::new(4);
        stored(&mut b, o(0), 1, GPU, 10);
        for backend in ["a", "b"] {
            for _ in 0..3 {
                b.store(o(0), 1, (CPU, Some(backend)), None, EngineMs(1.0));
            }
        }
        b.clear_scope(Some(0), Some(0), CPU);
        assert_eq!(b.remove(o(0), 1, GPU), Some(10));
        assert!(b.is_empty());
    }

    #[test]
    fn unresolved_announcements_are_bounded_and_recover_after_a_complete_store() {
        let mut b = ContentBridge::new(1);
        b.store(o(0), 1, CPU, None, EngineMs(1.0));
        b.store(o(0), 2, CPU, None, EngineMs(1.0));
        assert_eq!(b.len(), 1);
        assert_eq!(b.refused(), 1);
        stored(&mut b, o(0), 1, GPU, 10);
        assert_eq!(b.remove(o(0), 1, GPU), Some(10));
        assert_eq!(b.remove(o(0), 1, CPU), Some(10));
        assert!(b.is_empty());
    }

    #[test]
    fn the_entry_budget_covers_table_growth_and_origin_accounting() {
        let slot_bytes = std::mem::size_of::<((Origin, u64), Entry)>()
            + std::mem::size_of::<(Origin, usize)>()
            + 2;
        // A doubling table at 7/8 load can reserve 16/7 slots per live entry.
        assert!(slot_bytes * 16 <= usize::try_from(BRIDGE_ENTRY_BYTES).unwrap() * 7);
    }
}
