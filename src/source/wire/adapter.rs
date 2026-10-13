// SPDX-License-Identifier: Apache-2.0
//! Payload decoding and event normalization for the shared wire.
//!
//! [`decode_payload`] decodes one `MessagePack` value and rejects trailing bytes.
//! [`normalize_batch`] maps named event fields to the internal event model under an
//! engine's [`Profile`]. Each stored run expands to one event per block hash. Token IDs
//! are used for content identity derivation, then discarded.
//!
//! Everything engine-specific goes through [`Profile`]: which fields are known, how a
//! hash is read, where the cache salt enters the chain, which tiers derive, and what a
//! store means. The engine modules implement it beside their own tests.
//!
//! Framing is handled by [`super::framing`]; socket access by [`super::transport`].

pub(crate) mod names;
use names::field;

use serde_json::Value;

use crate::clock::EngineMs;
use crate::event_model::{Cleared, Event, Origin, Parent, Removed, Stored, Term};
use crate::record::ReuseReporting;
use crate::source::wire::content::{chain, BlockInput, ContentBridge, ROOT};

/// One event's fields, as decoded.
pub type Fields = serde_json::Map<String, Value>;

/// What differs per engine on the shared wire. [`normalize_batch`] decodes under one of
/// these; each engine module implements it once, as a unit struct, beside the tests for
/// its own differences.
pub trait Profile: std::fmt::Debug + Send + Sync {
    /// The fields this build reads on `kind`, or `None` for a kind it does not model.
    /// Anything else on a known kind is reported as drift.
    fn known_fields(&self, kind: &str) -> Option<&'static [&'static str]>;

    /// What a `store` means on this engine's stream (contract section 2.3). A fact
    /// about the engine, not the traffic, so it is a constant per profile.
    fn reuse_reporting(&self) -> ReuseReporting;

    /// An engine block hash from its wire value; `what` names it in the error.
    ///
    /// # Errors
    /// The value is not a hash on this engine's wire.
    fn block_hash(&self, value: &Value, what: &'static str) -> Result<u64, DecodeError>;

    /// The extra keys of block `i` of a stored run whose first block's parent is
    /// `event_parent`: what the engine folded into that block's hash beyond its tokens.
    fn extra_keys(&self, event: &Fields, i: usize, event_parent: Parent) -> Option<Vec<String>>;

    /// Whether a store on a tier other than `GPU` derives an identity from its tokens.
    /// An engine whose offload announcements can omit extra keys must say `false` and
    /// let those stores reuse the identity the GPU store established.
    fn derives_on_offload_tiers(&self) -> bool;
}

/// An unsigned 64-bit wire value, for a profile whose hashes are unsigned.
///
/// # Errors
/// [`DecodeError::NotUnsigned`] naming `what`.
pub fn unsigned(value: &Value, what: &'static str) -> Result<u64, DecodeError> {
    value.as_u64().ok_or(DecodeError::NotUnsigned { what })
}

/// A malformed payload or event: an unparseable payload or schema drift surfaced as an error,
/// never a silent mis-decode. The first two are the decode stage (the bytes are not one
/// msgpack value); the rest are the normalize stage (the value is not the pinned schema).
/// `conformance/vllm-wire` and `conformance/sglang-wire` pin the text of each.
#[derive(Debug)]
pub enum DecodeError {
    /// The payload is not valid msgpack.
    Msgpack(rmp_serde::decode::Error),
    /// This many bytes follow the first complete value.
    TrailingBytes(usize),
    /// The batch is not an array.
    BatchNotArray,
    /// `batch[0]`, the timestamp, is missing or not a number.
    TsMissing,
    /// `batch[1]`, the event list, is missing or not an array.
    EventsMissing,
    /// An event is not a map.
    EventNotMap,
    /// An event has no string `type`.
    TypeMissing,
    /// The named value must be an unsigned integer and is not.
    NotUnsigned {
        /// What the value is: a block hash, a rank, a group index.
        what: &'static str,
    },
    /// The named value must fit a `u32` and does not; `value` is what arrived.
    NotU32 {
        /// What the value is.
        what: &'static str,
        /// What arrived.
        value: Value,
    },
    /// Backend ownership must be a string when supplied.
    InvalidOwnership,
    /// The named event has no `block_hashes` array.
    NoBlockHashes {
        /// The event kind.
        event: &'static str,
    },
    /// A `BlockStored` has no `block_size`.
    BlockSizeMissing,
    /// A `BlockStored` declares a block size the contract's `int64` column cannot hold.
    BlockSizeOutOfRange(u64),
    /// A `BlockStored` declares a block larger than the tokens it carries.
    BlockSizeExceedsTokens {
        /// The declared block size.
        block_size: u64,
        /// The tokens the event carries.
        tokens: usize,
    },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("wire adapter decode error: ")?;
        match self {
            Self::Msgpack(e) => write!(f, "msgpack decode: {e}"),
            Self::TrailingBytes(n) => {
                write!(
                    f,
                    "msgpack decode: {n} trailing byte(s) after a complete value"
                )
            }
            Self::BatchNotArray => f.write_str("batch is not an array"),
            Self::TsMissing => f.write_str("batch[0] ts missing"),
            Self::EventsMissing => f.write_str("batch[1] events missing"),
            Self::EventNotMap => f.write_str("event is not a map"),
            Self::InvalidOwnership => f.write_str("ownership is not a string"),
            Self::TypeMissing => f.write_str("event has no string \"type\""),
            Self::NotUnsigned { what } => write!(f, "{what} is not an unsigned integer"),
            Self::NotU32 { what, value } => write!(f, "{what} is not a u32: {value}"),
            Self::NoBlockHashes { event } => write!(f, "{event} has no block_hashes array"),
            Self::BlockSizeMissing => f.write_str("BlockStored block_size missing"),
            Self::BlockSizeOutOfRange(n) => {
                write!(f, "BlockStored block_size {n} exceeds the contract's int64")
            }
            Self::BlockSizeExceedsTokens { block_size, tokens } => write!(
                f,
                "BlockStored block_size {block_size} exceeds its {tokens} token(s)"
            ),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Msgpack(e) => Some(e),
            _ => None,
        }
    }
}

/// Normalized batch and counters for skipped events.
///
/// Unknown event kinds are skipped and counted. Malformed known events return an
/// error for the batch.
#[derive(Debug, Default)]
pub struct Normalized {
    /// The normalized events, in wire order. The caller knows which subscription the
    /// batch arrived on; the batch does not repeat it per event.
    pub events: Vec<Event>,
    /// Events of a kind this adapter does not implement, skipped and counted.
    pub unknown_types: u64,
    /// Fields this adapter does not read, as `Kind.field`, on the kinds it does read. The
    /// drift signal for fields, beside `unknown_types` for kinds. A wire that grows a
    /// field, as upstream did with `ownership` after the 0.26.0 capture, or renames one
    /// this adapter depends on shows up here rather than vanishing.
    pub unknown_fields: std::collections::BTreeSet<String>,
}

fn note_unknown_fields(out: &mut Normalized, profile: &dyn Profile, kind: &str, event: &Fields) {
    let Some(known) = profile.known_fields(kind) else {
        return;
    };
    for key in event.keys() {
        if !known.contains(&key.as_str()) {
            out.unknown_fields.insert(format!("{kind}.{key}"));
        }
    }
}

/// Decode one `MessagePack` payload for [`normalize_batch`].
///
/// Decode values without assigning field defaults. Normalization validates the schema.
/// Reject trailing bytes because `rmp_serde` otherwise accepts only the leading value.
///
/// # Errors
/// Returns [`DecodeError`] for invalid `MessagePack`, values without a JSON representation,
/// or trailing bytes after the first complete value.
pub fn decode_payload(bytes: &[u8]) -> Result<Value, DecodeError> {
    use serde::Deserialize as _;
    let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
    let value = Value::deserialize(&mut de).map_err(DecodeError::Msgpack)?;
    let consumed = usize::try_from(de.into_inner().position()).unwrap_or(usize::MAX);
    if consumed != bytes.len() {
        return Err(DecodeError::TrailingBytes(
            bytes.len().saturating_sub(consumed),
        ));
    }
    Ok(value)
}

/// Engine timestamps are float seconds; the model is milliseconds. The capture host's
/// arrival stamps agreed to under a millisecond when `ts` was read as seconds. Converting
/// here keeps one time unit in the model.
fn to_ms(ts_seconds: f64) -> EngineMs {
    EngineMs(ts_seconds * 1000.0)
}

/// The optional parent hash: absent, or a hash read as the profile reads a block hash.
/// A present value that is not a hash is corruption.
fn parent_hash(profile: &dyn Profile, event: &Fields) -> Result<Parent, DecodeError> {
    match event.get(field::PARENT_BLOCK_HASH) {
        None | Some(Value::Null) => Ok(Parent::Root),
        Some(v) => profile.block_hash(v, "parent block hash").map(Parent::Hash),
    }
}

fn hashes<'a>(event: &'a Fields, event_kind: &'static str) -> Result<&'a Vec<Value>, DecodeError> {
    event
        .get(field::BLOCK_HASHES)
        .and_then(Value::as_array)
        .ok_or(DecodeError::NoBlockHashes { event: event_kind })
}

fn tier(event: &Fields) -> Option<Term> {
    event
        .get(field::MEDIUM)
        .and_then(Value::as_str)
        .map(Term::from)
}

/// `LOCAL`/`REMOTE` relative to the publisher, or absent.
///
/// Pass through unknown locality terms without coercion.
fn locality(event: &Fields) -> Option<Term> {
    event
        .get(field::LOCALITY)
        .and_then(Value::as_str)
        .map(Term::from)
}

/// Read an optional rank or group index. Reject invalid values rather than
/// mapping them into the default scope.
fn u32_opt(v: Option<&Value>, what: &'static str) -> Result<Option<u32>, DecodeError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| DecodeError::NotU32 {
                what,
                value: v.clone(),
            }),
    }
}

/// [`u32_opt`] for a field the engine omits only where there is exactly one (single
/// worker, single cache group), which is scope 0 by definition.
fn u32_field(event: &Fields, key: &'static str) -> Result<u32, DecodeError> {
    Ok(u32_opt(event.get(key), key)?.unwrap_or(0))
}

/// The token ids a `BlockStored` carries, as far as identity derivation is concerned.
enum Tokens {
    /// The event has no `token_ids`, or an empty one: the tokenless offload shape.
    Absent,
    /// A list with a value that is not a token id (negative, past `u32`, not a number).
    /// Corruption: the run derives no identity, and it is not read as tokenless either.
    Unusable,
    /// The ids, as the engine sent them.
    Ids(Vec<u32>),
}

impl Tokens {
    fn of(event: &Fields) -> Self {
        let Some(list) = event.get(field::TOKEN_IDS).and_then(Value::as_array) else {
            return Self::Absent;
        };
        if list.is_empty() {
            return Self::Absent;
        }
        list.iter()
            .map(|v| v.as_u64().and_then(|n| u32::try_from(n).ok()))
            .collect::<Option<Vec<u32>>>()
            .map_or(Self::Unusable, Self::Ids)
    }
}

/// Normalize one event batch `[ts, [event, ...], dp_rank?]` into events under the
/// publishing engine's profile. The batch's `dp_rank` (`SGLang` names it `attn_dp_rank`)
/// and each event's `group_idx` are the identity scope within the subscription the
/// caller received the batch on.
///
/// # Errors
/// Returns [`DecodeError`] on a batch or event that is structurally malformed: not an array,
/// an event that is not a map, an untyped event, a non-integer hash. An event whose `type` is
/// merely unimplemented is counted in [`Normalized::unknown_types`], not an error.
pub fn normalize_batch(
    profile: &dyn Profile,
    batch: &Value,
    bridge: &mut ContentBridge,
) -> Result<Normalized, DecodeError> {
    let batch_values = batch.as_array().ok_or(DecodeError::BatchNotArray)?;
    let at_ms = to_ms(
        batch_values
            .first()
            .and_then(Value::as_f64)
            .ok_or(DecodeError::TsMissing)?,
    );
    let events = batch_values
        .get(1)
        .and_then(Value::as_array)
        .ok_or(DecodeError::EventsMissing)?;
    // batch[2] is the data-parallel rank. It was present in all observed traffic. An absent
    // value reads as rank 0 for block events, which is a single-worker engine. The
    // declared-or-absent distinction is kept for `AllBlocksCleared`, where an absent scope
    // means global, never rank 0.
    let dp_declared = u32_opt(batch_values.get(2), "batch[2] dp_rank")?;
    let dp_rank = dp_declared.unwrap_or(0);

    let mut out = Normalized::default();
    for event in events {
        let event = event.as_object().ok_or(DecodeError::EventNotMap)?;
        let kind = event
            .get(field::TYPE)
            .and_then(Value::as_str)
            .ok_or(DecodeError::TypeMissing)?;
        let origin = Origin {
            dp_rank,
            group_idx: u32_field(event, field::GROUP_IDX)?,
        };
        note_unknown_fields(&mut out, profile, kind, event);
        match kind {
            names::BLOCK_STORED => {
                normalize_stored(profile, event, origin, at_ms, bridge, &mut out)?;
            }
            names::BLOCK_REMOVED => {
                normalize_removed(profile, event, origin, at_ms, bridge, &mut out)?;
            }
            names::ALL_BLOCKS_CLEARED => {
                normalize_cleared(event, dp_declared, at_ms, bridge, &mut out)?;
            }
            _ => out.unknown_types += 1,
        }
    }
    Ok(out)
}

/// Expand a stored run into one event per hash. Derive parents for contiguous runs,
/// resolve content identities from complete inputs, and update bridge counts.
fn normalize_stored(
    profile: &dyn Profile,
    event: &Fields,
    origin: Origin,
    at_ms: EngineMs,
    bridge: &mut ContentBridge,
    out: &mut Normalized,
) -> Result<(), DecodeError> {
    let block_hashes = hashes(event, names::BLOCK_STORED)?;
    let event_parent = parent_hash(profile, event)?;
    let block_size = stored_block_size(event)?;
    // List order establishes ancestry only when hashes cover the full token span.
    // Skipped or masked hashes break that mapping. Check the product for overflow
    // before accepting the span or slicing tokens into blocks.
    let token_count = event
        .get(field::TOKEN_IDS)
        .and_then(Value::as_array)
        .map(Vec::len);
    let block_len = usize::try_from(block_size).unwrap_or(0);
    if let Some(n) = token_count {
        if n > 0 && block_len > n {
            return Err(DecodeError::BlockSizeExceedsTokens {
                block_size,
                tokens: n,
            });
        }
    }
    let contiguous = block_len > 0
        && token_count.is_some_and(|n| block_hashes.len().checked_mul(block_len) == Some(n));
    let tier = tier(event);
    let lora_id = event.get(field::LORA_ID).and_then(Value::as_u64);
    let lora_name = event
        .get(field::LORA_NAME)
        .and_then(Value::as_str)
        .map(str::to_string);
    let spec_kind = event
        .get(field::KV_CACHE_SPEC_KIND)
        .and_then(Value::as_str)
        .map(Term::from);
    let spec_sliding_window = event
        .get(field::KV_CACHE_SPEC_SLIDING_WINDOW)
        .and_then(Value::as_i64);
    let locality = locality(event);
    let backend_id = backend_identity(event)?;
    // Read token values only for content derivation; normalized events retain
    // identities and metadata, without token arrays.
    let tokens = Tokens::of(event);
    // Resolve the leading parent once per run. Roots use the fixed root identity;
    // named parents require bridge state; unknown parents leave the seed unresolved.
    let mut parent_content_id: Option<u128> = match event_parent {
        Parent::Root => Some(ROOT),
        Parent::Hash(h) => bridge.resolve(origin, h),
        Parent::Unknown => None,
    };
    let ids = match &tokens {
        Tokens::Ids(ids)
            if contiguous && block_hashes.len().checked_mul(block_len) == Some(ids.len()) =>
        {
            Some(ids.as_slice())
        }
        Tokens::Ids(_) | Tokens::Absent | Tokens::Unusable => None,
    };
    // Tokenless offload announcements reuse an established identity. A malformed
    // token-to-hash span remains unresolved even when the bridge knows its hashes.
    let tokenless = matches!(tokens, Tokens::Absent);
    // Where the profile says an offload announcement may be incomplete, only GPU and
    // legacy untiered announcements establish identities and offload tiers reuse them.
    let offload =
        !profile.derives_on_offload_tiers() && tier.as_deref().is_some_and(|t| t != "GPU");
    for (i, h) in block_hashes.iter().enumerate() {
        // For a contiguous run, each block follows its predecessor. Skipped hashes
        // make the token-to-hash positions ambiguous, including the first parent.
        let parent = stored_parent(profile, contiguous, i, event_parent, block_hashes)?;
        let extra = profile.extra_keys(event, i, event_parent);
        let block_hash = profile.block_hash(h, "block hash")?;
        // An unresolved parent stops derivation for the remaining run. Restarting from
        // root would assign an incorrect identity to a non-root block.
        let derived = derive_content(
            parent_content_id,
            ids.filter(|_| !offload),
            block_len,
            i,
            extra.as_deref(),
        );
        // Only a derivation advances the chain. A bridge-resolved identity does not:
        // it answers for this block from an earlier observation and says nothing
        // about the next one, and a tokenless run has no next block to chain to.
        let accepted = bridge.store(
            origin,
            block_hash,
            (tier.as_deref(), backend_id.as_deref()),
            derived,
            at_ms,
        );
        parent_content_id = derived.and(accepted);
        let content_id = if derived.is_some() || tokenless || (offload && ids.is_some()) {
            accepted
        } else {
            None
        };
        if content_id.is_none() {
            bridge.note_unidentified();
        }
        out.events.push(Event::Stored(Stored {
            origin,
            block_hash,
            parent,
            block_size,
            tier: tier.clone(),
            extra_keys: extra,
            lora_id,
            lora_name: lora_name.clone(),
            spec_kind: spec_kind.clone(),
            spec_sliding_window,
            // Not knowable from this wire: the engine builds a hit and a fresh
            // insertion through one shared builder, so both emit identical shapes.
            // Declared `unlabelled` on the lifecycle record instead.
            reused: None,
            locality: locality.clone(),
            backend_id: backend_id.clone(),
            content_id,
            at_ms,
        }));
    }

    Ok(())
}

/// Decode a block size that fits the record contract's signed integer column.
fn stored_block_size(event: &Fields) -> Result<u64, DecodeError> {
    let block_size = event
        .get(field::BLOCK_SIZE)
        .and_then(Value::as_u64)
        .ok_or(DecodeError::BlockSizeMissing)?;
    if i64::try_from(block_size).is_err() {
        return Err(DecodeError::BlockSizeOutOfRange(block_size));
    }
    Ok(block_size)
}

fn stored_parent(
    profile: &dyn Profile,
    contiguous: bool,
    index: usize,
    event_parent: Parent,
    hashes: &[Value],
) -> Result<Parent, DecodeError> {
    match (contiguous, index) {
        (false, _) => Ok(Parent::Unknown),
        (true, 0) => Ok(event_parent),
        (true, _) => Ok(Parent::Hash(
            profile.block_hash(&hashes[index - 1], "block hash")?,
        )),
    }
}

fn backend_identity(event: &Fields) -> Result<Option<String>, DecodeError> {
    match event.get(field::OWNERSHIP) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(DecodeError::InvalidOwnership),
    }
}

/// Derive one block only when its parent and exact token slice are available.
fn derive_content(
    parent: Option<u128>,
    tokens: Option<&[u32]>,
    block_len: usize,
    index: usize,
    extra_keys: Option<&[String]>,
) -> Option<u128> {
    Some(chain(
        parent?,
        &BlockInput {
            tokens: &tokens?[index * block_len..(index + 1) * block_len],
            extra_keys,
        },
    ))
}

/// One `BlockRemoved`: one `Removed` per hash, each resolved to its content identity through
/// the bridge (a remove carries no tokens) and releasing its tier's residency.
fn normalize_removed(
    profile: &dyn Profile,
    event: &Fields,
    origin: Origin,
    at_ms: EngineMs,
    bridge: &mut ContentBridge,
    out: &mut Normalized,
) -> Result<(), DecodeError> {
    let block_hashes = hashes(event, names::BLOCK_REMOVED)?;
    let tier = tier(event);
    let locality = locality(event);
    let backend_id = backend_identity(event)?;
    for h in block_hashes {
        let block_hash = profile.block_hash(h, "block hash")?;
        // Evictions resolve through retained identities and release one announcement.
        let content_id =
            bridge.remove(origin, block_hash, (tier.as_deref(), backend_id.as_deref()));
        if content_id.is_none() {
            bridge.note_unidentified();
        }
        out.events.push(Event::Removed(Removed {
            origin,
            block_hash,
            tier: tier.clone(),
            locality: locality.clone(),
            backend_id: backend_id.clone(),
            content_id,
            at_ms,
        }));
    }

    Ok(())
}

/// One `AllBlocksCleared`: a single scope-level `Cleared`, transduced as declared. Scope
/// dimensions absent on the wire cover all values in both retained state and the record.
fn normalize_cleared(
    event: &Fields,
    dp_declared: Option<u32>,
    at_ms: EngineMs,
    bridge: &mut ContentBridge,
    out: &mut Normalized,
) -> Result<(), DecodeError> {
    let group_declared = u32_opt(event.get(field::GROUP_IDX), field::GROUP_IDX)?;
    let tier = tier(event);
    bridge.clear_scope(dp_declared, group_declared, tier.as_deref());
    out.events.push(Event::Cleared(Cleared {
        dp_rank: dp_declared,
        group_idx: group_declared,
        tier,
        at_ms,
    }));

    Ok(())
}
