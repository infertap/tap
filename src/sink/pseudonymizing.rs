// SPDX-License-Identifier: Apache-2.0
//! Map raw records to pseudonymized records before output.
//!
//! [`PseudonymizingSink`] accepts [`Record`] and forwards [`PseudonymizedRecord`].
//! Engine identities, content identities, and [`OperatorText`] use their respective
//! contexts from [`crate::pseudonym`]. Parent and block identities share a context to
//! preserve their relationships. Mapped records carry the configured key epoch.
//!
//! Each mapped record is scanned for unexpected raw values using per-record state.
//! Raw mode bypasses this sink.

use crate::pseudonym::{
    pseudonym, pseudonym_str, Key, BACKEND_ID_CONTEXT, LORA_NAME_CONTEXT, PARTITION_CONTEXT,
};
use crate::record::{Evict, Id, OperatorText, Pseudonym, PseudonymizedRecord, Record, Store};
use crate::sink::noraw::preexisting_string_paths;
use crate::sink::{assert_no_raw, Sink};

/// The active pseudonymization key and epoch.
/// Rotation replaces both. The producer retains no previous key and emits no mapping
/// between key spaces (record contract sections 3.3 and 3.4).
#[derive(Debug)]
pub struct PseudonymConfig {
    /// The key in force.
    pub key: Key,
    /// The epoch folded into every pseudonym and declared on the header.
    pub epoch: u32,
}

/// Replaces a raw record's identities with keyed pseudonyms at the host boundary.
///
/// The inner sink accepts [`PseudonymizedRecord`]. Raw mode bypasses this component.
/// An already-mapped record cannot pass through it again:
///
/// ```compile_fail
/// use infertap::record::PseudonymizedRecord;
/// use infertap::sink::{MemorySink, PseudonymizingSink, Sink};
/// fn remap(sink: &mut PseudonymizingSink<MemorySink>, record: PseudonymizedRecord) {
///     sink.emit(record);
/// }
/// ```
///
/// Pseudonymizing sinks cannot be nested either:
///
/// ```compile_fail
/// use infertap::pseudonym::BLOCK_ID_CONTEXT;
/// use infertap::sink::{MemorySink, PseudonymConfig, PseudonymizingSink};
/// fn nested(config: PseudonymConfig, inner: PseudonymizingSink<MemorySink>) {
///     let _ = PseudonymizingSink::new(config, BLOCK_ID_CONTEXT, inner);
/// }
/// ```
#[derive(Debug)]
pub struct PseudonymizingSink<S> {
    config: PseudonymConfig,
    context: &'static str,
    /// The raw id(s) the current record carried, for the no-raw assertion. Cleared and
    /// refilled per record: it never accumulates, so memory stays O(1) at production rates.
    carried: Vec<u128>,
    /// The raw operator strings this record carried, for the same assertion. Cleared and
    /// refilled per record, like `carried`.
    carried_strs: Vec<String>,
    /// Total values visited by the post-mapping scanner, subject to its exclusions.
    scanned: u64,
    inner: S,
}

/// Fields rewritten by pseudonymization. Raw text at these paths is never
/// exempted as a preexisting occurrence.
const MAPPED_TEXT_FIELDS: [&str; 3] = ["extra_keys", "lora_name", "backend_id"];

impl<S: Sink<PseudonymizedRecord>> PseudonymizingSink<S> {
    /// Wrap `inner`. `context` is the per-purpose domain-separation string for the engine's
    /// ids (for example [`crate::pseudonym::BLOCK_ID_CONTEXT`]); the content identity and
    /// the operator strings carry their own.
    #[must_use]
    pub fn new(config: PseudonymConfig, context: &'static str, inner: S) -> Self {
        Self {
            config,
            context,
            carried: Vec::new(),
            carried_strs: Vec::new(),
            scanned: 0,
            inner,
        }
    }

    /// Values visited by the scanner. A successful scan is subject to the exclusions in `noraw`.
    #[must_use]
    pub const fn scanned_total(&self) -> u64 {
        self.scanned
    }

    /// Derive the canary from this sink's active key and epoch.
    /// Using the sink's configuration keeps the canary aligned with emitted pseudonyms.
    #[must_use]
    pub fn canary(&self) -> String {
        crate::pseudonym::canary(&self.config.key, self.config.epoch)
    }

    /// Unwrap to the inner sink.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Map one raw identity and retain it for the post-mapping scan.
    fn map_id(&mut self, id: Id, context: &'static str) -> Pseudonym {
        self.carried.push(u128::from(id.0));
        Pseudonym(pseudonym(
            &self.config.key,
            context,
            self.config.epoch,
            id.0,
        ))
    }

    fn map_content(&mut self, id: crate::record::ContentId) -> Pseudonym {
        self.carried.push(id.0);
        Pseudonym(crate::pseudonym::pseudonym_content(
            &self.config.key,
            self.config.epoch,
            id.0,
        ))
    }

    /// Map raw operator text under its field's domain-separation context.
    fn map_text(&mut self, text: OperatorText, context: &'static str) -> Pseudonym {
        let mapped = pseudonym_str(&self.config.key, context, self.config.epoch, &text.0);
        self.carried_strs.push(text.0);
        Pseudonym(mapped)
    }

    fn collect_raw_strs(record: &Record) -> Vec<String> {
        let s = match record {
            Record::Store(s) => s,
            Record::Evict(e) => return e.backend_id.iter().map(|t| t.0.clone()).collect(),
            _ => return Vec::new(),
        };
        s.extra_keys
            .iter()
            .flatten()
            .chain(s.lora_name.iter())
            .chain(s.backend_id.iter())
            .map(|t| t.0.clone())
            .collect()
    }

    /// Consume raw records and construct mapped records. Exhaustive field destructuring
    /// requires each new field to be handled explicitly.
    fn map_record(&mut self, record: Record) -> PseudonymizedRecord {
        let engine = self.context;
        match record {
            Record::Store(Store {
                instance_id,
                at_ms,
                seq,
                dp_rank,
                group_idx,
                block_id,
                parent_id,
                parent_unknown,
                n_tokens,
                tier,
                extra_keys,
                lora_id,
                lora_name,
                spec_kind,
                spec_sliding_window,
                reused,
                locality,
                backend_id,
                content_id,
                epoch: _,
            }) => PseudonymizedRecord::Store(Store {
                instance_id,
                at_ms,
                seq,
                dp_rank,
                group_idx,
                block_id: self.map_id(block_id, engine),
                parent_id: parent_id.map(|id| self.map_id(id, engine)),
                parent_unknown,
                n_tokens,
                tier,
                extra_keys: extra_keys.map(|keys| {
                    keys.into_iter()
                        .map(|text| self.map_text(text, PARTITION_CONTEXT))
                        .collect()
                }),
                lora_id,
                lora_name: lora_name.map(|text| self.map_text(text, LORA_NAME_CONTEXT)),
                spec_kind,
                spec_sliding_window,
                reused,
                locality,
                backend_id: backend_id.map(|text| self.map_text(text, BACKEND_ID_CONTEXT)),
                content_id: content_id.map(|id| self.map_content(id)),
                epoch: Some(self.config.epoch),
            }),
            Record::Evict(Evict {
                instance_id,
                at_ms,
                seq,
                dp_rank,
                group_idx,
                block_id,
                tier,
                locality,
                backend_id,
                content_id,
                epoch: _,
            }) => PseudonymizedRecord::Evict(Evict {
                instance_id,
                at_ms,
                seq,
                dp_rank,
                group_idx,
                block_id: self.map_id(block_id, engine),
                tier,
                locality,
                backend_id: backend_id.map(|text| self.map_text(text, BACKEND_ID_CONTEXT)),
                content_id: content_id.map(|id| self.map_content(id)),
                epoch: Some(self.config.epoch),
            }),
            Record::Clear(r) => PseudonymizedRecord::Clear(r),
            Record::HolderReset(r) => PseudonymizedRecord::HolderReset(r),
            Record::AgentStart(r) => PseudonymizedRecord::AgentStart(r),
            Record::Heartbeat(r) => PseudonymizedRecord::Heartbeat(r),
            Record::AgentStop(r) => PseudonymizedRecord::AgentStop(r),
            Record::IdentityRefused(r) => PseudonymizedRecord::IdentityRefused(r),
            Record::SegmentOpen(r) => PseudonymizedRecord::SegmentOpen(r),
            Record::SegmentsDropped(r) => PseudonymizedRecord::SegmentsDropped(r),
            Record::SegmentRecovered(r) => PseudonymizedRecord::SegmentRecovered(r),
        }
    }
}

impl<S: Sink<PseudonymizedRecord>> Sink for PseudonymizingSink<S> {
    fn emit(&mut self, record: Record) {
        self.carried.clear(); // per-record only; never accumulates
        self.carried_strs.clear();
        // Record preexisting occurrences outside mapped fields. Operator text can
        // match public vocabulary such as a kind or tier; these matches are exempt
        // from the post-mapping scan. Mapped fields remain subject to the scan.
        let raw_strs = Self::collect_raw_strs(&record);
        let preexisting = if raw_strs.is_empty() {
            Vec::new()
        } else {
            preexisting_string_paths(&record, &raw_strs, &MAPPED_TEXT_FIELDS)
        };
        let record = self.map_record(record);
        match assert_no_raw(&record, &self.carried, &self.carried_strs, &preexisting) {
            Ok(n) => self.scanned += n as u64,
            // Fail closed: a raw value surviving pseudonymization is a fingerprint leak.
            Err(why) => {
                panic!("infertap raw-value leak in pseudonymized output, refusing to emit: {why}")
            }
        }
        self.inner.emit(record);
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write_error(&self) -> Option<std::io::Error> {
        self.inner.write_error()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{PseudonymConfig, PseudonymizingSink};
    use crate::pseudonym::{Key, BLOCK_ID_CONTEXT};
    use crate::record::{Evict, Id, OperatorText, Record};
    use crate::sink::{MemorySink, Sink};
    use crate::testing::store;
    use serde_json::json;

    fn test_key() -> Key {
        Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
            .unwrap()
    }

    fn config() -> PseudonymConfig {
        PseudonymConfig {
            key: test_key(),
            epoch: 7,
        }
    }

    #[test]
    fn replaces_raw_id_with_pseudonym_and_stamps_epoch() {
        let mut s = PseudonymizingSink::new(config(), BLOCK_ID_CONTEXT, MemorySink::new(None));
        s.emit(Record::Store(store(1)));
        let r = &s.into_inner().records[0];
        assert_eq!(r["block_id"].as_str().unwrap().len(), 32); // 128-bit hex
        assert_ne!(r["block_id"], json!("1")); // not the raw id
        assert_eq!(r["epoch"], json!(7));
    }

    #[test]
    fn parent_pointer_is_pseudonymized_and_the_edge_survives() {
        let mut s = PseudonymizingSink::new(config(), BLOCK_ID_CONTEXT, MemorySink::new(None));
        let mut child = store(9);
        child.parent_id = Some(Id(1));
        s.emit(Record::Store(child));
        s.emit(Record::Evict(Evict {
            instance_id: "i0".into(),
            at_ms: crate::clock::EngineMs(2.0),
            seq: None,
            dp_rank: 0,
            group_idx: 0,
            block_id: Id(1),
            tier: Some("GPU".into()),
            locality: None,
            backend_id: None,
            content_id: None,
            epoch: None,
        }));
        let rs = s.into_inner().records;
        // not the raw id, and 128-bit hex
        assert_ne!(rs[0]["parent_id"], json!("1"));
        assert_eq!(rs[0]["parent_id"].as_str().unwrap().len(), 32);
        // the prefix edge survives: the child's parent pseudonym IS block 1's pseudonym
        assert_eq!(rs[0]["parent_id"], rs[1]["block_id"]);
    }

    #[test]
    fn a_parentless_record_gets_no_parent_field() {
        let mut s = PseudonymizingSink::new(config(), BLOCK_ID_CONTEXT, MemorySink::new(None));
        s.emit(Record::Store(store(1)));
        let r = &s.into_inner().records[0];
        assert!(r.get("parent_id").is_none()); // absent stays absent, not null
    }

    /// The canary uses the active configuration and changes with the key or epoch.
    #[test]
    fn the_canary_reports_the_key_in_force_and_only_that_key() {
        let cur =
            PseudonymizingSink::new(config(), BLOCK_ID_CONTEXT, MemorySink::new(None)).canary();
        // Same key, same epoch, anywhere in the fleet: that is the whole property.
        assert_eq!(cur, crate::pseudonym::canary(&test_key(), 7));
        assert_ne!(
            cur,
            crate::pseudonym::canary(&test_key(), 6),
            "a rotation must move the canary, or it proves nothing about which key is in force"
        );
    }

    /// Every identity a store carries is mapped: the engine's id, its parent, and the
    /// content identity. Typed, so a fourth identity field is an `Id` or does not compile.
    #[test]
    fn every_identity_is_mapped_and_none_survives_in_the_clear() {
        const RAW: u64 = 12_955_354_968_414_679_641; // uniform over u64, like a real hash
        let mut s = PseudonymizingSink::new(config(), BLOCK_ID_CONTEXT, MemorySink::new(None));
        let mut r = store(RAW);
        r.parent_id = Some(Id(RAW));
        r.content_id = Some(crate::record::ContentId(u128::from(RAW)));
        s.emit(Record::Store(r));
        let out = &s.into_inner().records[0];
        for field in ["block_id", "parent_id", "content_id"] {
            assert_ne!(
                out[field],
                json!(RAW.to_string()),
                "{field} left in the clear"
            );
            assert_eq!(
                out[field].as_str().map(str::len),
                Some(32),
                "{field} is not a 128-bit hex pseudonym"
            );
        }
    }

    /// The two identity spaces cannot collide, because they are mapped under different
    /// contexts. This is what makes "never join across them" a property rather than a rule a
    /// consumer has to remember: the engine's id is only comparable inside the process that
    /// computed it, ours is comparable everywhere, and summing them would be meaningless.
    #[test]
    fn the_engine_and_content_spaces_are_domain_separated() {
        const SAME: u64 = 12_955_354_968_414_679_641;
        let mut s = PseudonymizingSink::new(config(), BLOCK_ID_CONTEXT, MemorySink::new(None));
        let mut r = store(SAME);
        r.content_id = Some(crate::record::ContentId(u128::from(SAME)));
        s.emit(Record::Store(r));
        let out = &s.into_inner().records[0];
        assert_ne!(
            out["block_id"], out["content_id"],
            "one value under two contexts must not yield one pseudonym, or a consumer joining \
             the spaces by accident would find spurious matches"
        );
    }

    #[test]
    fn operator_strings_are_mapped_under_their_own_contexts() {
        let cfg = PseudonymConfig {
            key: test_key(),
            epoch: 0,
        };
        let mut s = PseudonymizingSink::new(cfg, BLOCK_ID_CONTEXT, MemorySink::new(None));
        let mut r = store(4_611_686_018_427_387_904);
        r.extra_keys = Some(vec![
            OperatorText("tenant-a".into()),
            OperatorText("42".into()),
        ]);
        r.lora_name = Some(OperatorText("customer-adapter".into()));
        s.emit(Record::Store(r));
        let out = &s.into_inner().records[0];
        let items = out["extra_keys"].as_array().expect("array");
        assert_eq!(items.len(), 2, "every item survives, mapped");
        assert!(items
            .iter()
            .all(|v| v.as_str().is_some_and(|h| h.len() == 32)));
        assert!(out["lora_name"].as_str().is_some_and(|h| h.len() == 32));
        assert_ne!(
            items[0], out["lora_name"],
            "the two dimensions are domain-separated"
        );
    }
}
