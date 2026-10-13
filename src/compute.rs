// SPDX-License-Identifier: Apache-2.0
//! Convert normalized events to raw records without retaining per-block state.
//! The adapter supplies identity and origin metadata. The egress pseudonymizer performs
//! key-dependent mapping. Consumers derive cache classifications from the record stream.

use crate::event_model::{Event, Parent};
use crate::record::{
    AllBlocks, Clear, Evict, HolderReset, Id, InstanceLabel, OperatorText, Record, Store,
};
use crate::sink::Sink;

/// The transducer: one event in, one record out, and a count of events ingested.
#[derive(Debug)]
pub struct Transducer {
    events_ingested: u64,
}

impl Default for Transducer {
    fn default() -> Self {
        Self::new()
    }
}

impl Transducer {
    /// A fresh transducer with nothing ingested.
    #[must_use]
    pub const fn new() -> Self {
        Self { events_ingested: 0 }
    }

    /// Events ingested since construction, the tap-side half of the completeness bracket.
    #[must_use]
    pub const fn events_ingested(&self) -> u64 {
        self.events_ingested
    }

    /// Convert one event to one record without consulting retained residency state.
    ///
    /// Segment headers carry run-level provenance and workload declarations. `seq` is
    /// the source message sequence, shared by all records derived from that message.
    /// Leave it absent when the transport provides no sequence; synthesizing a local
    /// counter would conceal transport gaps.
    pub fn ingest<S: Sink>(
        &mut self,
        instance: &InstanceLabel,
        event: &Event,
        seq: Option<u64>,
        sink: &mut S,
    ) {
        self.events_ingested += 1;
        let instance_id = instance.clone();
        let record = match event {
            Event::Stored(e) => Record::Store(Store {
                instance_id,
                at_ms: e.at_ms,
                seq,
                dp_rank: e.origin.dp_rank,
                group_idx: e.origin.group_idx,
                block_id: Id(e.block_hash),
                // Unknown ancestry uses its own flag; omitting parent_id alone would imply a root.
                parent_id: match e.parent {
                    Parent::Hash(p) => Some(Id(p)),
                    Parent::Root | Parent::Unknown => None,
                },
                parent_unknown: matches!(e.parent, Parent::Unknown),
                n_tokens: e.block_size,
                tier: e.tier.clone(),
                // Operator-controlled text, where cache_salt arrives: mapped at egress.
                extra_keys: e
                    .extra_keys
                    .as_ref()
                    .map(|k| k.iter().cloned().map(OperatorText).collect()),
                lora_id: e.lora_id,
                lora_name: e.lora_name.clone().map(OperatorText),
                spec_kind: e.spec_kind.clone(),
                spec_sliding_window: e.spec_sliding_window,
                // Only ever present under a `labelled` declaration; absent is not "false".
                reused: e.reused,
                // Emitted only where the publisher said something, like `spec_kind` and
                // unlike `tier`: absence is a fact about the publisher.
                locality: e.locality.clone(),
                backend_id: e.backend_id.clone().map(OperatorText),
                // Preserve content identity alongside the engine hash identity.
                content_id: e.content_id.map(crate::record::ContentId),
                epoch: None,
            }),
            Event::Removed(e) => Record::Evict(Evict {
                instance_id,
                at_ms: e.at_ms,
                seq,
                dp_rank: e.origin.dp_rank,
                group_idx: e.origin.group_idx,
                block_id: Id(e.block_hash),
                tier: e.tier.clone(),
                locality: e.locality.clone(),
                backend_id: e.backend_id.clone().map(OperatorText),
                content_id: e.content_id.map(crate::record::ContentId),
                epoch: None,
            }),
            // Emit one clear record with the declared scope. The event does not enumerate
            // blocks. An absent scope dimension covers all values; it is not a zero value.
            Event::Cleared(e) => Record::Clear(Clear {
                instance_id,
                at_ms: e.at_ms,
                seq,
                dp_rank: e.dp_rank,
                group_idx: e.group_idx,
                scope: AllBlocks,
                tier: e.tier.clone(),
            }),
            // Publisher resets apply across KV groups and do not enumerate blocks.
            Event::HolderReset(e) => Record::HolderReset(HolderReset {
                instance_id,
                at_ms: e.detected_at_ms,
                seq,
                dp_rank: e.dp_rank,
                boundary_ms: e.boundary_ms,
            }),
        };
        sink.emit(record);
    }
}
