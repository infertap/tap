// SPDX-License-Identifier: Apache-2.0
//! `MemorySink`: a bounded in-RAM record sample plus exact per-kind counts. For tests and
//! short runs; a long run streams to disk or an exporter instead.

use crate::record::RecordData;

use std::collections::BTreeMap;

use serde_json::Value;

use crate::sink::Sink;

/// Bounded in-RAM record sample + exact per-kind counts.
///
/// The sample holds each record as its JSON value, the shape a test compares against a
/// corpus. `cap` bounds memory on a long run; counts stay exact regardless (the aggregate
/// numbers never depend on the sample).
#[derive(Debug, Default)]
pub struct MemorySink {
    /// Sample bound; `None` keeps every record.
    pub cap: Option<usize>,
    /// The sample, up to `cap` records, as JSON values.
    pub records: Vec<Value>,
    /// Exact per-kind counts, sample or not, keyed by the record's wire name.
    pub counts: BTreeMap<&'static str, u64>,
}

impl MemorySink {
    /// A sink holding at most `cap` records in RAM, or all of them.
    #[must_use]
    pub const fn new(cap: Option<usize>) -> Self {
        Self {
            cap,
            records: Vec::new(),
            counts: BTreeMap::new(),
        }
    }
}

impl<P: crate::record::Phase> Sink<RecordData<P>> for MemorySink {
    fn emit(&mut self, record: RecordData<P>) {
        *self.counts.entry(record.kind()).or_insert(0) += 1;
        if self.cap.is_none_or(|c| self.records.len() < c) {
            self.records.push(record.to_value());
        }
    }
}
