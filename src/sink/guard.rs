// SPDX-License-Identifier: Apache-2.0
//! `GuardedSink`: the record size bound.
//!
//! Refuses records larger than the serialized contract limit before delegating to a sink.
//! The check includes the newline. It measures size, not the meaning of text fields.

use crate::record::RecordData;

use crate::sink::Sink;

/// Maximum serialized record size, including the newline (contract section 2.1).
pub const MAX_RECORD_BYTES: usize = 1 << 20;

/// The compact-JSON length of a record, counted rather than materialized.
pub(crate) fn compact_len<P: crate::record::Phase>(record: &RecordData<P>) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    // Treat serialization failure as oversized so check_size rejects the record.
    match serde_json::to_writer(&mut counter, record) {
        Ok(()) => counter.0,
        Err(_) => usize::MAX,
    }
}

/// Check the complete serialized line, including its newline.
pub(crate) fn check_size<P: crate::record::Phase>(record: &RecordData<P>) -> std::io::Result<()> {
    if compact_len(record).saturating_add(1) > MAX_RECORD_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "record exceeds the 1 MiB serialized size limit",
        ));
    }
    Ok(())
}

/// Wraps a sink and refuses any record past [`MAX_RECORD_BYTES`].
/// The phase is fixed for this instance; raw and mapped records cannot be mixed.
///
/// ```compile_fail
/// use infertap::record::{Record, PseudonymizedRecord};
/// use infertap::sink::{GuardedSink, MemorySink, Sink};
/// fn mixed(raw: Record, mapped: PseudonymizedRecord) {
///     let mut sink = GuardedSink::new(MemorySink::new(None));
///     sink.emit(raw);
///     sink.emit(mapped);
/// }
/// ```
#[derive(Debug)]
pub struct GuardedSink<S, P: crate::record::Phase = crate::record::Raw> {
    phase: std::marker::PhantomData<P>,
    inner: S,
    refused_oversized: u64,
}

impl<S, P: crate::record::Phase> GuardedSink<S, P> {
    /// Wrap `inner`. Every record it would emit is measured first.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self {
            inner,
            phase: std::marker::PhantomData,
            refused_oversized: 0,
        }
    }

    /// Records refused for exceeding [`MAX_RECORD_BYTES`]. The caller reports this loss.
    #[must_use]
    pub const fn refused_oversized(&self) -> u64 {
        self.refused_oversized
    }

    /// Unwrap to inspect the inner sink (its counts, buffered records, or writer).
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Borrow the inner sink.
    pub const fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: Sink<RecordData<P>>, P: crate::record::Phase> Sink<RecordData<P>> for GuardedSink<S, P> {
    fn emit(&mut self, record: RecordData<P>) {
        // Count compact JSON bytes without allocating a serialized copy. Refuse an
        // oversized record while allowing collection of subsequent records.
        let serialized_len = compact_len(&record).saturating_add(1); // the newline ships too
        if serialized_len > MAX_RECORD_BYTES {
            self.refused_oversized += 1;
            return;
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
    use super::GuardedSink;
    use crate::record::{OperatorText, Record};
    use crate::sink::{MemorySink, Sink};
    use crate::testing::store;

    #[test]
    fn passes_records_through() {
        let mut s = GuardedSink::new(MemorySink::new(None));
        s.emit(Record::Store(store(1)));
        assert_eq!(s.inner().records.len(), 1);
    }

    #[test]
    fn a_record_past_the_size_bound_is_refused_and_counted() {
        let mut s = GuardedSink::new(MemorySink::new(None));
        // Pathological: an extra_keys array a broken engine could emit. The refusal is
        // per record; the stream survives and the loss is counted.
        let mut huge = store(1);
        huge.extra_keys = Some(vec![OperatorText("x".repeat(1024)); 1100]);
        s.emit(Record::Store(huge));
        s.emit(Record::Store(store(2)));
        assert_eq!(s.refused_oversized(), 1);
        let records = &s.inner().records;
        assert_eq!(
            records.len(),
            1,
            "the oversized record and only it was refused"
        );
        assert_eq!(records[0]["block_id"], "2");
    }

    #[test]
    fn the_serialized_size_boundary_includes_the_newline() {
        use super::MAX_RECORD_BYTES;
        for size in [MAX_RECORD_BYTES - 1, MAX_RECORD_BYTES, MAX_RECORD_BYTES + 1] {
            let mut record = store(1);
            record.lora_name = Some(OperatorText(String::new()));
            let base = serde_json::to_vec(&Record::Store(record.clone()))
                .expect("serialize")
                .len()
                + 1;
            record.lora_name = Some(OperatorText("x".repeat(size - base)));
            let record = Record::Store(record);
            assert_eq!(
                serde_json::to_vec(&record).expect("serialize").len() + 1,
                size
            );
            let mut sink = GuardedSink::new(MemorySink::new(None));
            sink.emit(record);
            let refused = u64::from(size > MAX_RECORD_BYTES);
            assert_eq!(sink.refused_oversized(), refused, "size {size}");
            assert_eq!(
                sink.inner().records.len(),
                usize::from(refused == 0),
                "size {size}"
            );
        }
    }
}
