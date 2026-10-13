// SPDX-License-Identifier: Apache-2.0
//! Pseudonymization and size checks compose over an independently implemented sink.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use infertap::record::RecordData;

use infertap::pseudonym::{Key, BLOCK_ID_CONTEXT};
use infertap::record::{OperatorText, Record};
use infertap::sink::{GuardedSink, PseudonymConfig, PseudonymizingSink, Sink};
use infertap::testing::{store, store_record};
use serde_json::{json, Value};

/// A sink type foreign to the crate.
struct ForeignSink(Vec<Value>);

impl<P: infertap::record::Phase> Sink<RecordData<P>> for ForeignSink {
    fn emit(&mut self, record: RecordData<P>) {
        self.0.push(record.to_value());
    }
}

fn chain() -> PseudonymizingSink<GuardedSink<ForeignSink, infertap::record::Pseudonymized>> {
    let key = Key::from_bytes(b"infertap-conformance-test-key!!!infertap-conformance-nonce!!!!!!")
        .expect("test key");
    PseudonymizingSink::new(
        PseudonymConfig { key, epoch: 0 },
        BLOCK_ID_CONTEXT,
        GuardedSink::new(ForeignSink(Vec::new())),
    )
}

#[test]
fn a_future_sink_gets_the_full_chain_by_construction() {
    let mut s = chain();
    s.emit(store_record(1));
    let records = s.into_inner().into_inner().0;
    assert_eq!(records.len(), 1);
    // what reached the foreign sink is post-decorator: the pinned pseudonym, not the raw id
    assert_eq!(
        records[0]["block_id"],
        json!("db83955d1a485fe98364fd9e47c8bc08")
    );
    assert_eq!(records[0]["epoch"], json!(0));
}

#[test]
fn the_size_bound_holds_through_the_chain_for_any_sink_type() {
    let mut s = chain();
    // Past the bound even after every key is mapped to a 32-hex pseudonym: the guard sits
    // below the pseudonymizing sink and measures what ships.
    let mut huge = store(1);
    huge.extra_keys = Some(vec![OperatorText("x".to_string()); 40_000]);
    s.emit(Record::Store(huge));
    s.emit(store_record(2));
    let guarded = s.into_inner();
    assert_eq!(guarded.refused_oversized(), 1);
    let records = guarded.into_inner().0;
    assert_eq!(
        records.len(),
        1,
        "the oversized record and only it was refused"
    );
}

#[test]
fn guarded_writers_infer_the_phase_for_flush_and_error_reporting() {
    let mut raw = GuardedSink::new(infertap::sink::JsonlSink::new(Vec::new()));
    raw.emit(store_record(1));
    raw.flush().unwrap();
    assert!(raw.write_error().is_none());
    let mut mapped = chain();
    mapped.emit(store_record(1));
    let mut output = mapped.into_inner();
    output.flush().unwrap();
    assert!(output.write_error().is_none());
}
