// SPDX-License-Identifier: Apache-2.0
//! Compare emitted delivery records with `conformance/delivery/records.json`.
//! Assert exact fields and values except runtime timestamps and incarnation identifiers,
//! whose representations are checked independently.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use infertap::testing::jsonl_records as lines;
use infertap::testing::repo_root as root;
use std::fs;
use std::path::{Path, PathBuf};

use infertap::clock::{EngineMs, ProducerMs};
use infertap::record::Provenance;
use infertap::sink::segments::{SegmentConfig, SegmentedSink, ACTIVE};
use infertap::sink::Sink;
use serde_json::{json, Value};

fn corpus() -> Value {
    let path = root().join("conformance/delivery/records.json");
    serde_json::from_str(&fs::read_to_string(path).expect("read records.json")).expect("json")
}

fn scratch(name: &str) -> PathBuf {
    infertap::testing::scratch(&format!("deliv-{name}"))
}

fn provenance() -> Provenance {
    Provenance::try_from(
        corpus()["provenance"]
            .as_object()
            .expect("provenance")
            .clone(),
    )
    .expect("the corpus provenance is valid")
}

/// The suite pins write-ahead behavior, so it opens without the conversion worker.
fn open_sink(dir: &Path, max_bytes: u64, cap: Option<u64>) -> SegmentedSink {
    SegmentedSink::open_write_ahead_only(
        SegmentConfig {
            dir: dir.to_path_buf(),
            max_bytes,
            // The bounds the corpus pins: the header states them, so a rig declaring different
            // ones would fail the field comparison for a reason that is about the rig.
            max_secs: 300,
            max_disk_bytes: cap,
            // The corpus header declares a workload class (contract section 2.7), so the rig does.
            workload_class: Some("agentic".into()),
            heartbeat_secs: 60,
            // The epoch the corpus pins: the header declares it, so a rig declaring a different
            // one fails the field comparison for a reason that is about the rig.
            key_epoch: Some(3),
        },
        provenance(),
    )
    .expect("open")
}

fn all_records(dir: &Path) -> Vec<Value> {
    let mut names: Vec<_> = fs::read_dir(dir)
        .expect("read_dir")
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        // Exact, not case-insensitive: layout.json pins the suffix as `.jsonl`, so a file
        // named `.JSONL` is not a segment and must not be read as one.
        .filter(|n| Path::new(n).extension().is_some_and(|e| e == "jsonl"))
        .collect();
    names.sort();
    names.iter().flat_map(|n| lines(&dir.join(n))).collect()
}

fn of_kind(records: &[Value], kind: &str) -> Vec<Value> {
    records
        .iter()
        .filter(|r| r["kind"] == json!(kind))
        .cloned()
        .collect()
}

fn expected(kind: &str) -> Vec<Value> {
    corpus()["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .map(|c| c["expect"].clone())
        .filter(|e| e["kind"] == json!(kind))
        .collect()
}

/// Fields whose values depend on the run rather than the contract fixture.
///
/// Validate their types separately. Assert that filenames agree with sequence
/// numbers and that each count matches the declared range.
const RUN_VALUED: [&str; 8] = [
    "incarnation",
    "at_ms",
    "dropped_bytes",
    "count",
    "first",
    "last",
    "first_seq",
    "last_seq",
];

/// Compare complete field sets as well as values so unexpected record fields fail the test.
fn assert_matches(actual: &Value, expect: &Value, what: &str) {
    let a = actual.as_object().expect("actual object");
    let e = expect.as_object().expect("expected object");

    let mut a_keys: Vec<_> = a.keys().cloned().collect();
    let mut e_keys: Vec<_> = e.keys().cloned().collect();
    a_keys.sort();
    e_keys.sort();
    assert_eq!(
        a_keys, e_keys,
        "{what}: the writer's field set and the corpus's disagree. A field the corpus does not \
         pin is a field no consumer models."
    );

    for (k, want) in e {
        let got = a
            .get(k)
            .unwrap_or_else(|| panic!("{what}: missing field {k:?}"));
        if RUN_VALUED.contains(&k.as_str()) {
            // Not the magnitude, but the type: a `count` that arrived as a string, or a
            // `dropped_bytes` that became null, would otherwise pass unnoticed.
            assert_eq!(
                std::mem::discriminant(got),
                std::mem::discriminant(want),
                "{what}: field {k:?} changed JSON type: pinned {want}, got {got}"
            );
            continue;
        }
        assert_eq!(Some(got), Some(want), "{what}: field {k:?}");
    }
}

/// Validate `<run_start_unix_ms>-<pid>` against the incarnation pattern in
/// `layout.json`; the timestamp and PID vary between runs.
fn assert_incarnation_shape(v: &Value, what: &str) {
    let s = v
        .as_str()
        .unwrap_or_else(|| panic!("{what}: incarnation must be a string"));
    let (run_start, pid) = s
        .split_once('-')
        .unwrap_or_else(|| panic!("{what}: incarnation {s:?} is not <run_start_ms>-<pid>"));
    assert!(
        !run_start.is_empty() && run_start.bytes().all(|b| b.is_ascii_digit()),
        "{what}: run-start {run_start:?} is not an integer"
    );
    assert!(
        !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit()),
        "{what}: pid {pid:?} is not an integer"
    );
}

#[test]
fn the_writer_emits_the_pinned_segment_open() {
    let dir = scratch("open");
    let s = open_sink(&dir, 1 << 20, None);
    s.close().expect("close");

    let opens = of_kind(&all_records(&dir), "segment_open");
    assert_eq!(opens.len(), 1, "one segment, one header");
    assert_matches(&opens[0], &expected("segment_open")[0], "segment_open");
    assert_incarnation_shape(&opens[0]["incarnation"], "segment_open");
    assert_eq!(
        opens[0]["segment_seq"],
        json!(0),
        "a fresh incarnation starts at 0, which is what makes seq 0 a restart rather than a gap"
    );
}

#[test]
fn the_writer_emits_the_pinned_segments_dropped() {
    let dir = scratch("dropped");
    // A cap low enough that the ring drops sealed segments, with segments small enough to seal
    // often. The tap records the loss in-band; without that record a consumer sees only a hole.
    let mut s = open_sink(&dir, 256, Some(4096));
    for i in 0..400_u64 {
        s.emit(infertap::testing::store_record(
            17_843_516_567_353_125_252_u64 + i,
        ));
    }
    s.close().expect("close");

    let dropped = of_kind(&all_records(&dir), "segments_dropped");
    assert!(
        !dropped.is_empty(),
        "the ring dropped segments and must have said so in-band"
    );
    assert_matches(
        &dropped[0],
        &expected("segments_dropped")[0],
        "segments_dropped",
    );
    assert_incarnation_shape(&dropped[0]["incarnation"], "segments_dropped");

    // The range is what makes the loss localizable rather than merely acknowledged, and it is
    // describable only because drops are grouped per incarnation: across two runs the set is
    // never contiguous, which is how this record once reported two dropped segments as 9..90.
    let first = dropped[0]["first_seq"].as_u64().expect("first_seq");
    let last = dropped[0]["last_seq"].as_u64().expect("last_seq");
    let count = dropped[0]["count"].as_u64().expect("count");
    assert!(last >= first, "the dropped range must not run backwards");
    // The filenames stay, and must agree with the integers rather than approximate them.
    let name_seq = |v: &Value| -> u64 {
        v.as_str()
            .expect("filename")
            .trim_end_matches(".jsonl")
            .rsplit_once('-')
            .expect("seq")
            .1
            .parse()
            .expect("integer")
    };
    assert_eq!(
        name_seq(&dropped[0]["first"]),
        first,
        "first filename disagrees with first_seq"
    );
    assert_eq!(
        name_seq(&dropped[0]["last"]),
        last,
        "last filename disagrees with last_seq"
    );
    assert_eq!(
        count,
        last - first + 1,
        "count must equal the range it names, or a consumer cannot tell which segments are gone"
    );
}

#[test]
fn the_writer_emits_the_pinned_segment_recovered_for_both_outcomes() {
    // Attributable: the header survived, so the orphan seals under its own incarnation and the
    // trailer says so.
    let dir = scratch("recovered-attributed");
    let header = json!({"kind": "segment_open", "incarnation": "1785153670000-4242",
                        "segment_seq": 7, "at_ms": 1.0});
    let body = json!({"kind": "store", "instance_id": "i0", "block_id": 1_u64, "at_ms": 2.0});
    fs::write(dir.join(ACTIVE), format!("{header}\n{body}\n{{\"torn\":")).expect("write");
    open_sink(&dir, 1 << 20, None).close().expect("close");

    let recovered = of_kind(&all_records(&dir), "segment_recovered");
    assert_eq!(recovered.len(), 1);
    let want = expected("segment_recovered");
    assert_matches(&recovered[0], &want[0], "segment_recovered (attributed)");
    assert_eq!(recovered[0]["attributed"], json!(true));
    assert!(
        recovered[0]["dropped_bytes"]
            .as_u64()
            .expect("dropped_bytes")
            > 0,
        "a torn tail was discarded and the trailer must say how much"
    );

    // Headerless: the file was never durably written, so it is discarded and declared second
    // in the recovering incarnation's first segment (contract section 4.3).
    let dir = scratch("recovered-unattributed");
    let junk = format!("{body}\n");
    fs::write(dir.join(ACTIVE), &junk).expect("write");
    open_sink(&dir, 1 << 20, None).close().expect("close");

    let records = all_records(&dir);
    let recovered = of_kind(&records, "segment_recovered");
    assert_eq!(recovered.len(), 1);
    assert_matches(&recovered[0], &want[1], "segment_recovered (unattributed)");
    assert_eq!(recovered[0]["attributed"], json!(false));
    assert_eq!(recovered[0]["dropped_bytes"], json!(junk.len() as u64));
    assert_eq!(
        records[0]["kind"], "segment_open",
        "the recovering segment's own header first"
    );
    assert_eq!(
        records[1]["kind"], "segment_recovered",
        "the declaration second"
    );
    assert!(
        records.iter().all(|r| r["block_id"] != body["block_id"]),
        "no record of the discarded file ships"
    );
    let names: Vec<_> = fs::read_dir(&dir)
        .expect("read_dir")
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        names
            .iter()
            .all(|n| n.starts_with("seg-") || n == ".writer.lock.jsonl"),
        "nothing headerless remains: {names:?}"
    );
}

/// Every kind this corpus pins is a kind the tap can actually emit, and every delivery kind the
/// tap emits is pinned here.
///
/// The second direction is the one that matters. A consumer's coverage assertion, that every
/// kind the contract pins is a kind it models, is only as complete as the contract, so a kind
/// the tap emits and this file omits would slip past both repositories.
#[test]
fn the_corpus_names_every_delivery_kind_the_tap_can_emit() {
    // The segment sink is a module directory; every file of it may author a record kind.
    let mut source = String::new();
    for entry in fs::read_dir(root().join("src/sink/segments")).expect("read segments/") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|e| e == "rs") {
            source.push_str(&fs::read_to_string(&path).expect("read a segments file"));
        }
    }
    // Records are typed: the kinds the writer emits are the `Record::` variants its source
    // constructs, and a variant's wire name is its snake_case.
    let mut emitted: Vec<String> = source
        .match_indices("Record::")
        .filter_map(|(i, m)| {
            let rest = &source[i + m.len()..];
            let end = rest.find(|c: char| !c.is_ascii_alphanumeric())?;
            let variant = &rest[..end];
            (rest[end..].starts_with('(') && variant.starts_with("Segment")).then(|| {
                let mut snake = String::new();
                for (j, ch) in variant.chars().enumerate() {
                    if ch.is_ascii_uppercase() && j > 0 {
                        snake.push('_');
                    }
                    snake.push(ch.to_ascii_lowercase());
                }
                snake
            })
        })
        .collect();
    emitted.sort();
    emitted.dedup();

    let mut pinned: Vec<String> = corpus()["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .map(|c| c["expect"]["kind"].as_str().expect("kind").to_string())
        .collect();
    pinned.sort();
    pinned.dedup();

    assert_eq!(
        emitted, pinned,
        "the delivery kinds the writer emits and the kinds this corpus pins have diverged. A \
         kind the tap emits and the contract omits is invisible to every consumer downstream."
    );
}

/// The shipped segment shape: the header is first, and every later record belongs to the
/// incarnation it names.
///
/// This containment is the property a consumer needs and cannot get from the record shapes
/// alone. It is what lets a reader attribute a whole file's rows to one tap run, and therefore
/// to one key, one configuration, and one continuity sequence, from a single record.
#[test]
fn a_sealed_segment_has_its_header_first_and_matches_the_pinned_shape() {
    let dir = scratch("segment-shape");
    // The pinned segment's shape: two stores, a heartbeat, an evict, after the header.
    let mut s = open_sink(&dir, 1 << 20, None);
    s.emit(infertap::testing::store_record(1));
    s.emit(infertap::testing::store_record(2));
    s.emit(infertap::lifecycle::heartbeat_record(
        &infertap::lifecycle::EnvelopeStats {
            msgs_seen: 3,
            dropped: 0,
            oversized: 0,
            noraw_scanned: None,
            unknown_types: 0,
            events_ingested: 2,
            content_unresolved: 0,
            content_bridge_entries: 0,
            content_bridge_refused: 0,
            publisher_restarts: 0,
            rss_bytes: None,
            reuse_reporting: infertap::record::ReuseReporting::Unlabelled,
            endpoints: vec![],
        },
        None,
        ProducerMs(1_785_153_670_020.0),
    ));
    let evict = |block: u64, at_ms: f64| {
        let at_ms = EngineMs(at_ms);
        infertap::record::Record::Evict(infertap::record::Evict {
            instance_id: "i0".into(),
            at_ms,
            seq: None,
            dp_rank: 0,
            group_idx: 0,
            block_id: infertap::record::Id(block),
            tier: Some("GPU".into()),
            locality: None,
            backend_id: None,
            content_id: None,
            epoch: None,
        })
    };
    s.emit(evict(1, 1_785_153_670_030.0));
    // The corpus segment goes on: a second instance's store and evict, a clear, a reset.
    s.emit(infertap::testing::store_record(3));
    s.emit(evict(3, 1_785_153_670_050.0));
    s.emit(infertap::record::Record::Clear(infertap::record::Clear {
        instance_id: "i1".into(),
        at_ms: EngineMs(1_785_153_670_060.0),
        seq: None,
        dp_rank: Some(1),
        group_idx: Some(2),
        scope: infertap::record::AllBlocks,
        tier: Some("gpu".into()),
    }));
    s.emit(infertap::record::Record::HolderReset(
        infertap::record::HolderReset {
            instance_id: "i1".into(),
            at_ms: ProducerMs(1_785_153_670_070.0),
            seq: None,
            dp_rank: Some(1),
            boundary_ms: EngineMs(1_785_153_670_065.0),
        },
    ));
    s.close().expect("close");

    let names: Vec<_> = fs::read_dir(&dir)
        .expect("read_dir")
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("seg-"))
        .collect();
    assert_eq!(names.len(), 1, "one sealed segment");
    let got = lines(&dir.join(&names[0]));

    assert_eq!(
        got[0]["kind"],
        json!("segment_open"),
        "the header must be the FIRST record: recovery reads a crashed file's identity out of \
         its own first line, so a header anywhere else is a header that was not there when it \
         mattered"
    );
    let incarnation = got[0]["incarnation"].as_str().expect("incarnation");
    assert!(
        names[0].contains(incarnation),
        "the filename echoes the header's incarnation; the header is the source"
    );

    let c = corpus();
    let pinned = c["segment"]["lines"].as_array().expect("lines");
    assert_eq!(
        got.len(),
        pinned.len(),
        "record count matches the pinned segment"
    );
    for (i, (g, p)) in got.iter().zip(pinned).enumerate() {
        assert_eq!(g["kind"], p["kind"], "record {i} kind");
    }
}
