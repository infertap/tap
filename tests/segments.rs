// SPDX-License-Identifier: Apache-2.0
//! Segment sealing, recovery, and retention.
//!
//! Check record completeness, segment immutability, headers, and incarnation scope.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use infertap::testing::jsonl_records as lines;
use std::fs;
use std::path::{Path, PathBuf};

use infertap::record::Provenance;
use infertap::record::Record;
use infertap::sink::segments::{SegmentConfig, SegmentedSink, ACTIVE};
use infertap::sink::Sink;
use serde_json::{json, Map, Value};

fn scratch(name: &str) -> PathBuf {
    infertap::testing::scratch(&format!("seg-{name}"))
}

fn cfg(dir: &Path, max_bytes: u64, max_secs: u64, cap: Option<u64>) -> SegmentConfig {
    SegmentConfig {
        dir: dir.to_path_buf(),
        max_bytes,
        max_secs,
        max_disk_bytes: cap,
        workload_class: None,
        heartbeat_secs: 60,
        key_epoch: None,
    }
}

/// The suite pins write-ahead behavior, so it opens without the conversion worker;
/// conversion has its own tests below.
fn sink(dir: &Path, max_bytes: u64, max_secs: u64, cap: Option<u64>) -> SegmentedSink {
    SegmentedSink::open_write_ahead_only(cfg(dir, max_bytes, max_secs, cap), Provenance::default())
        .expect("open")
}

fn sealed(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .expect("read_dir")
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("seg-"))
        .collect();
    v.sort();
    v
}

fn rec(i: u64) -> Record {
    infertap::testing::store_record(17_843_516_567_353_125_252_u64 + i)
}

/// A second run preserves the first run's sealed segments.
#[test]
fn a_second_run_cannot_touch_the_first_runs_records() {
    let dir = scratch("no-truncate");
    let first_incarnation;
    {
        let mut s = sink(&dir, 1 << 20, 3600, None);
        first_incarnation = s.incarnation().to_string();
        for i in 0..50 {
            s.emit(rec(i));
        }
        s.close().expect("close");
    }
    let after_first = sealed(&dir);
    assert_eq!(after_first.len(), 1);
    let first_bytes = fs::read(dir.join(&after_first[0])).expect("read");

    // These sinks share a process ID. Separate their open timestamps to model
    // distinct process lifetimes.
    let mut incarnations = vec![first_incarnation];
    for _ in 0..2 {
        std::thread::sleep(std::time::Duration::from_millis(2));
        let s = sink(&dir, 1 << 20, 3600, None);
        incarnations.push(s.incarnation().to_string());
        s.close().expect("close");
    }
    let distinct: std::collections::BTreeSet<&String> = incarnations.iter().collect();
    assert_eq!(
        distinct.len(),
        3,
        "the three runs did not take distinct identities ({incarnations:?}): the harness \
         failed to simulate three process lifetimes, so what follows would test nothing"
    );

    let after = sealed(&dir);
    assert_eq!(after.len(), 3, "each run seals its own segment");
    assert!(
        after.contains(&after_first[0]),
        "the first run's segment survives"
    );
    assert_eq!(
        fs::read(dir.join(&after_first[0])).expect("read"),
        first_bytes,
        "and is byte-identical"
    );
}

/// Every segment is self-describing: its own identity is its first record, so nothing
/// downstream has to parse a filename to know what it is looking at.
#[test]
fn every_segment_opens_with_its_own_identity() {
    let dir = scratch("header");
    let mut s = sink(&dir, 400, 3600, None); // small: forces several seals
    for i in 0..40 {
        s.emit(rec(i));
    }
    let incarnation = s.incarnation().to_string();
    s.close().expect("close");

    let names = sealed(&dir);
    assert!(
        names.len() >= 3,
        "expected several segments, got {}",
        names.len()
    );
    let mut seqs: Vec<u64> = Vec::new();
    for name in &names {
        let rows = lines(&dir.join(name));
        let head = &rows[0];
        assert_eq!(head["kind"], "segment_open");
        assert_eq!(head["incarnation"], json!(incarnation));
        seqs.push(head["segment_seq"].as_u64().expect("segment_seq"));
    }
    // Numeric, not lexicographic: seq 10 sorts before seq 2 as a string.
    seqs.sort_unstable();
    assert_eq!(
        seqs,
        (0..names.len() as u64).collect::<Vec<_>>(),
        "segment_seq must be contiguous within an incarnation"
    );
}

/// A sealed segment never contains a partial line, whatever the crash did to the active one.
/// That is what lets a consumer treat any unparseable line as corruption rather than as the
/// expected shape of a truncated tail.
#[test]
fn sealed_segments_are_always_complete() {
    let dir = scratch("complete");
    let mut s = sink(&dir, 300, 3600, None);
    for i in 0..30 {
        s.emit(rec(i));
    }
    s.close().expect("close");
    for name in sealed(&dir) {
        let text = fs::read_to_string(dir.join(&name)).expect("read");
        assert!(text.ends_with('\n'), "{name} must end on a record boundary");
        for line in text.lines() {
            serde_json::from_str::<Value>(line).unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}

// --- orphan recovery ------------------------------------------------------------------

fn write_orphan(dir: &Path, incarnation: &str, seq: u64, tail: &str) {
    let header = json!({"kind": "segment_open", "incarnation": incarnation,
                        "segment_seq": seq, "at_ms": 1.0});
    let body = json!({"kind": "store", "block_id": 1_u64, "at_ms": 2.0});
    fs::write(dir.join(ACTIVE), format!("{header}\n{body}\n{tail}")).expect("write orphan");
}

/// An orphan belongs to the incarnation that wrote it, not the one that finds it. Sealing it
/// under the recovering process's identity corrupts both sequences, and because both variants
/// produce well-formed files, nothing downstream would notice.
#[test]
fn an_orphan_is_sealed_under_its_own_incarnation_not_the_recovering_one() {
    let dir = scratch("orphan-identity");
    write_orphan(&dir, "1700000000000-4242", 7, "");

    let s = sink(&dir, 1 << 20, 3600, None);
    let recovering = s.incarnation().to_string();
    s.close().expect("close");

    let names = sealed(&dir);
    assert!(
        names.contains(&"seg-1700000000000-4242-7.jsonl".to_string()),
        "orphan must be sealed under ITS identity, got {names:?}"
    );
    assert!(
        !recovering.starts_with("1700000000000-4242"),
        "test is only meaningful if the incarnations differ"
    );
    // and the recovering incarnation's own segment is separate, starting at seq 0
    let own: Vec<_> = names.iter().filter(|n| n.contains(&recovering)).collect();
    assert_eq!(own.len(), 1);
    assert!(
        own[0].ends_with("-0.jsonl"),
        "a fresh incarnation starts at 0"
    );
}

/// A record that was fully written but lost only its newline is a complete record. Scanning
/// back to the previous newline would silently discard it.
#[test]
fn an_orphan_tail_that_is_a_complete_object_is_kept() {
    let dir = scratch("orphan-complete-tail");
    let tail = r#"{"kind":"evict","block_id":99,"at_ms":3.0}"#; // no trailing newline
    write_orphan(&dir, "1700000000000-1", 0, tail);

    sink(&dir, 1 << 20, 3600, None).close().expect("close");
    let rows = lines(&dir.join("seg-1700000000000-1-0.jsonl"));
    assert_eq!(
        rows.len(),
        4,
        "header + body + recovered tail + recovery trailer"
    );
    assert_eq!(rows[2]["kind"], "evict");
    assert_eq!(rows[2]["block_id"], json!(99));
    assert_eq!(rows[3]["kind"], "segment_recovered");
    assert_eq!(rows[3]["attributed"], json!(true));
    assert_eq!(
        rows[3]["dropped_bytes"],
        json!(0),
        "a record that lost only its newline loses no bytes"
    );
}

/// A genuinely torn record cannot be salvaged and must not reach a sealed file.
#[test]
fn an_orphan_tail_that_is_torn_is_discarded() {
    let dir = scratch("orphan-torn");
    write_orphan(&dir, "1700000000000-2", 0, r#"{"kind":"evict","blo"#);

    sink(&dir, 1 << 20, 3600, None).close().expect("close");
    let rows = lines(&dir.join("seg-1700000000000-2-0.jsonl"));
    assert_eq!(
        rows.len(),
        3,
        "header + body + recovery trailer; the torn tail is gone"
    );
    assert_eq!(rows[1]["kind"], "store");
    assert_eq!(rows[2]["kind"], "segment_recovered");
    assert!(
        rows[2]["dropped_bytes"].as_u64().expect("n") > 0,
        "the loss is accounted"
    );
}

/// A headerless file is not a segment (contract section 4.3): the header is fsync'd
/// before any record, so such a file was never durably written. It is discarded, never
/// shipped, and the discard is declared second in the recovering incarnation's first segment.
#[test]
fn a_headerless_file_is_discarded_and_declared() {
    let dir = scratch("headerless");
    // no segment_open header, one complete record, then a torn one
    let junk = "{\"kind\":\"store\",\"block_id\":5,\"at_ms\":1.0}\n{\"kind\":\"sto";
    fs::write(dir.join(ACTIVE), junk).expect("write");
    sink(&dir, 1 << 20, 3600, None).close().expect("close");

    let names: Vec<String> = fs::read_dir(&dir)
        .expect("read_dir")
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        names
            .iter()
            .all(|n| n.starts_with("seg-") || n == ".writer.lock.jsonl"),
        "nothing headerless may remain or ship: {names:?}"
    );
    let rows = lines(&dir.join(&sealed(&dir)[0]));
    assert_eq!(
        rows[0]["kind"], "segment_open",
        "the recovering segment's own header first"
    );
    assert_eq!(
        rows[1]["kind"], "segment_recovered",
        "the declaration second"
    );
    assert_eq!(rows[1]["attributed"], json!(false));
    assert_eq!(
        rows[1]["dropped_bytes"],
        json!(junk.len() as u64),
        "the whole file is declared, not only the torn tail"
    );
    assert!(
        rows.iter().all(|r| r["block_id"] != json!(5)),
        "no record of the discarded file ships"
    );
}

/// An orphan with nothing complete in it has no data to ship and no identity to ship it under.
#[test]
fn an_orphan_with_no_complete_record_is_discarded() {
    let dir = scratch("orphan-empty");
    fs::write(dir.join(ACTIVE), "{\"kind\":\"sto").expect("write");
    sink(&dir, 1 << 20, 3600, None).close().expect("close");
    assert!(
        !fs::read_dir(&dir)
            .expect("read_dir")
            .filter_map(std::result::Result::ok)
            .any(|e| e.file_name().to_string_lossy().starts_with("orphan-")),
        "nothing complete survived, so there is nothing to seal"
    );
}

// --- sealing triggers and the ring ------------------------------------------------------

/// Age-based sealing makes heartbeats eligible for shipping below the size threshold.
#[test]
fn a_quiet_instance_still_seals_on_the_time_ceiling() {
    let dir = scratch("quiet");
    let mut s = sink(&dir, 1 << 30, 0, None); // size unreachable, age ceiling immediate
    s.emit(rec(0));
    s.emit(rec(1));
    s.emit(rec(2));
    s.close().expect("close");
    assert!(
        sealed(&dir).len() >= 2,
        "the age ceiling must seal without the size bound ever being hit"
    );
}

/// Bounded, declared loss. The in-band record says what went; the gap it leaves in
/// `segment_seq` is what survives if that record is itself dropped later.
#[test]
fn the_ring_drops_oldest_records_it_and_leaves_a_sequence_gap() {
    let dir = scratch("ring");
    // 4x segment size is the floor; use it exactly so the ring engages quickly
    let mut s = sink(&dir, 300, 3600, Some(1200));
    for i in 0..120 {
        s.emit(rec(i));
    }
    let dropped = s.dropped_segments();
    s.close().expect("close");
    assert!(dropped > 0, "the ring must have engaged");

    let seqs: Vec<u64> = sealed(&dir)
        .iter()
        .map(|n| lines(&dir.join(n))[0]["segment_seq"].as_u64().expect("seq"))
        .collect();
    let lowest = *seqs.iter().min().expect("some segments");
    assert!(
        lowest > 0,
        "the oldest segments were dropped, so the surviving sequence must not start at 0"
    );

    // and the loss is stated in-band somewhere in what survived
    let announced = sealed(&dir).iter().any(|n| {
        lines(&dir.join(n))
            .iter()
            .any(|r| r["kind"] == "segments_dropped")
    });
    assert!(announced, "the drop must also be recorded in-band");
}

/// A cap that cannot hold several segments would seal and immediately drop, forever. Refuse
/// it rather than misbehave quietly.
#[test]
fn a_disk_cap_below_a_few_segments_is_rejected() {
    let dir = scratch("thrash");
    let Err(err) = SegmentedSink::open(
        cfg(&dir, 64 * 1024 * 1024, 300, Some(1024)),
        Provenance::default(),
    ) else {
        panic!("a cap below several segments must be refused")
    };
    assert!(err.to_string().contains("below 4x"), "got {err}");
}

/// Retention keeps a contiguous suffix ending at the highest sequence.
/// Use more than ten segments to distinguish numeric from lexicographic order.
#[test]
fn the_ring_keeps_the_newest_segments_as_a_contiguous_run() {
    let dir = scratch("ring-order");
    let mut s = sink(&dir, 300, 3600, Some(1200));
    for i in 0..600 {
        s.emit(rec(i));
    }
    let dropped = s.dropped_segments();
    s.close().expect("close");
    assert!(dropped > 0, "the ring must have engaged");

    let mut seqs: Vec<u64> = sealed(&dir)
        .iter()
        .map(|n| lines(&dir.join(n))[0]["segment_seq"].as_u64().expect("seq"))
        .collect();
    seqs.sort_unstable();

    let highest = *seqs.last().expect("some segments");
    assert!(
        highest >= 10,
        "the run must reach two-digit seqs or lexicographic and numeric order agree and this \
         test cannot fail; got highest {highest}"
    );
    for w in seqs.windows(2) {
        assert_eq!(
            w[1],
            w[0] + 1,
            "survivors must be contiguous: a retention ring keeps a suffix, and a hole here \
             means it dropped by name order rather than by age. Surviving seqs: {seqs:?}"
        );
    }
    assert_eq!(
        seqs[0] + seqs.len() as u64 - 1,
        highest,
        "the surviving run must end at the newest segment written"
    );
}

/// Each drop record describes a contiguous segment range within one incarnation.
/// Its count must agree with the first and last sequence numbers.
#[test]
fn a_declared_drop_names_a_range_that_matches_its_count() {
    let dir = scratch("ring-declares");
    let mut s = sink(&dir, 300, 3600, Some(1200));
    for i in 0..600 {
        s.emit(rec(i));
    }
    s.close().expect("close");

    let declarations: Vec<Value> = sealed(&dir)
        .iter()
        .flat_map(|n| lines(&dir.join(n)))
        .filter(|r| r["kind"] == "segments_dropped")
        .collect();
    assert!(
        !declarations.is_empty(),
        "the drop must be recorded in-band"
    );

    for d in &declarations {
        let count = d["count"].as_u64().expect("count");
        let first = d["first_seq"].as_u64().expect("first_seq");
        let last = d["last_seq"].as_u64().expect("last_seq");
        assert!(last >= first, "range must not run backwards: {d}");
        assert_eq!(
            count,
            last - first + 1,
            "count must equal the range it names, or a consumer reading first..last as the \
             dropped set is wrong by however much the gap hides: {d}"
        );
        // The filenames stay, and they agree with the integers rather than duplicating them
        // approximately.
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
            name_seq(&d["first"]),
            first,
            "first filename and first_seq disagree"
        );
        assert_eq!(
            name_seq(&d["last"]),
            last,
            "last filename and last_seq disagree"
        );
    }
}

/// Across a restart, the ring drops the previous run's segments before the current run's,
/// and says so under the previous run's identity.
///
/// `segment_seq` resets to 0 on every new incarnation, so the sequence alone cannot order
/// two runs: run B's segment 0 and run A's segment 0 are both 0 while A's is older. The
/// incarnation leads the sort, and it orders correctly because `<run_start_unix_ms>-<pid>`
/// begins with a 13-digit millisecond timestamp, fixed-width for the next two centuries, so
/// string order is time order across that range.
///
/// The first run is uncapped so it leaves a full backlog, and the second is capped tightly
/// so its first seal reclaims that backlog in one pass. That keeps the test independent of
/// segment arithmetic. Declaration records occupy segment space and so provoke further
/// seals, drops, and declarations; under sustained pressure the second run eventually
/// reclaims its own early segments, the ones holding the declarations about the first run,
/// and the declaration vanishes with the data it described. The structural gap in
/// `segment_seq` is the durable signal and the in-band record corroborates it.
#[test]
fn a_restart_drops_the_previous_runs_segments_first_and_names_that_run() {
    let dir = scratch("ring-restart");

    let mut a = sink(&dir, 300, 3600, None);
    let first_run = a.incarnation().to_string();
    for i in 0..40 {
        a.emit(rec(i));
    }
    a.close().expect("close a");
    let backlog = sealed(&dir).len();
    assert!(
        backlog > 4,
        "the first run must leave a backlog; got {backlog}"
    );

    let mut b = sink(&dir, 300, 3600, Some(1200));
    let second_run = b.incarnation().to_string();
    assert_ne!(first_run, second_run, "a restart is a new incarnation");
    for i in 40..44 {
        b.emit(rec(i));
    }
    b.close().expect("close b");

    let seq_in = |name: &str| -> (String, u64) {
        let stem = name.trim_start_matches("seg-").trim_end_matches(".jsonl");
        let (inc, seq) = stem.rsplit_once('-').expect("seq");
        (inc.to_string(), seq.parse().expect("integer seq"))
    };

    let declared: Vec<Value> = sealed(&dir)
        .iter()
        .flat_map(|n| lines(&dir.join(n)))
        .filter(|r| r["kind"] == "segments_dropped")
        .collect();
    assert!(!declared.is_empty(), "the drop must be recorded in-band");

    // Each declaration names the incarnation whose files were reclaimed.
    for d in &declared {
        let (from_name, _) = seq_in(d["first"].as_str().expect("first"));
        assert_eq!(
            d["incarnation"].as_str().expect("incarnation"),
            from_name,
            "a declaration must name the incarnation in its own filenames, not the process that \
             noticed: {d}"
        );
    }

    let about_first: Vec<&Value> = declared
        .iter()
        .filter(|d| d["incarnation"] == Value::String(first_run.clone()))
        .collect();
    assert!(
        !about_first.is_empty(),
        "the second run reclaimed the first run's backlog and must have declared it under the \
         first run's incarnation. Declared: {declared:?}"
    );

    // Oldest first, across the restart: every segment of the first run that SURVIVED is newer
    // than every one it dropped.
    let highest_dropped = about_first
        .iter()
        .map(|d| d["last_seq"].as_u64().expect("last_seq"))
        .max()
        .expect("some drop");
    for name in sealed(&dir) {
        let (inc, seq) = seq_in(&name);
        if inc == first_run {
            assert!(
                seq > highest_dropped,
                "{name} survived while seq {highest_dropped} of the same run was dropped: the \
                 ring is not dropping oldest-first across the restart"
            );
        }
    }
}

/// The WAL and its Parquet twin share one identity. Reclaim both files
/// together, even if deleting the first would satisfy the disk budget.
#[test]
fn the_ring_drops_a_twin_pair_atomically_and_declares_it_once() {
    let dir = scratch("ring-pair");
    // One identity, two forms, sized so the budget is met after one of them: 1600 bytes
    // against a 1200 cap. Reclaiming either file alone satisfies the arithmetic; only the
    // pair rule forces both.
    fs::write(dir.join("seg-100-0.jsonl"), vec![b'x'; 800]).expect("plant");
    fs::write(dir.join("seg-100-0.parquet"), vec![b'x'; 800]).expect("plant");

    sink(&dir, 300, 3600, Some(1200)).close().expect("close");

    assert!(
        !dir.join("seg-100-0.jsonl").exists() && !dir.join("seg-100-0.parquet").exists(),
        "both forms of the dropped identity must be gone"
    );
    let declarations: Vec<Value> = sealed(&dir)
        .iter()
        .flat_map(|n| lines(&dir.join(n)))
        .filter(|r| r["kind"] == "segments_dropped")
        .collect();
    assert_eq!(
        declarations.len(),
        1,
        "one identity, one declaration: {declarations:?}"
    );
    assert_eq!(
        declarations[0]["count"],
        json!(1),
        "the loss is ONE segment, not two files: {}",
        declarations[0]
    );
    assert_eq!(declarations[0]["incarnation"], json!("100"));
}

/// Repeated restarts must stay below an absolute disk ceiling. The bound
/// allows variation in filename lengths and the phase of segment reclamation.
#[test]
fn a_crashloop_stays_within_the_disk_cap() {
    const CAP: u64 = 1200;
    let dir = scratch("crashloop-cap");
    let mut incarnations = std::collections::BTreeSet::new();
    for _ in 0..100 {
        let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", "crashloop_restart_child", "--ignored"])
            .env("INFERTAP_CRASHLOOP_TEST_DIR", &dir)
            .stdout(std::process::Stdio::null())
            .status()
            .expect("restart child");
        assert!(status.success(), "restart child failed: {status}");
        let header = &lines(&dir.join(ACTIVE))[0];
        let incarnation = header["incarnation"].as_str().expect("incarnation");
        assert!(
            incarnations.insert(incarnation.to_owned()),
            "restart reused incarnation {incarnation}"
        );
    }
    sink(&dir, 300, 3600, Some(CAP)).close().expect("close");
    let bytes: u64 = fs::read_dir(&dir)
        .expect("read_dir")
        .filter_map(std::result::Result::ok)
        .map(|e| e.metadata().map_or(0, |m| m.len()))
        .sum();
    assert!(
        bytes <= CAP * 3,
        "100 restarts left {bytes} bytes against a {CAP} cap. Unbounded growth here means the \
         ring is not running at startup, only on a mid-run seal, and a crashloop never seals \
         mid-run."
    );
}

#[test]
#[ignore = "subprocess fixture for crash-loop recovery"]
fn crashloop_restart_child() {
    let dir =
        PathBuf::from(std::env::var_os("INFERTAP_CRASHLOOP_TEST_DIR").expect("test directory"));
    let _sink = sink(&dir, 300, 3600, Some(1200));
    // Exit without closing or dropping the sink, leaving a durable active header.
    std::process::exit(0);
}

/// The segment declares once; the records carry only what varies.
///
/// Provenance and the workload class are properties of the producer run, so they sit on the
/// header and a reader applies them to every record in the segment (contract sections 2.2 and 2.7). A record
/// that repeated them would put a constant on every line of the stream.
#[test]
fn the_header_declares_and_the_records_do_not() {
    let dir = scratch("declarations");
    let mut prov = Map::new();
    prov.insert("gpu".into(), "h100".into());
    prov.insert("vllm_version".into(), "0.26.0".into());
    let prov = Provenance::try_from(prov).expect("valid provenance");

    let mut s = SegmentedSink::open_write_ahead_only(
        SegmentConfig {
            dir: dir.clone(),
            max_bytes: 1 << 20,
            max_secs: 3600,
            max_disk_bytes: None,
            workload_class: Some("agentic".into()),
            heartbeat_secs: 60,
            key_epoch: None,
        },
        prov.clone(),
    )
    .expect("open");
    s.emit(rec(1));
    s.close().expect("close");

    let name = sealed(&dir).into_iter().next().expect("a sealed segment");
    let text = fs::read_to_string(dir.join(name)).expect("read");
    let mut lines = text.lines();

    let header: Value = serde_json::from_str(lines.next().expect("header")).expect("json");
    assert_eq!(header["kind"], "segment_open");
    assert_eq!(
        header["gpu"], "h100",
        "the header states provenance: {header}"
    );
    assert_eq!(header["vllm_version"], "0.26.0");
    assert_eq!(header["workload_class"], "agentic");
    assert_eq!(
        header["contract_version"],
        infertap::sink::segments::CONTRACT_VERSION,
        "the header declares the contract the crate implements"
    );

    let record: Value = serde_json::from_str(lines.next().expect("record")).expect("json");
    assert_eq!(record["kind"], "store");
    for declared in ["gpu", "vllm_version", "workload_class"] {
        assert!(
            record.get(declared).is_none(),
            "{declared} is declared once on the header, not repeated per record: {record}"
        );
    }
}

/// A producer that declares no workload class says nothing, which is not the same as declaring
/// that it serves none. The field is absent rather than empty.
#[test]
fn an_undeclared_workload_class_is_absent_from_the_header() {
    let dir = scratch("undeclared");
    let mut s = sink(&dir, 1 << 20, 3600, None);
    s.emit(rec(1));
    s.close().expect("close");

    let name = sealed(&dir).into_iter().next().expect("a sealed segment");
    let text = fs::read_to_string(dir.join(name)).expect("read");
    let header: Value = serde_json::from_str(text.lines().next().expect("header")).expect("json");
    assert!(
        header.get("workload_class").is_none(),
        "nothing declared means the key is absent, never empty: {header}"
    );
}

/// Readers may not have the start record. Each segment header must carry the configured
/// liveness bounds used to detect stale producers (contract section 5.8).
#[test]
fn every_header_declares_the_configured_liveness_bounds() {
    let dir = scratch("bounds");
    let mut s = SegmentedSink::open_write_ahead_only(
        SegmentConfig {
            dir: dir.clone(),
            max_bytes: 1 << 20,
            max_secs: 900,
            max_disk_bytes: None,
            workload_class: None,
            heartbeat_secs: 45,
            key_epoch: None,
        },
        Provenance::default(),
    )
    .expect("open");
    s.emit(rec(1));
    s.close().expect("close");

    let name = sealed(&dir).into_iter().next().expect("a sealed segment");
    let text = fs::read_to_string(dir.join(name)).expect("read");
    let header: Value = serde_json::from_str(text.lines().next().expect("header")).expect("json");
    assert_eq!(
        header["heartbeat_secs"], 45,
        "the configured bound, never a default: {header}"
    );
    assert_eq!(header["max_segment_secs"], 900, "{header}");
}

/// The shipped form: a sealed segment converts to a verified Parquet twin, and only then does
/// the write-ahead JSONL disappear. Every test here opens the sink the shipped way, with the
/// conversion worker, where the suite above opens without it, and asserts after `close()`,
/// which drains the converter's queue. No test sleeps on the worker.
mod conversion {
    use super::{
        scratch, sink, Path, PathBuf, Provenance, Record, SegmentConfig, SegmentedSink, Sink,
    };
    use infertap::sink::segments::{PARQUET_SUFFIX, SEG_SUFFIX, TMP_SUFFIX};
    use std::fs;

    fn cfg_convert(dir: &Path, max_bytes: u64, cap: Option<u64>) -> SegmentConfig {
        SegmentConfig {
            dir: dir.to_path_buf(),
            max_bytes,
            max_secs: 3600,
            max_disk_bytes: cap,
            workload_class: None,
            heartbeat_secs: 60,
            key_epoch: None,
        }
    }

    /// Schema-valid records: the converter refuses a field whose type contradicts the contract
    /// schema, so the fixtures here carry the types the schema declares.
    fn store(i: u32) -> Record {
        let mut s = infertap::testing::store(u64::from(i));
        s.at_ms = infertap::clock::EngineMs(1_785_153_670_000.0 + f64::from(i));
        s.tier = Some("gpu".into());
        Record::Store(s)
    }

    fn names(dir: &Path, suffix: &str) -> Vec<String> {
        let mut v: Vec<String> = fs::read_dir(dir)
            .expect("read_dir")
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with("seg-") && n.ends_with(suffix))
            .collect();
        v.sort();
        v
    }

    fn plant(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let p = dir.join(name);
        fs::write(&p, bytes).expect("plant file");
        p
    }

    #[test]
    fn a_sealed_segment_becomes_its_twin_and_the_wal_is_gone() {
        let dir = scratch("convert-seal");
        let mut s =
            SegmentedSink::open(cfg_convert(&dir, 512, None), Provenance::default()).expect("open");
        for i in 0..20 {
            s.emit(store(i));
        }
        s.close().expect("close");

        let twins = names(&dir, PARQUET_SUFFIX);
        assert!(
            twins.len() >= 2,
            "a 512-byte bound over 20 records seals more than once; got {twins:?}"
        );
        assert!(
            names(&dir, SEG_SUFFIX).is_empty(),
            "every sealed WAL converted and was deleted"
        );
        assert!(names(&dir, TMP_SUFFIX).is_empty(), "no temporary survives");
        for t in &twins {
            let bytes = fs::read(dir.join(t)).expect("read twin");
            assert_eq!(&bytes[..4], b"PAR1", "{t} is not Parquet");
            assert_eq!(
                &bytes[bytes.len() - 4..],
                b"PAR1",
                "{t} has no committed footer"
            );
        }
    }

    #[test]
    fn the_reconcile_converts_a_wal_left_behind() {
        let dir = scratch("convert-reconcile");
        {
            let mut s = sink(&dir, 1 << 20, 3600, None);
            for i in 0..5 {
                s.emit(store(i));
            }
            s.close().expect("close");
        }
        assert_eq!(
            names(&dir, SEG_SUFFIX).len(),
            1,
            "one WAL awaits conversion"
        );

        // These sinks share a process ID; separate their open timestamps.
        std::thread::sleep(std::time::Duration::from_millis(2));
        SegmentedSink::open(cfg_convert(&dir, 1 << 20, None), Provenance::default())
            .expect("open")
            .close()
            .expect("close");

        assert!(
            names(&dir, SEG_SUFFIX).is_empty(),
            "the reconcile converted the predecessor's WAL"
        );
        assert_eq!(
            names(&dir, PARQUET_SUFFIX).len(),
            2,
            "the predecessor's twin plus this run's own sealed segment"
        );
    }

    #[test]
    fn the_reconcile_deletes_a_wal_whose_twin_is_committed() {
        let dir = scratch("convert-both");
        plant(&dir, "seg-100-1.jsonl", b"{\"kind\":\"segment_open\"}\n");
        let twin = plant(
            &dir,
            "seg-100-1.parquet",
            b"PAR1 committed and verified PAR1",
        );

        std::thread::sleep(std::time::Duration::from_millis(2));
        SegmentedSink::open(cfg_convert(&dir, 1 << 20, None), Provenance::default())
            .expect("open")
            .close()
            .expect("close");

        assert!(
            !dir.join("seg-100-1.jsonl").exists(),
            "the WAL is redundant"
        );
        assert!(twin.exists(), "the committed twin stands");
    }

    #[test]
    fn the_reconcile_unlinks_a_stray_temporary() {
        let dir = scratch("convert-tmp");
        plant(&dir, "seg-100-2.parquet.tmp", b"half-written");

        SegmentedSink::open(cfg_convert(&dir, 1 << 20, None), Provenance::default())
            .expect("open")
            .close()
            .expect("close");

        assert!(
            names(&dir, TMP_SUFFIX).is_empty(),
            "a temporary is never terminal; recovery unlinks it"
        );
    }

    #[test]
    fn the_disk_cap_counts_twin_bytes() {
        let dir = scratch("convert-cap");
        for seq in 0..6 {
            plant(&dir, &format!("seg-100-{seq}.parquet"), &[0u8; 4096]);
        }

        // Cap 16 KiB over 24 KiB of twins: enforcement at open drops the oldest two. The
        // non-converting sink keeps this run's own output out of the measurement; the cap
        // counts twin bytes whether or not this process is the one converting.
        super::sink(&dir, 1024, 3600, Some(16 * 1024))
            .close()
            .expect("close");

        let held: u64 = names(&dir, PARQUET_SUFFIX)
            .iter()
            .map(|n| dir.join(n).metadata().map_or(0, |m| m.len()))
            .sum();
        assert!(
            held <= 16 * 1024,
            "twin bytes are cap bytes; {held} retained over a 16384 cap"
        );
        assert!(
            !dir.join("seg-100-0.parquet").exists() && !dir.join("seg-100-1.parquet").exists(),
            "the oldest twins were the ones reclaimed"
        );
    }

    #[test]
    fn without_conversion_nothing_is_touched() {
        let dir = scratch("convert-off");
        plant(&dir, "seg-100-1.jsonl", b"{\"kind\":\"segment_open\"}\n");
        plant(
            &dir,
            "seg-100-1.parquet",
            b"PAR1 from before the mode switch PAR1",
        );
        plant(&dir, "seg-100-2.parquet.tmp", b"half-written");

        std::thread::sleep(std::time::Duration::from_millis(2));
        sink(&dir, 1 << 20, 3600, None).close().expect("close");

        assert!(
            dir.join("seg-100-1.jsonl").exists()
                && dir.join("seg-100-1.parquet").exists()
                && dir.join("seg-100-2.parquet.tmp").exists(),
            "a mode transition is the operator's to untangle, not this process's to guess at"
        );
    }
}

#[test]
fn oversized_headers_are_refused_before_touching_the_directory() {
    use infertap::sink::guard::MAX_RECORD_BYTES;
    for field in ["provenance", "workload_class"] {
        let root = scratch(&format!("header-size-{field}"));
        let dir = root.join("uncreated");
        let mut cfg = SegmentConfig::new(dir.clone());
        let mut provenance = Provenance::default();
        if field == "provenance" {
            provenance = Provenance::try_from(Map::from_iter([(
                "note".into(),
                json!("x".repeat(MAX_RECORD_BYTES)),
            )]))
            .expect("valid keys");
        } else {
            cfg.workload_class = Some("x".repeat(MAX_RECORD_BYTES));
        }
        assert_eq!(
            SegmentedSink::open(cfg, provenance)
                .expect_err("oversized header")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert!(!dir.exists());
        fs::remove_dir_all(root).expect("cleanup");
    }
}

#[test]
fn the_segment_writer_checks_record_size_without_an_outer_guard() {
    let dir = scratch("inner-size-guard");
    let mut sink = sink(&dir, 1 << 22, 300, None);
    let mut huge = infertap::testing::store(1);
    huge.extra_keys = Some(vec![infertap::record::OperatorText("x".repeat(1 << 20))]);
    sink.emit(Record::Store(huge));
    assert_eq!(
        sink.write_error().expect("size error").kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert!(sink.close().is_err());
    for path in fs::read_dir(&dir).expect("directory") {
        let bytes = fs::read(path.expect("entry").path()).expect("file");
        assert!(bytes
            .split(|b| *b == b'\n')
            .all(|line| line.len() < (1 << 20)));
    }
    fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn a_live_writer_owns_its_directory_until_dropped() {
    let dir = scratch("exclusive-writer");
    let mut first = sink(&dir, 1 << 20, 3600, None);
    let layout: Value =
        serde_json::from_str(include_str!("../conformance/delivery/layout.json")).unwrap();
    for entry in fs::read_dir(&dir).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        assert!(
            layout["shipper_exclude"]
                .as_array()
                .unwrap()
                .iter()
                .any(|pattern| {
                    let pattern = pattern.as_str().unwrap();
                    pattern
                        .strip_prefix('*')
                        .map_or(name == pattern, |suffix| name.ends_with(suffix))
                }),
            "a live writer's control files must remain on the node: {name}"
        );
    }
    first.emit(rec(1));
    first.flush().unwrap();
    let before = fs::read(dir.join(ACTIVE)).unwrap();
    let second =
        SegmentedSink::open_write_ahead_only(cfg(&dir, 1 << 20, 3600, None), Provenance::default());
    assert!(second.is_err());
    assert_eq!(fs::read(dir.join(ACTIVE)).unwrap(), before);
    drop(first);
    sink(&dir, 1 << 20, 3600, None).close().unwrap();
    assert!(sealed(&dir)
        .iter()
        .any(|n| lines(&dir.join(n)).iter().any(|r| r["kind"] == "store")));
}

#[test]
fn recovery_refuses_paths_and_existing_destinations_without_modifying_files() {
    for incarnation in ["x/../../outside", "..", "x\\outside", "123-456"] {
        let dir = scratch("recovery-refusal");
        write_orphan(&dir, incarnation, 0, "");
        let active = fs::read(dir.join(ACTIVE)).unwrap();
        let dest = dir.join("seg-123-456-0.jsonl");
        fs::write(&dest, b"sealed sentinel").unwrap();
        assert!(SegmentedSink::open_write_ahead_only(
            cfg(&dir, 1 << 20, 3600, None),
            Provenance::default()
        )
        .is_err());
        assert_eq!(fs::read(dir.join(ACTIVE)).unwrap(), active);
        assert_eq!(fs::read(dest).unwrap(), b"sealed sentinel");
    }
}

#[test]
fn recovery_bounds_reads_of_a_large_sparse_orphan() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = scratch("sparse-orphan");
    write_orphan(&dir, "123-456", 0, "");
    let mut f = fs::OpenOptions::new()
        .write(true)
        .open(dir.join(ACTIVE))
        .unwrap();
    f.seek(SeekFrom::Start(1 << 32)).unwrap();
    f.write_all(b"\n{broken").unwrap();
    drop(f);
    sink(&dir, 1 << 20, 3600, None).close().unwrap();
    let path = dir.join("seg-123-456-0.jsonl");
    assert!(fs::metadata(path).unwrap().len() > 1 << 32);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "subprocess fixture for directory ownership"]
fn directory_lock_child() {
    let dir = PathBuf::from(std::env::var_os("INFERTAP_LOCK_TEST_DIR").unwrap());
    let mut owner = sink(&dir, 1 << 20, 3600, None);
    owner.emit(rec(1));
    owner.flush().unwrap();
    fs::write(dir.join("ready"), b"ready").unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn directory_ownership_survives_contention_and_releases_after_process_death() {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = scratch("process-lock");
    let mut child = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "directory_lock_child", "--ignored"])
            .env("INFERTAP_LOCK_TEST_DIR", &dir)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !dir.join("ready").exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before acquiring lock"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "child did not acquire lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let before = fs::read(dir.join(ACTIVE)).unwrap();
    assert!(SegmentedSink::open_write_ahead_only(
        cfg(&dir, 1 << 20, 3600, None),
        Provenance::default()
    )
    .is_err());
    assert_eq!(fs::read(dir.join(ACTIVE)).unwrap(), before);
    drop(child);
    sink(&dir, 1 << 20, 3600, None).close().unwrap();
    assert!(sealed(&dir)
        .iter()
        .any(|n| lines(&dir.join(n)).iter().any(|r| r["kind"] == "store")));
}
