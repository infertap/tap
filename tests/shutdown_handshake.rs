// SPDX-License-Identifier: Apache-2.0
//! The tap's half of the shutdown handshake, run against the real binary: the incarnation
//! marker at open, the closed marker after the last conversion, and a second incarnation
//! in the same root replacing the first's markers. Needs the live transport, so it runs
//! under `--all-features`, where CI runs it; the shipper's half is `tests/shipper_drain.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // a test that cannot fail loudly is not a test
#![cfg(feature = "zmq")]

use std::fs;
use std::path::Path;
use std::process::Command;

use infertap::sink::segments::{CLOSED_MARKER_PREFIX, INCARNATION_MARKER};

/// An address nothing listens on: bound by the kernel's choice and released.
fn silent_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

/// One bounded run of the tap against a silent endpoint: it opens, hears nothing, seals
/// on `--duration`, converts, and closes. Returns the incarnation it wrote and the exit
/// status.
fn run(root: &Path) -> (String, std::process::ExitStatus) {
    let status = Command::new(env!("CARGO_BIN_EXE_infertap"))
        .args([
            "--engine-endpoint",
            &format!("e=tcp://{}", silent_address().trim_start_matches("tcp://")),
            "--raw-ids",
            "--out-dir",
            root.to_str().unwrap(),
            "--duration",
            "1",
            "--poll-timeout-ms",
            "50",
        ])
        .status()
        .expect("the tap runs");
    let incarnation =
        fs::read_to_string(root.join(INCARNATION_MARKER)).expect("the incarnation marker");
    (incarnation, status)
}

/// A clean bounded run: exits 0. Returns the incarnation.
fn run_once(root: &Path) -> String {
    let (incarnation, status) = run(root);
    assert!(status.success(), "a bounded run exits clean: {status:?}");
    incarnation
}

fn closed_marker(root: &Path, incarnation: &str) -> String {
    fs::read_to_string(root.join(format!("{CLOSED_MARKER_PREFIX}{incarnation}")))
        .expect("the closed marker")
}

fn markers(root: &Path) -> Vec<String> {
    let mut out: Vec<String> = fs::read_dir(root)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with(CLOSED_MARKER_PREFIX))
        .collect();
    out.sort();
    out
}

#[test]
fn the_tap_names_its_incarnation_at_open_and_marks_it_closed_after_conversion() {
    let root = infertap::testing::scratch("handshake");
    let first = run_once(&root);
    assert!(
        !first.is_empty() && !first.contains('\n'),
        "one incarnation name: {first:?}"
    );
    // The closed marker names the same incarnation and reports nothing left unconverted,
    // and by the time it exists the last segment is its shipped form: the write-ahead
    // file is gone and the twin is there.
    assert_eq!(
        markers(&root),
        vec![format!("{CLOSED_MARKER_PREFIX}{first}")]
    );
    assert_eq!(closed_marker(&root, &first), "0");
    assert!(
        root.join("kv")
            .join(format!("seg-{first}-0.parquet"))
            .exists(),
        "the final segment converted before the marker was written"
    );
    assert!(!root
        .join("kv")
        .join(format!("seg-{first}-0.jsonl"))
        .exists());
    assert!(!root.join("kv").join("active.jsonl").exists());

    // A second incarnation in the same root: a new name, and the first's closed marker
    // is gone, so a shipper draining later cannot mistake it for this one's.
    let second = run_once(&root);
    assert_ne!(first, second);
    assert_eq!(
        markers(&root),
        vec![format!("{CLOSED_MARKER_PREFIX}{second}")]
    );
    let _ = fs::remove_dir_all(&root);
}

/// A closed incarnation is not a converted one. A segment the converter cannot turn into
/// its shipped form is still on disk at close; the marker reports it, and the run exits
/// nonzero saying so, because in a pod that segment is lost with the emptyDir. Before
/// this, the tap exited 0 with the marker written and the segment left behind, and the
/// shipper reported success over a segment it excludes.
#[test]
fn a_close_that_leaves_a_segment_unconverted_reports_it_and_fails() {
    let root = infertap::testing::scratch("handshake-unconverted");
    let kv = root.join("kv");
    fs::create_dir_all(&kv).unwrap();
    // A poison write-ahead segment from a previous incarnation: the open's reconcile
    // queues it, the converter refuses it, and it is still there at close.
    fs::write(
        kv.join("seg-1-0.jsonl"),
        "{\"kind\":\"segment_open\"}\n{\"kind\":\"store\",\"n_tokens\":\"not a number\"}\n",
    )
    .unwrap();
    let (incarnation, status) = run(&root);
    assert_eq!(
        status.code(),
        Some(1),
        "a run that leaves a segment unconverted fails visibly"
    );
    assert_eq!(
        closed_marker(&root, &incarnation),
        "1",
        "the marker reports what was left"
    );
    assert!(
        kv.join("seg-1-0.jsonl").exists(),
        "the segment is still on disk, unshippable"
    );
    // The run's own segment converted: only the poison one is reported.
    assert!(kv.join(format!("seg-{incarnation}-0.parquet")).exists());
    let _ = fs::remove_dir_all(&root);
}
