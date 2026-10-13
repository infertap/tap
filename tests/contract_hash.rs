// SPDX-License-Identifier: Apache-2.0
//! Verify the vendored corpus against `CONTRACT_HASH` and `SPEC_PIN` offline.
//!
//! Hash files in sorted relative-path order, excluding `CONTRACT_HASH`. Prefix each
//! path and body with its eight-byte big-endian length, matching the upstream tool.
//! Fixture changes must originate upstream; this test cannot update the pin.
//! CI separately resolves the pinned tag and compares the vendored tree upstream.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            files_under(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn corpus_hash(corpus: &Path) -> String {
    let mut files = Vec::new();
    files_under(corpus, &mut files);
    let mut rels: Vec<(String, PathBuf)> = files
        .into_iter()
        .filter(|p| p.file_name().is_some_and(|n| n != "CONTRACT_HASH"))
        .map(|p| {
            let rel = p
                .strip_prefix(corpus)
                .expect("under corpus")
                .to_string_lossy()
                .replace('\\', "/");
            (rel, p)
        })
        .collect();
    rels.sort();
    let mut h = Sha256::new();
    for (rel, path) in rels {
        let body = std::fs::read(&path).expect("read fixture");
        h.update((rel.len() as u64).to_be_bytes());
        h.update(rel.as_bytes());
        h.update((body.len() as u64).to_be_bytes());
        h.update(&body);
    }
    hex::encode(h.finalize())
}

fn pin_value(key: &str) -> String {
    let pin = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/SPEC_PIN"))
        .expect("SPEC_PIN at the repo root");
    pin.lines()
        .find_map(|l| l.strip_prefix(&format!("{key}=")).map(str::to_string))
        .unwrap_or_else(|| panic!("SPEC_PIN carries no {key}"))
}

#[test]
fn the_vendored_corpus_matches_the_blessing_and_the_pin() {
    let corpus = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/conformance"));
    let computed = corpus_hash(corpus);
    let blessed = std::fs::read_to_string(corpus.join("CONTRACT_HASH"))
        .expect("CONTRACT_HASH")
        .trim()
        .to_string();
    assert_eq!(
        computed, blessed,
        "vendored bytes disagree with the upstream blessing: a fixture was edited here or \
         the vendor is half-synced. Re-vendor with scripts/sync-spec.sh; never edit fixtures"
    );
    assert_eq!(
        computed,
        pin_value("corpus_sha256"),
        "vendored bytes disagree with SPEC_PIN: bump the pin with scripts/sync-spec.sh"
    );
}
