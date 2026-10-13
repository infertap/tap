// SPDX-License-Identifier: Apache-2.0
//! Verify the tap and packaged shippers against the delivery-layout corpus.
//! Constants, filename patterns, and shipper exclusions must agree with the pinned contract.

#![allow(clippy::unwrap_used, clippy::expect_used)] // a test that cannot fail loudly is not a test

use infertap::testing::repo_root as root;
use std::fs;

use serde_json::Value;

fn layout() -> Value {
    let path = root().join("conformance/delivery/layout.json");
    serde_json::from_str(&fs::read_to_string(path).expect("read layout.json")).expect("json")
}

fn field(v: &Value, k: &str) -> String {
    v[k].as_str()
        .unwrap_or_else(|| panic!("layout.json has no string field {k:?}"))
        .to_string()
}

#[test]
fn the_taps_constants_match_the_contract() {
    let l = layout();
    assert_eq!(
        infertap::sink::segments::ACTIVE,
        field(&l, "active_segment")
    );
    assert_eq!(
        infertap::sink::segments::SEG_PREFIX,
        field(&l, "sealed_prefix")
    );
    assert_eq!(
        infertap::sink::segments::SEG_SUFFIX,
        field(&l, "wal_suffix")
    );
    let shipped: Vec<&str> = l["shipped_suffixes"]
        .as_array()
        .expect("layout.json shipped_suffixes")
        .iter()
        .map(|v| v.as_str().expect("suffix is a string"))
        .collect();
    assert_eq!(
        shipped,
        vec![
            infertap::sink::segments::SEG_SUFFIX,
            infertap::sink::segments::PARQUET_SUFFIX,
        ]
    );
    assert_eq!(
        infertap::sink::segments::TMP_SUFFIX,
        field(&l, "conversion_tmp_suffix")
    );
}

/// Extract an `ExecStart=` command's arguments from a systemd unit.
///
/// Parsed rather than pattern-matched, and the difference matters: a pattern loose enough to
/// survive reformatting is loose enough to pass on a *wrong value*, and one tight enough to
/// catch a wrong value breaks confusingly the first time someone adds a line continuation,
/// which this unit already uses.
fn exec_start_args(unit: &str) -> Vec<String> {
    let joined = unit.replace("\\\n", " ");
    let line = joined
        .lines()
        .find(|l| l.trim_start().starts_with("ExecStart="))
        .expect("unit has an ExecStart");
    line.trim_start()
        .trim_start_matches("ExecStart=")
        .split_whitespace()
        .map(|t| t.trim_matches(['"', '\'']).to_string())
        .collect()
}

/// The files the shipper is told to leave behind are the ones whose content can still change
/// (the active WAL, a conversion temporary) plus the write-ahead form awaiting conversion.
/// Shipping any of them either stores overlapping records twice, since batch identity is a
/// content hash, or ships a form the contract retired from shipping.
#[test]
fn the_shipper_excludes_exactly_the_contracts_holdbacks() {
    let unit = fs::read_to_string(root().join("packaging/systemd/infertap-ship.service"))
        .expect("read shipper unit");
    let args = exec_start_args(&unit);

    let excluded: Vec<&str> = args
        .iter()
        .enumerate()
        .filter(|(_, a)| *a == "--exclude")
        .filter_map(|(i, _)| args.get(i + 1).map(String::as_str))
        .collect();
    let l = layout();
    let contract: Vec<&str> = l["shipper_exclude"]
        .as_array()
        .expect("layout.json shipper_exclude")
        .iter()
        .map(|v| v.as_str().expect("exclude is a string"))
        .collect();
    assert_eq!(excluded, contract);
}

/// The exclude list must hold back the active segment by NAME, not only by the `*.jsonl`
/// glob beside it: the glob exists for the WAL backlog, and a future rename of the backlog's
/// suffix must not silently expose the one file that is mid-write.
#[test]
fn the_active_segment_is_excluded_by_its_own_name() {
    let l = layout();
    let excludes: Vec<&str> = l["shipper_exclude"]
        .as_array()
        .expect("layout.json shipper_exclude")
        .iter()
        .map(|v| v.as_str().expect("exclude is a string"))
        .collect();
    assert!(
        excludes.contains(&infertap::sink::segments::ACTIVE),
        "the active segment must be named in the excludes, not merely matched by a glob"
    );
}

/// Check the dated destination layout in the unit's literal encoding:
/// $$ passes a dollar to the shell, %% passes a percent to date, and %H
/// is the hostname expanded by systemd.
#[test]
fn the_shipper_destination_renders_the_contracts_dated_layout() {
    let l = layout();
    // Both sides pin the same stated layout: if layout.json moves, this fails HERE first,
    // and the unit and this test move with it in one commit.
    assert_eq!(
        field(&l, "dest_key_layout"),
        "<prefix>/<utc yyyy-mm-dd>/<utc hh>/<node>/<sealed segment file>"
    );

    let unit = fs::read_to_string(root().join("packaging/systemd/infertap-ship.service"))
        .expect("read shipper unit");
    let joined = unit.replace("\\\n", " ");
    let line = joined
        .lines()
        .find(|l| l.trim_start().starts_with("ExecStart="))
        .expect("unit has an ExecStart");
    let dest = line
        .split("move /var/lib/infertap/kv ")
        .nth(1)
        .expect("the destination follows the source directory")
        .split(" --exclude")
        .next()
        .expect("the destination precedes the excludes")
        .trim()
        .trim_matches('"');
    assert_eq!(
        dest, "$${INFERTAP_DEST}$$(date -u +%%Y-%%m-%%d/%%H)/%H/",
        "prefix, then the shipper's UTC date and hour, then the node, then the file"
    );

    // The template concatenates the prefix directly, so the deployment's own example must
    // carry the trailing slash the concatenation assumes.
    let env = fs::read_to_string(root().join("packaging/systemd/ship.env.example"))
        .expect("read ship.env.example");
    let dest_line = env
        .lines()
        .find(|l| l.starts_with("INFERTAP_DEST="))
        .expect("the example declares a destination");
    assert!(
        dest_line.trim_end().ends_with("/kv/"),
        "the destination prefix ends in the stream name, with its own trailing slash"
    );
}

/// A denylist, not an allowlist. The contract states the holdbacks by name, and an allowlist
/// would silently skip any terminal class the contract adds later.
#[test]
fn the_shipper_filters_by_exclusion_not_by_allowlist() {
    let unit = fs::read_to_string(root().join("packaging/systemd/infertap-ship.service"))
        .expect("read shipper unit");
    let args = exec_start_args(&unit);
    assert!(
        !args.iter().any(|a| a == "--include"),
        "the contract is a denylist: an allowlist would skip a terminal class it never named"
    );
}

/// The ring can unlink a segment between the shipper's listing and its open. A shipper that
/// treats that as a retryable failure stalls its queue, turning a bounded, recorded drop into
/// total shipping stoppage.
#[test]
fn the_shipper_tolerates_a_vanished_source_file() {
    let unit = fs::read_to_string(root().join("packaging/systemd/infertap-ship.service"))
        .expect("read shipper unit");
    assert!(
        exec_start_args(&unit)
            .iter()
            .any(|a| a == "--ignore-errors"),
        "a vanished source file must be an expected outcome, not a retryable error"
    );
}

/// Deleting on successful upload is what makes "still on disk" mean "not yet shipped": the
/// property the disk-cap ring depends on to drop only unshipped data.
#[test]
fn the_shipper_deletes_on_success() {
    let unit = fs::read_to_string(root().join("packaging/systemd/infertap-ship.service"))
        .expect("read shipper unit");
    let args = exec_start_args(&unit);
    assert!(
        args.iter().any(|a| a == "move"),
        "rclone `copy` would leave the source behind, so the ring could never tell shipped \
         data from unshipped"
    );
}

/// Shipping must not outrun sealing by so much that the arrival cadence stops being a
/// staleness signal: a consumer reads "no new segment past the seal interval" as an instance
/// that stopped or is losing its tail.
#[test]
fn the_ship_interval_is_below_the_default_seal_interval() {
    let timer = fs::read_to_string(root().join("packaging/systemd/infertap-ship.timer"))
        .expect("read shipper timer");
    let secs: u64 = timer
        .lines()
        .find_map(|l| l.trim().strip_prefix("OnUnitActiveSec="))
        .expect("timer sets OnUnitActiveSec")
        .trim()
        .parse()
        .expect("OnUnitActiveSec is seconds");
    assert!(
        secs < infertap::sink::segments::DEFAULT_MAX_SECS,
        "shipping every {secs}s against a {}s seal interval leaves arrival lag above the \
         cadence a consumer measures staleness against",
        infertap::sink::segments::DEFAULT_MAX_SECS
    );
}

/// Pin rclone runtime parallelism independently of the host CPU count,
/// so the configured thread limit has a predictable operating margin.
#[test]
fn the_shippers_thread_count_is_pinned_against_the_host() {
    let unit = fs::read_to_string(root().join("packaging/systemd/infertap-ship.service"))
        .expect("read shipper unit");
    assert!(
        unit.contains("Environment=GOMAXPROCS="),
        "an unpinned Go runtime sizes itself from nproc: the shipper's footprint becomes \
         a function of the host, and every bound in this unit was derived on small boxes"
    );
    let cage: u64 = unit
        .lines()
        .find_map(|l| l.strip_prefix("TasksMax="))
        .expect("the shipper declares a thread cage")
        .trim()
        .parse()
        .expect("TasksMax is a number");
    let procs: u64 = unit
        .lines()
        .find_map(|l| l.strip_prefix("Environment=GOMAXPROCS="))
        .expect("checked above")
        .trim()
        .parse()
        .expect("GOMAXPROCS is a number");
    assert!(
        cage >= procs * 16,
        "the cage must leave the pinned runtime real headroom: GOMAXPROCS procs plus the \
         runtime's own threads plus a blocked-syscall thread per in-flight transfer. A \
         cage of {cage} over {procs} procs meters parallelism by accident"
    );
}

// --- the Kubernetes shipper: the same contract, asserted from the ConfigMap script ---
//
// The sidecar's shipper is a script in `packaging/kubernetes/tap-config.yaml`, one copy
// mounted by every manifest. It shipped with `--include 'seg-*.parquet'` and a flat
// destination for one release while the systemd unit was asserted here and it was not,
// and a consumer's dated discovery could not bound what it shipped. Everything asserted
// of the unit above is asserted of the script below.

use infertap::testing::sidecar_ship_script;

/// The `rclone move ...` command of the script, continuations joined, as tokens.
fn sidecar_rclone_args(script: &str) -> Vec<String> {
    let joined = script.replace("\\\n", " ");
    let line = joined
        .lines()
        .map(str::trim_start)
        .find(|l| l.starts_with("rclone move "))
        .expect("the script runs rclone move");
    line.split_whitespace()
        .map(|t| t.trim_matches(['"', '\'']).to_string())
        .collect()
}

#[test]
fn the_sidecar_shipper_excludes_exactly_the_contracts_holdbacks() {
    let args = sidecar_rclone_args(&sidecar_ship_script());
    let excluded: Vec<&str> = args
        .iter()
        .enumerate()
        .filter(|(_, a)| *a == "--exclude")
        .filter_map(|(i, _)| args.get(i + 1).map(String::as_str))
        .collect();
    let l = layout();
    let contract: Vec<&str> = l["shipper_exclude"]
        .as_array()
        .expect("layout.json shipper_exclude")
        .iter()
        .map(|v| v.as_str().expect("exclude is a string"))
        .collect();
    assert_eq!(excluded, contract);
    assert!(
        !args.iter().any(|a| a == "--include"),
        "the contract is a denylist: an allowlist would skip a terminal class it never named"
    );
    assert!(
        args.iter().any(|a| a == "--ignore-errors"),
        "a vanished source file must be an expected outcome, not a retryable error"
    );
    assert!(
        args.iter().any(|a| a == "move"),
        "copy would leave the source behind"
    );
}

/// The sidecar's destination renders the contract's dated layout with the pod in the node
/// slot: inside a cluster the pod is the credential and identity unit.
#[test]
fn the_sidecar_shipper_destination_renders_the_contracts_dated_layout() {
    let l = layout();
    assert_eq!(
        field(&l, "dest_key_layout"),
        "<prefix>/<utc yyyy-mm-dd>/<utc hh>/<node>/<sealed segment file>"
    );
    let script = sidecar_ship_script();
    let joined = script.replace("\\\n", " ");
    let line = joined
        .lines()
        .map(str::trim_start)
        .find(|l| l.starts_with("rclone move "))
        .expect("the script runs rclone move");
    let dest = line
        .split("move \"$INFERTAP_SEGMENTS/kv\" ")
        .nth(1)
        .expect("the destination follows the source directory")
        .split("--exclude")
        .next()
        .expect("the destination precedes the excludes")
        .trim()
        .trim_matches('"');
    assert_eq!(
        dest, "${INFERTAP_DEST}$(date -u +%Y-%m-%d/%H)/${POD_NAME}/",
        "prefix, then the shipper's UTC date and hour, then the pod, then the file"
    );

    // The template concatenates the prefix directly, so the Secret's example must carry
    // the trailing slash the concatenation assumes.
    let yaml = fs::read_to_string(root().join("packaging/kubernetes/tap-config.yaml"))
        .expect("read tap-config.yaml");
    let dest_line = yaml
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("INFERTAP_DEST:"))
        .expect("the Secret example declares a destination");
    assert!(
        dest_line.trim_end_matches('"').ends_with("/kv/"),
        "the destination prefix ends in the stream name, with its own trailing slash"
    );
}

/// Every manifest that runs the shipper mounts the one script and hands it the pod name;
/// a manifest that inlined its own copy would be the divergence this file exists to catch.
#[test]
fn every_manifest_runs_the_one_shipper_script() {
    for name in ["tap-sidecar.yaml", "example-vllm-deployment.yaml"] {
        let text = fs::read_to_string(root().join("packaging/kubernetes").join(name))
            .expect("read the manifest");
        assert!(
            text.contains(r#"command: ["/bin/sh", "/scripts/ship.sh"]"#),
            "{name} does not run the shared shipper script"
        );
        assert!(
            !text.contains("rclone move"),
            "{name} inlines its own rclone command instead of the shared script"
        );
        assert!(
            text.contains("name: infertap-shipper-script"),
            "{name} does not mount the shipper script ConfigMap"
        );
    }
}

/// The example deployment is the sidecar patch applied to a plain engine pod: the two
/// added containers and the volumes are line-identical between the two files, comments
/// aside, except the served model named in the provenance. Divergence here is the class
/// the shipper had.
#[test]
fn the_example_deployment_carries_the_sidecar_patch_verbatim() {
    fn added_block(name: &str) -> Vec<String> {
        let text = fs::read_to_string(root().join("packaging/kubernetes").join(name))
            .expect("read the manifest");
        let start = text
            .find("        - name: tap\n")
            .expect("the tap container");
        text[start..]
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .map(str::to_string)
            .collect()
    }
    let patch = added_block("tap-sidecar.yaml");
    let example = added_block("example-vllm-deployment.yaml");
    assert_eq!(
        patch.len(),
        example.len(),
        "the added containers and volumes differ in length between the patch and the example"
    );
    for (p, e) in patch.iter().zip(&example) {
        if p.contains("\\\"model\\\":") && e.contains("\\\"model\\\":") {
            continue; // the one permitted difference: the served model's name
        }
        assert_eq!(p, e, "the patch and the example diverge");
    }
}
