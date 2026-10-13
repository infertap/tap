// SPDX-License-Identifier: Apache-2.0
//! The DCO gate must distinguish a signed range from a range it cannot inspect.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::process::{Command, Output};

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args([
            "-c",
            "user.name=Test Contributor",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

fn check(dir: &Path, args: &[&str]) -> Output {
    Command::new("bash")
        .arg(dir.join("scripts/dco-check.sh"))
        .args(args)
        .current_dir(dir)
        .output()
        .expect("DCO script")
}

#[test]
fn the_dco_gate_rejects_invalid_revisions_and_unsigned_commits() {
    let dir = infertap::testing::scratch("dco-gate");
    std::fs::create_dir(dir.join("scripts")).unwrap();
    std::fs::copy(
        infertap::testing::repo_root().join("scripts/dco-check.sh"),
        dir.join("scripts/dco-check.sh"),
    )
    .unwrap();
    git(&dir, &["init", "-q"]);
    git(
        &dir,
        &["commit", "--allow-empty", "-s", "-m", "Signed root"],
    );
    let root = git(&dir, &["rev-parse", "HEAD"]);
    assert!(check(&dir, &[]).status.success());
    git(
        &dir,
        &["commit", "--allow-empty", "-s", "-m", "Signed child"],
    );
    assert!(check(&dir, &[&root, "HEAD"]).status.success());
    assert!(
        check(&dir, &["0000000000000000000000000000000000000000", "HEAD"])
            .status
            .success()
    );
    for args in [
        ["missing", "HEAD"],
        ["HEAD", "missing"],
        ["HEAD..missing", "HEAD"],
    ] {
        let result = check(&dir, &args);
        assert!(
            !result.status.success(),
            "invalid revisions passed: {args:?}"
        );
        assert!(!String::from_utf8_lossy(&result.stdout).contains("are signed off"));
    }
    git(&dir, &["commit", "--allow-empty", "-m", "Unsigned child"]);
    assert!(!check(&dir, &[&root, "HEAD"]).status.success());
    std::fs::remove_dir_all(dir).unwrap();
}
