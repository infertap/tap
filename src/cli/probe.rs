// SPDX-License-Identifier: Apache-2.0
//! Output-freshness check for exec-based liveness probes.
//!
//! The probe compares the newest eligible output timestamp with the configured
//! heartbeat or segment interval. Fresh output returns exit status 0; missing or
//! stale output returns 1. Both results include a one-line summary on stdout.
//! A startup probe or initial delay should cover initial segment creation.

use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, SystemTime};

use crate::cli::opts::{Opts, Output};
use crate::cli::output::target;
use infertap::sink::segments::DEFAULT_MAX_SECS;

/// The freshness bound the configuration implies.
pub(crate) fn bound(o: &Opts) -> Duration {
    let poll = Duration::from_millis(o.poll_timeout_ms);
    if o.heartbeat_secs > 0 {
        Duration::from_secs(o.heartbeat_secs.saturating_mul(2)).saturating_add(poll)
    } else {
        Duration::from_secs(o.segment_secs.unwrap_or(DEFAULT_MAX_SECS).saturating_mul(2))
            .saturating_add(poll)
    }
}

/// Age of the newest write in `dir`, or `None` if the directory does not exist or holds
/// no readable file.
pub(crate) fn newest_write_age(dir: &Path, now: SystemTime) -> Option<Duration> {
    let mut newest: Option<SystemTime> = None;
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let is_segment = name.strip_prefix("seg-").is_some_and(|rest| {
            let stem = rest
                .strip_suffix(".parquet")
                .or_else(|| rest.strip_suffix(".jsonl"));
            stem.and_then(|s| s.rsplit_once('-'))
                .is_some_and(|(incarnation, seq)| {
                    !incarnation.is_empty() && seq.parse::<u64>().is_ok()
                })
        });
        if name != "active.jsonl" && !is_segment {
            continue;
        }
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        if let Ok(modified) = entry.metadata().and_then(|m| m.modified()) {
            newest = Some(newest.map_or(modified, |n| n.max(modified)));
        }
    }
    let newest = newest?;
    Some(now.duration_since(newest).unwrap_or_default())
}

pub(crate) fn run(o: &Opts) -> ExitCode {
    let Some(output @ Output::Segments(_)) = &o.output else {
        println!("probe stale: --out-dir is not configured, so there is no stream to check");
        return ExitCode::FAILURE;
    };
    let dir = target(output);
    let bound = bound(o);
    match newest_write_age(&dir, SystemTime::now()) {
        Some(age) if age <= bound => {
            println!(
                "probe ok: {} written {}s ago (bound {}s)",
                dir.display(),
                age.as_secs(),
                bound.as_secs()
            );
            ExitCode::SUCCESS
        }
        Some(age) => {
            println!(
                "probe stale: {} last written {}s ago, past the {}s bound",
                dir.display(),
                age.as_secs(),
                bound.as_secs()
            );
            ExitCode::FAILURE
        }
        None => {
            println!(
                "probe stale: {} is absent or empty; the tap has not opened a segment",
                dir.display()
            );
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{bound, newest_write_age};
    use crate::cli::opts::parse;
    use std::time::{Duration, SystemTime};

    #[test]
    fn a_sidecar_probe_uses_rank_config_without_subscription_endpoints() {
        let dir = infertap::testing::scratch("probe-sidecar-config");
        std::fs::create_dir_all(dir.join("kv")).unwrap();
        std::fs::write(dir.join("kv/active.jsonl"), "").unwrap();
        let args = vec![
            "probe".to_string(),
            "--config".to_string(),
            "tap.toml".to_string(),
            "--out-dir".to_string(),
            dir.to_string_lossy().into_owned(),
        ];
        let opts = crate::cli::opts::Opts::parse_with(&args, std::iter::empty(), &|_| {
            Ok("dp_local = 2\ndp_max = 4\nkey_file = '/secrets/pseudonym.key'\n".into())
        })
        .expect("the probe does not require subscription endpoints");
        assert!(opts.endpoints.is_empty());
        assert_eq!(super::run(&opts), std::process::ExitCode::SUCCESS);
        std::fs::remove_file(dir.join("kv/active.jsonl")).unwrap();
        assert_eq!(super::run(&opts), std::process::ExitCode::FAILURE);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_bound_is_twice_the_heartbeat_or_twice_the_segment_age() {
        let o = parse(&["--raw-ids", "--heartbeat-secs", "30"]).expect("parse");
        assert_eq!(
            bound(&o),
            Duration::from_secs(60) + Duration::from_millis(250)
        );
        let o = parse(&[
            "--raw-ids",
            "--heartbeat-secs",
            "0",
            "--segment-secs",
            "120",
        ])
        .expect("parse");
        assert_eq!(
            bound(&o),
            Duration::from_secs(240) + Duration::from_millis(250)
        );
    }

    #[test]
    fn control_files_and_directories_do_not_make_a_stream_healthy() {
        let dir = infertap::testing::scratch("probe-controls");
        std::fs::create_dir_all(&dir).expect("mkdir");
        for name in [".writer.lock.jsonl", "unrelated", "seg-1-0.parquet.tmp"] {
            std::fs::write(dir.join(name), b"x").expect("write");
        }
        std::fs::create_dir(dir.join("seg-1-0.parquet")).expect("directory");
        assert!(newest_write_age(&dir, SystemTime::now()).is_none());
        std::fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn extreme_intervals_saturate_without_panicking() {
        let mut o = parse(&["--raw-ids"]).expect("parse");
        o.heartbeat_secs = u64::MAX;
        o.poll_timeout_ms = u64::MAX;
        assert_eq!(bound(&o), Duration::MAX);
        o.heartbeat_secs = 0;
        o.segment_secs = Some(u64::MAX);
        assert_eq!(bound(&o), Duration::MAX);
    }

    #[test]
    fn the_newest_write_decides_and_an_absent_directory_is_stale() {
        let dir = std::env::temp_dir().join(format!("infertap-probe-{}", std::process::id()));
        assert!(newest_write_age(&dir, SystemTime::now()).is_none());
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(dir.join("seg-1-0.parquet"), b"x").expect("write");
        let old = dir.join("active.jsonl");
        std::fs::write(&old, b"x").expect("write");
        let f = std::fs::File::options()
            .write(true)
            .open(&old)
            .expect("open");
        f.set_modified(SystemTime::now() - Duration::from_secs(3600))
            .expect("set an old mtime");
        let age = newest_write_age(&dir, SystemTime::now()).expect("some file");
        assert!(
            age < Duration::from_secs(60),
            "the fresh parquet decides: {age:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
