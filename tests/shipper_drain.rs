// SPDX-License-Identifier: Apache-2.0
//! The shipper's termination handshake, run for real: the sidecar's `ship.sh` under `sh`
//! with a stub in rclone's place, signalled the way Kubernetes signals it, against the
//! marker files the tap writes. Pinned: the tap closes after the signal and the late
//! segment ships; the tap closed before the signal and the final pass runs at once; a
//! marker from an earlier incarnation does not end the wait; a missing incarnation is
//! waited for and never taken as done; a close that left segments unconverted is a
//! failure; a slow backlog cannot outrun the wait; and an unavailable destination ends in
//! a bounded, nonzero, logged drain.

#![allow(clippy::unwrap_used, clippy::expect_used)] // a test that cannot fail loudly is not a test

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use infertap::sink::segments::{CLOSED_MARKER_PREFIX, INCARNATION_MARKER};

/// A root with the tap's marker for `incarnation` and one converted segment, plus a stub
/// `rclone` beside it that records every invocation and the source listing at the time.
struct Rig {
    root: PathBuf,
    bin: PathBuf,
    log: PathBuf,
}

impl Rig {
    fn new(tag: &str, incarnation: &str) -> Self {
        use std::os::unix::fs::PermissionsExt as _;
        let root = infertap::testing::scratch(&format!("ship-{tag}"));
        fs::create_dir_all(root.join("kv")).unwrap();
        fs::write(root.join(INCARNATION_MARKER), incarnation).unwrap();
        fs::write(
            root.join("kv").join(format!("seg-{incarnation}-0.parquet")),
            b"twin",
        )
        .unwrap();
        let bin = root.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let log = root.join("rclone.log");
        let stub = format!(
            "#!/bin/sh\n\
             {{ printf 'ARGS:'; for a in \"$@\"; do printf ' %s' \"$a\"; done; printf '\\n'; \
             printf 'FILES:'; ls \"$2\" 2>/dev/null | tr '\\n' ' '; printf '\\n'; }} >> \"{}\"\n\
             [ \"${{STUB_FAIL:-0}}\" = 1 ] && exit 1\n\
             exit 0\n",
            log.display()
        );
        let rclone = bin.join("rclone");
        fs::write(&rclone, stub).unwrap();
        fs::set_permissions(&rclone, fs::Permissions::from_mode(0o755)).unwrap();
        Self { root, bin, log }
    }

    fn kv(&self) -> PathBuf {
        self.root.join("kv")
    }

    /// Start the shipper with a short drain budget, the stub on the path, and the root
    /// pointed here.
    fn start(&self, wait_secs: u32, fail: bool) -> Child {
        let script = self.root.join("ship.sh");
        fs::write(&script, infertap::testing::sidecar_ship_script()).unwrap();
        let path = format!(
            "{}:{}",
            self.bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        Command::new("sh")
            .arg(&script)
            .env("PATH", path)
            .env("POD_NAME", "vllm-0")
            .env("INFERTAP_DEST", "s3:bucket/kv/")
            .env("INFERTAP_SEGMENTS", &self.root)
            .env("INFERTAP_PASS_SECS", "2")
            .env("INFERTAP_DRAIN_WAIT_SECS", wait_secs.to_string())
            .env("INFERTAP_DRAIN_UPLOAD_SECS", "3")
            .env("STUB_FAIL", if fail { "1" } else { "0" })
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("sh runs the shipper")
    }

    /// The stub's log: each invocation as (args, files listed at the time).
    fn calls(&self) -> Vec<(String, String)> {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let mut out = Vec::new();
        let mut lines = text.lines();
        while let Some(args) = lines.next() {
            let files = lines.next().unwrap_or("");
            out.push((
                args.trim_start_matches("ARGS:").trim().to_string(),
                files.trim_start_matches("FILES:").trim().to_string(),
            ));
        }
        out
    }

    /// Wait for the shipper's `n`th pass, then for the shell to park in its `wait` on the
    /// sleeper: the state a running shipper is in almost all the time, and the state the
    /// signal must land in. Sent while the pass's `rclone` is still being reaped, bash 3.2
    /// defers the trap to the end of the next `wait`; the shipper's own shell, busybox
    /// ash, does not, and the test measures the handshake, not that quirk. The sleeper's
    /// existence is polled rather than assumed after a pause, because a fixed pause loses
    /// under load.
    fn wait_for_calls(&self, child: &Child, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.calls().len() < n {
            assert!(Instant::now() < deadline, "the shipper never ran its pass");
            std::thread::sleep(Duration::from_millis(50));
        }
        let parent = child.id().to_string();
        let parked = || {
            Command::new("pgrep")
                .args(["-P", &parent, "-x", "sleep"])
                .output()
                .is_ok_and(|o| o.status.success())
        };
        while !parked() {
            assert!(
                Instant::now() < deadline,
                "the shipper never parked in its wait"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// A pass the drain's wait loop ran: it carries `--retries 1` beside `--min-age`,
    /// which the periodic pass does not. Its presence is what establishes that the
    /// shipper entered its wait; a sleep cannot.
    fn wait_passes(&self) -> usize {
        self.calls()
            .iter()
            .filter(|(args, _)| args.contains("--min-age 2s") && args.contains("--retries 1"))
            .count()
    }

    /// Block until the drain's wait loop has run at least one pass.
    fn wait_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.wait_passes() == 0 {
            assert!(
                Instant::now() < deadline,
                "the shipper never entered its wait"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// The tap's closed marker for `incarnation`, reporting `unconverted` segments left.
    fn close_tap_with(&self, incarnation: &str, unconverted: usize) {
        fs::write(
            self.root
                .join(format!("{CLOSED_MARKER_PREFIX}{incarnation}")),
            unconverted.to_string(),
        )
        .unwrap();
    }

    /// A complete close: everything converted.
    fn close_tap(&self, incarnation: &str) {
        self.close_tap_with(incarnation, 0);
    }
}

fn terminate(child: &Child) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("kill runs");
    assert!(status.success());
}

/// Wait for the shipper to exit, bounded, and return its status and stderr.
fn finish(mut child: Child, within: Duration) -> (i32, String) {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut err = String::new();
            if let Some(mut e) = child.stderr.take() {
                use std::io::Read as _;
                e.read_to_string(&mut err).unwrap();
            }
            return (status.code().unwrap_or(-1), err);
        }
        assert!(
            Instant::now() < deadline,
            "the shipper did not exit within {within:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The tap closes after the signal: the shipper keeps shipping, the late segment lands
/// during the wait, and the final pass carries it, with no --min-age to exclude it.
#[test]
fn a_tap_that_closes_after_the_signal_still_ships_its_last_segment() {
    let rig = Rig::new("close-after", "100-1");
    let child = rig.start(10, false);
    rig.wait_for_calls(&child, 1);
    terminate(&child);
    // The shipper is in its wait: at least one wait pass has run. Only then does the
    // tap's last segment convert, and the tap close.
    rig.wait_entered();
    fs::write(rig.kv().join("seg-100-1-1.parquet"), b"late twin").unwrap();
    rig.close_tap("100-1");
    let (code, err) = finish(child, Duration::from_secs(15));
    assert_eq!(code, 0, "{err}");
    let calls = rig.calls();
    let (last_args, last_files) = calls.last().unwrap();
    assert!(
        last_files.contains("seg-100-1-1.parquet"),
        "the final pass saw the late segment: {last_files}"
    );
    assert!(
        !last_args.contains("--min-age"),
        "the final pass must not exclude a fresh segment: {last_args}"
    );
    assert!(last_args.contains("--max-duration 3s"), "{last_args}");
    let _ = fs::remove_dir_all(&rig.root);
}

/// The tap closed before the signal: the marker is already there, so the final pass
/// runs at once, well inside the wait budget.
#[test]
fn a_tap_that_closed_before_the_signal_is_shipped_at_once() {
    let rig = Rig::new("close-before", "200-1");
    fs::write(rig.kv().join("seg-200-1-1.parquet"), b"last twin").unwrap();
    rig.close_tap("200-1");
    let child = rig.start(30, false);
    rig.wait_for_calls(&child, 1);
    let signalled = Instant::now();
    terminate(&child);
    let (code, err) = finish(child, Duration::from_secs(10));
    assert_eq!(code, 0, "{err}");
    assert!(
        signalled.elapsed() < Duration::from_secs(5),
        "no wait when the tap is already closed"
    );
    let (_, last_files) = rig.calls().last().unwrap().clone();
    assert!(last_files.contains("seg-200-1-1.parquet"));
    let _ = fs::remove_dir_all(&rig.root);
}

/// A marker from an earlier incarnation does not end the wait: the shipper waits on the
/// incarnation that is live at the signal, runs its budget out, ships what is converted,
/// and says so with a nonzero exit.
#[test]
fn a_stale_marker_does_not_end_the_wait() {
    let rig = Rig::new("stale", "300-2");
    rig.close_tap("300-1"); // an earlier tap container's marker
    let child = rig.start(2, false);
    rig.wait_for_calls(&child, 1);
    let signalled = Instant::now();
    terminate(&child);
    rig.wait_entered();
    let (code, err) = finish(child, Duration::from_secs(15));
    assert_eq!(code, 1, "a drain cut short is a failure: {err}");
    assert!(err.contains("did not close within 2s"), "{err}");
    // The script's clock is whole seconds and it adds one to make the wait at least the
    // budget; measured from the signal, the wait is the budget or more.
    assert!(
        signalled.elapsed() >= Duration::from_secs(2),
        "the wait ran its budget before giving up: {:?}",
        signalled.elapsed()
    );
    assert!(rig.wait_passes() >= 1, "the wait shipped while it waited");
    assert!(
        rig.calls().len() > rig.wait_passes() + 1,
        "the final pass still ran after the budget"
    );
    let _ = fs::remove_dir_all(&rig.root);
}

/// An unavailable destination ends in a bounded, visibly unsuccessful drain: the final
/// pass fails, the exit code says so, and the log names it.
#[test]
fn an_unavailable_destination_is_a_bounded_and_visible_failure() {
    let rig = Rig::new("dest-down", "400-1");
    rig.close_tap("400-1");
    let child = rig.start(5, true);
    rig.wait_for_calls(&child, 1);
    terminate(&child);
    let (code, err) = finish(child, Duration::from_secs(10));
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("did not deliver"), "{err}");
    let _ = fs::remove_dir_all(&rig.root);
}

/// No incarnation marker is not nothing to wait for: a tap that has not opened yet, or
/// could not write its marker, is waited for like a closed one, and a wait that ends
/// without one exits 1, after shipping what is there. A marker that appears during the
/// wait is honoured.
#[test]
fn a_missing_incarnation_is_waited_for_and_is_a_failure_if_it_never_comes() {
    let rig = Rig::new("no-incarnation", "500-1");
    fs::remove_file(rig.root.join(INCARNATION_MARKER)).unwrap();
    let child = rig.start(2, false);
    rig.wait_for_calls(&child, 1);
    let signalled = Instant::now();
    terminate(&child);
    rig.wait_entered();
    let (code, err) = finish(child, Duration::from_secs(15));
    assert_eq!(code, 1, "completion cannot be established: {err}");
    assert!(err.contains("no tap incarnation appeared"), "{err}");
    assert!(
        signalled.elapsed() >= Duration::from_secs(2),
        "the wait ran its budget: {:?}",
        signalled.elapsed()
    );
    let _ = fs::remove_dir_all(&rig.root);

    // The startup race: the marker lands during the wait, then the close.
    let rig = Rig::new("late-incarnation", "500-2");
    fs::remove_file(rig.root.join(INCARNATION_MARKER)).unwrap();
    let child = rig.start(10, false);
    rig.wait_for_calls(&child, 1);
    terminate(&child);
    rig.wait_entered();
    fs::write(rig.root.join(INCARNATION_MARKER), "500-2").unwrap();
    // One more wait pass: the shipper read the late marker and is waiting on its close.
    let passes = rig.wait_passes();
    while rig.wait_passes() == passes {
        std::thread::sleep(Duration::from_millis(50));
    }
    rig.close_tap("500-2");
    let (code, err) = finish(child, Duration::from_secs(15));
    assert_eq!(code, 0, "{err}");
    let _ = fs::remove_dir_all(&rig.root);
}

/// A closed incarnation is not a converted one: a marker reporting segments left
/// unconverted ends the wait, ships what converted, and exits 1 naming the count. An
/// unreadable marker is treated the same, never as complete.
#[test]
fn a_close_that_left_segments_unconverted_is_a_visible_failure() {
    let rig = Rig::new("unconverted", "600-1");
    rig.close_tap_with("600-1", 2);
    let child = rig.start(10, false);
    rig.wait_for_calls(&child, 1);
    terminate(&child);
    let (code, err) = finish(child, Duration::from_secs(10));
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("closed with '2' segment(s) unconverted"),
        "{err}"
    );
    assert!(
        rig.calls().len() >= 2,
        "the final pass still shipped what converted"
    );
    let _ = fs::remove_dir_all(&rig.root);

    let rig = Rig::new("unreadable-marker", "600-2");
    fs::write(rig.root.join(format!("{CLOSED_MARKER_PREFIX}600-2")), b"").unwrap();
    let child = rig.start(10, false);
    rig.wait_for_calls(&child, 1);
    terminate(&child);
    let (code, err) = finish(child, Duration::from_secs(10));
    assert_eq!(code, 1, "an empty marker does not claim completion: {err}");
    let _ = fs::remove_dir_all(&rig.root);
}

/// Every pass during the wait is cut off at what is left of the wait, so a slow upload
/// backlog cannot carry the drain past its budget by more than one pass, and the whole
/// drain fits wait + upload + a beat.
#[test]
fn a_slow_upload_backlog_cannot_outrun_the_wait() {
    let rig = Rig::new("slow-backlog", "700-1");
    // The stub takes two seconds per pass, ignoring the cutoff it is given, as an upload
    // that hangs on a slow destination would until rclone's own cutoff fires.
    fs::write(
        rig.bin.join("rclone"),
        fs::read_to_string(rig.bin.join("rclone"))
            .unwrap()
            .replace("exit 0\n", "sleep 2\nexit 0\n"),
    )
    .unwrap();
    let child = rig.start(3, false);
    rig.wait_for_calls(&child, 1);
    let signalled = Instant::now();
    terminate(&child);
    let (code, err) = finish(child, Duration::from_secs(20));
    assert_eq!(code, 1, "the tap never closed: {err}");
    let calls = rig.calls();
    // Passes after the first (the periodic one) and before the final one are the wait's;
    // each is cut off at what was left of the three-second wait.
    for (args, _) in &calls[1..calls.len() - 1] {
        let cutoff: u64 = args
            .split("--max-duration ")
            .nth(1)
            .unwrap()
            .trim_end_matches('s')
            .split(' ')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(cutoff <= 3, "a wait pass past the remaining wait: {args}");
    }
    assert!(
        signalled.elapsed() < Duration::from_secs(3 + 1 + 3 + 4),
        "the drain outran wait (plus the clock's second) + upload + a pass: {:?}",
        signalled.elapsed()
    );
    let _ = fs::remove_dir_all(&rig.root);
}

/// The script's budgets and the tap's shutdown fit the example's grace period, and the
/// tap's own conversion budget fits the shipper's wait, so the arithmetic in the sidecar's
/// header is the arithmetic in the files.
#[test]
fn the_grace_period_covers_the_handshake() {
    let script = infertap::testing::sidecar_ship_script();
    let default_of = |name: &str| -> u64 {
        let line = script
            .lines()
            .find(|l| l.contains(&format!("${{{name}:=")))
            .unwrap_or_else(|| panic!("{name} has a default"));
        line.split(":=")
            .nth(1)
            .unwrap()
            .trim_end_matches("}\"")
            .parse()
            .unwrap()
    };
    let pass = default_of("INFERTAP_PASS_SECS");
    let wait = default_of("INFERTAP_DRAIN_WAIT_SECS");
    let upload = default_of("INFERTAP_DRAIN_UPLOAD_SECS");
    // The tap's conversion budget runs from the instant its shutdown begins over
    // everything still in front of the worker, queued and swept, checked between
    // conversions; the seal before it and one conversion's overrun are the margin.
    let sweep = infertap::sink::segments::SHUTDOWN_SWEEP_BUDGET.as_secs();
    assert!(
        sweep + 10 <= wait,
        "the tap's shutdown conversion budget ({sweep}s) plus a seal and one conversion \
         must fit the shipper's wait ({wait}s)"
    );
    let example = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("packaging/kubernetes/example-vllm-deployment.yaml"),
    )
    .unwrap();
    let grace: u64 = example
        .lines()
        .find(|l| l.trim().starts_with("terminationGracePeriodSeconds:"))
        .expect("the example states its grace period")
        .split(':')
        .nth(1)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    // The margin absorbs the second the script's whole-second clock adds to the wait.
    assert!(
        grace >= pass + wait + 1 + upload + 10,
        "grace {grace}s is below pass {pass} + wait {wait} + 1 + upload {upload} + margin 10"
    );
    let sidecar = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("packaging/kubernetes/tap-sidecar.yaml"),
    )
    .unwrap();
    assert!(
        !sidecar.contains("terminationGracePeriodSeconds:"),
        "the patch must not override a grace period the engine already needs"
    );
    assert!(
        sidecar.contains(&format!(
            "{pass} + {wait} + 1 + {upload} + 10 = {} s",
            pass + wait + 1 + upload + 10
        )),
        "the sidecar's header states the arithmetic in the files"
    );
}
