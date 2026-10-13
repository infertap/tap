// SPDX-License-Identifier: Apache-2.0
//! Background conversion of sealed JSONL segments to verified Parquet files.

use super::ring::lock_ring;
use super::ring::RingDropped;
use super::ring::SegmentId;
use super::sync_dir;
use super::ACTIVE;
use super::SEG_PREFIX;
use super::SEG_SUFFIX;
use super::TMP_SUFFIX;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Background conversion worker, notified with sealed JSONL paths.
///
/// Enqueueing never blocks. Full-queue notifications are counted and skipped;
/// directory sweeps recover the backlog when the queue empties and on shutdown.
/// Failed conversions retain the source WAL.
///
/// Encoder initialization failure is reported once and stops the worker. The
/// shipper excludes unconverted WALs; retention can remove them with loss records.
#[derive(Debug)]
pub(super) struct ConvertWorker {
    tx: Option<std::sync::mpsc::SyncSender<PathBuf>>,
    join: Option<std::thread::JoinHandle<()>>,
    /// The instant shutdown began, shared with the thread: once set, every conversion
    /// the worker still has in front of it, queued or swept, is under the same budget
    /// from that instant.
    stop: StopDeadline,
    /// The directory, for the count of what shutdown left unconverted.
    dir: PathBuf,
    /// Notifications a full queue refused. Each is a segment the sweep converts later;
    /// the count says how far behind the worker ran.
    stranded: u64,
}

/// The shutdown deadline, `None` until [`ConvertWorker::shutdown`] sets it.
type StopDeadline = std::sync::Arc<std::sync::Mutex<Option<Instant>>>;

/// The queue between the sink and the worker. Small on purpose: what the queue cannot
/// hold is on disk, and disk is what the sweep reads.
const QUEUE_SLOTS: usize = 256;

/// Conversion budget from the start of shutdown, shared by queued work and sweeps.
/// Check between conversions, so one conversion can overrun the deadline. Remaining
/// WALs stay on disk and are counted by [`SegmentedSink::close`](super::SegmentedSink::close).
/// The service manager's stop timeout provides the external deadline.
pub const SHUTDOWN_SWEEP_BUDGET: Duration = Duration::from_secs(20);

impl ConvertWorker {
    pub(super) fn spawn(dir: PathBuf, ring_dropped: RingDropped) -> Self {
        Self::spawn_with(dir, ring_dropped, SHUTDOWN_SWEEP_BUDGET, None)
    }

    /// A worker whose shutdown began before its thread did, with no budget: it converts
    /// nothing, on either path, and reports everything. For the test that pins the
    /// deadline stopping work, with no race against the thread's first sweep.
    #[cfg(test)]
    fn spawn_stopped(dir: PathBuf, ring_dropped: RingDropped) -> Self {
        Self::spawn_with(dir, ring_dropped, Duration::ZERO, Some(Instant::now()))
    }

    /// [`spawn`](Self::spawn) with the budget explicit and the stop instant pre-set.
    fn spawn_with(
        dir: PathBuf,
        ring_dropped: RingDropped,
        budget: Duration,
        stopped_at: Option<Instant>,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<PathBuf>(QUEUE_SLOTS);
        let stop: StopDeadline = std::sync::Arc::new(std::sync::Mutex::new(stopped_at));
        let thread_stop = std::sync::Arc::clone(&stop);
        let thread_dir = dir.clone();
        let join = std::thread::spawn(move || {
            // Catch panics across the worker loop so failures outside conversion are
            // reported to the sink as well.
            let body = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Some(mut worker) =
                    Worker::new(thread_dir, rx, ring_dropped, thread_stop, budget)
                {
                    worker.run();
                }
            }));
            if body.is_err() {
                log::error!(
                    "the conversion worker panicked and stopped; sealed segments stay \
                     unconverted JSONL until the next open's sweep, unshippable until then"
                );
            }
        });
        Self {
            tx: Some(tx),
            join: Some(join),
            stop,
            dir,
            stranded: 0,
        }
    }

    /// Hand a sealed JSONL to the worker. Never blocks: a full queue is a backlog the
    /// worker's own sweep finishes, counted here and said once, at the first.
    pub(super) fn notify(&mut self, sealed: &Path) {
        let Some(tx) = &self.tx else {
            return;
        };
        // Warn once when the queue fills; directory sweeps recover missed notifications.
        // A disconnected worker has already logged its failure. Its WALs remain on disk.
        if let Err(std::sync::mpsc::TrySendError::Full(_)) = tx.try_send(sealed.to_path_buf()) {
            self.stranded += 1;
            if self.stranded == 1 {
                log::warn!(
                    "the conversion queue is full ({QUEUE_SLOTS} segments behind); the \
                     worker's sweep converts the rest from disk"
                );
            }
        }
    }

    /// Notifications the full queue refused since spawn.
    pub(super) const fn stranded(&self) -> u64 {
        self.stranded
    }

    /// A handle with no thread behind it: the test holds the receiver, so the queue fills
    /// and disconnects on the test's say-so.
    #[cfg(test)]
    fn detached(dir: PathBuf) -> (Self, std::sync::mpsc::Receiver<PathBuf>) {
        let (tx, rx) = std::sync::mpsc::sync_channel::<PathBuf>(QUEUE_SLOTS);
        (
            Self {
                tx: Some(tx),
                join: None,
                stop: std::sync::Arc::new(std::sync::Mutex::new(None)),
                dir,
                stranded: 0,
            },
            rx,
        )
    }

    /// Stop the worker within the conversion budget and count remaining WALs.
    /// The budget is checked between conversions; the service manager bounds process exit.
    ///
    /// # Errors
    /// Returns the directory-listing error if the remaining WAL count cannot be determined.
    pub(super) fn shutdown(&mut self) -> io::Result<usize> {
        if let Ok(mut stop) = self.stop.lock() {
            *stop = Some(Instant::now());
        }
        drop(self.tx.take());
        if let Some(j) = self.join.take() {
            if j.join().is_err() {
                // Unreachable while the body catches its own panics; said anyway, so a
                // future body that does not is not silent.
                log::error!("the conversion worker's thread ended in a panic");
            }
        }
        unconverted_write_ahead(&self.dir)
            .map(|left| left.len())
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "listing {} after conversion, so what was left is unknown: {e}",
                        self.dir.display()
                    ),
                )
            })
    }
}

impl Drop for ConvertWorker {
    fn drop(&mut self) {
        if self.join.is_some() {
            let _ = self.shutdown();
        }
    }
}

/// The worker's side: the state the thread owns and the loop it runs.
struct Worker {
    dir: PathBuf,
    rx: std::sync::mpsc::Receiver<PathBuf>,
    conv: crate::sink::convert::Converter,
    ring_dropped: RingDropped,
    /// When shutdown began, once it has; read before every conversion.
    stop: StopDeadline,
    /// How long after that instant conversions may still start.
    budget: Duration,
    /// Paths that failed conversion because of malformed input or a panic.
    /// Skip these files on later sweeps to avoid repeated work and diagnostics.
    /// Retention still counts and can remove them. Remove stale entries each sweep
    /// so the set does not accumulate paths from deleted files.
    poisoned: HashSet<PathBuf>,
}

impl Worker {
    /// A worker with its encoder built, or `None` after saying why the encoder could not
    /// be.
    fn new(
        dir: PathBuf,
        rx: std::sync::mpsc::Receiver<PathBuf>,
        ring_dropped: RingDropped,
        stop: StopDeadline,
        budget: Duration,
    ) -> Option<Self> {
        match crate::sink::convert::Converter::new() {
            Ok(conv) => Some(Self {
                dir,
                rx,
                conv,
                ring_dropped,
                stop,
                budget,
                poisoned: HashSet::new(),
            }),
            Err(e) => {
                log::error!(
                    "shipped-form converter unavailable ({e}); sealed segments stay \
                     unconverted JSONL, which this deployment's shipper does not ship and \
                     which the ring reclaims as declared drops if the disk cap engages"
                );
                None
            }
        }
    }

    /// Convert what the queue delivers; sweep the directory whenever the queue is empty;
    /// on close, one final sweep. Everything after shutdown begins, the queue included,
    /// is under the one budget from that instant.
    fn run(&mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(path) => {
                    if self.out_of_time() {
                        break;
                    }
                    self.convert(&path);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    // The queue is drained. Before blocking, sweep the directory for WALs
                    // the bounded channel stranded: a reconcile backlog past its slots,
                    // or a notify whose try_send found the queue full.
                    self.sweep();
                    match self.rx.recv() {
                        Ok(path) => {
                            if self.out_of_time() {
                                break;
                            }
                            self.convert(&path);
                        }
                        Err(_) => break,
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            }
        }
        // The channel is closed and drained. One final sweep catches anything a
        // non-blocking send dropped, including close()'s own final segment, within
        // what is left of the budget.
        self.sweep();
    }

    /// Whether shutdown has begun and its budget has run out. Said once.
    fn out_of_time(&self) -> bool {
        let Some(began) = self.stop.lock().ok().and_then(|s| *s) else {
            return false;
        };
        if began.elapsed() < self.budget {
            return false;
        }
        log::warn!(
            "shutdown ran out of its {}s conversion budget with segments still \
             unconverted; they stay write-ahead segments, and do not ship from this \
             incarnation",
            self.budget.as_secs()
        );
        true
    }

    fn convert(&mut self, jsonl: &Path) -> Settled {
        let settled = convert_one(&self.conv, jsonl, &self.ring_dropped, &mut self.poisoned);
        if settled == Settled::Poisoned {
            // The set is bounded by the disk at every step it grows, not only at a
            // sweep, which a queue that never empties would never run.
            self.poisoned.retain(|p| p.exists());
        }
        settled
    }

    /// One sweep session: convert every unconverted write-ahead segment in the directory,
    /// rescanning until a scan finds none or the deadline passes. The rescan is what makes
    /// the bounded channel safe: anything the queue could not hold is on disk, and disk is
    /// what gets swept. A segment whose conversion fails retryably is not retried within
    /// the session, since that would spin the worker; it stays for the next one. A poison
    /// segment is never retried.
    fn sweep(&mut self) {
        // A poison mark outlives its file by at most one sweep: the ring reclaims poison
        // segments like any other, and a set that kept every path ever marked would grow
        // with the run instead of with the disk.
        self.poisoned.retain(|p| p.exists());
        let mut skip: HashSet<PathBuf> = HashSet::new();
        loop {
            let Ok(segments) = unconverted_write_ahead(&self.dir) else {
                return; // an unlistable directory: the next session retries
            };
            let pending: Vec<PathBuf> = segments
                .into_iter()
                .filter(|w| !skip.contains(w) && !self.poisoned.contains(w))
                .collect();
            if pending.is_empty() {
                return;
            }
            for segment in pending {
                if self.out_of_time() {
                    return;
                }
                if self.convert(&segment) == Settled::Retry {
                    skip.insert(segment);
                }
            }
        }
    }
}

/// How one conversion attempt ended. Every arm but [`Retry`](Self::Retry) means the
/// write-ahead segment needs no further attempt from this worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Settled {
    /// The twin is committed and the write-ahead segment deleted.
    Converted,
    /// The write-ahead segment was gone before or during the attempt: the ring or the
    /// shipper won the race, and nothing remains to convert.
    Gone,
    /// The ring declared this segment dropped while the twin was being written; the
    /// temporary is unlinked and nothing with the shipped name appeared.
    Refused,
    /// Poison: a malformed line or a panic. Marked, and never tried again by this worker.
    Poisoned,
    /// A retryable failure, such as I/O, the Parquet library, or a footer mismatch: the
    /// write-ahead segment stays for the next session.
    Retry,
}

/// Convert a sealed WAL and delete it after committing the verified Parquet file.
///
/// Commit holds the ring lock while checking the drop set and WAL existence and
/// renaming the temporary file. Retention records drops and unlinks files under
/// the same lock, preventing conversion from publishing an already-dropped segment.
pub(super) fn convert_one(
    conv: &crate::sink::convert::Converter,
    jsonl: &Path,
    ring_dropped: &RingDropped,
    poisoned: &mut HashSet<PathBuf>,
) -> Settled {
    if !jsonl.exists() {
        return Settled::Gone;
    }
    if poisoned.contains(jsonl) {
        return Settled::Poisoned;
    }
    let identity = jsonl
        .file_name()
        .and_then(|n| n.to_str())
        .map(SegmentId::parse);
    let commit = |tmp: &Path, dest: &Path| -> io::Result<bool> {
        let ring = lock_ring(ring_dropped);
        let declared_dropped = identity.as_ref().is_some_and(|id| ring.contains(id));
        if declared_dropped || !jsonl.exists() {
            return Ok(false);
        }
        fs::rename(tmp, dest)?;
        Ok(true)
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conv.convert_guarded(jsonl, &commit)
    }));
    match outcome {
        Ok(Ok(Some(_))) => {
            if fs::remove_file(jsonl).is_ok() {
                if let Some(dir) = jsonl.parent() {
                    let _ = sync_dir(dir);
                }
            }
            Settled::Converted
        }
        Ok(Ok(None)) => Settled::Refused,
        Ok(Err(e)) if e.is_retryable() => {
            log::warn!(
                "conversion failed for {} ({e}); the JSONL stays for retry, unshippable \
                 until converted, and is reclaimed as a declared drop if the disk cap \
                 engages",
                jsonl.display()
            );
            Settled::Retry
        }
        Ok(Err(e)) => {
            log::error!(
                "{} cannot be converted ({e}) and will not be retried; it stays a sealed \
                 segment, unshippable, counted by the disk cap, and reclaimed as a declared \
                 drop when the ring reaches it",
                jsonl.display()
            );
            poisoned.insert(jsonl.to_path_buf());
            Settled::Poisoned
        }
        Err(_) => {
            log::error!(
                "conversion panicked for {} and will not be retried; it stays a sealed \
                 segment, unshippable, counted by the disk cap, and reclaimed as a declared \
                 drop when the ring reaches it",
                jsonl.display()
            );
            poisoned.insert(jsonl.to_path_buf());
            Settled::Poisoned
        }
    }
}

/// Whether a directory entry is a terminal write-ahead segment: sealed, `.jsonl`, and never
/// the active file.
pub(super) fn is_write_ahead_name(name: &str) -> bool {
    name != ACTIVE && name.ends_with(SEG_SUFFIX) && name.starts_with(SEG_PREFIX)
}

/// Terminal write-ahead segments with no committed twin beside them, sorted so sweeps run
/// in a deterministic order.
pub(super) fn unconverted_write_ahead(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let is_write_ahead = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(is_write_ahead_name);
        if is_write_ahead && !path.with_extension("parquet").exists() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

/// Reconcile the shipped form at open, idempotently: a stray conversion temporary is unlinked;
/// a sealed JSONL whose twin exists is deleted, because the twin was verified before its
/// rename; a sealed JSONL with no twin is queued for conversion.
pub(super) fn reconcile_shipped_form(dir: &Path, worker: &mut ConvertWorker) -> io::Result<()> {
    let mut pending: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.ends_with(TMP_SUFFIX) {
            let _ = fs::remove_file(&path);
            continue;
        }
        if !is_write_ahead_name(name) {
            continue;
        }
        let twin = path.with_extension("parquet");
        if twin.exists() {
            let _ = fs::remove_file(&path);
        } else {
            pending.push(path);
        }
    }
    sync_dir(dir)?;
    for p in pending {
        worker.notify(&p);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{ConvertWorker, QUEUE_SLOTS};
    use std::path::{Path, PathBuf};

    /// Fill the notification channel before running the worker. The disk sweep must
    /// convert every file whose notification was refused, without a shutdown deadline.
    #[test]
    fn a_backlog_past_the_channel_capacity_is_swept_not_stranded() {
        use super::{RingDropped, Worker};
        use std::sync::{Arc, Mutex};
        let dir = crate::testing::scratch("convert-sweep");
        let (mut handle, rx) = ConvertWorker::detached(dir.clone());
        let count = QUEUE_SLOTS + 2;
        for i in 0..count {
            let path = dir.join(format!("seg-100-{i}.jsonl"));
            std::fs::write(&path, "{\"kind\":\"segment_open\"}\n").unwrap();
            handle.notify(&path);
        }
        assert_eq!(handle.stranded(), 2);
        let ring: RingDropped = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let mut worker = Worker::new(
            dir.clone(),
            rx,
            ring,
            Arc::clone(&handle.stop),
            super::SHUTDOWN_SWEEP_BUDGET,
        )
        .unwrap();
        // Close the channel without starting shutdown: this test exercises draining,
        // while the separate zero-budget test exercises deadline expiry.
        drop(handle.tx.take());
        worker.run();
        assert!(super::unconverted_write_ahead(&dir).unwrap().is_empty());
        let twins = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .count();
        assert_eq!(twins, count);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A poison mark is bounded by the disk: once the ring (or anyone) unlinks the file,
    /// the next sweep forgets it, so repeated conversion failures over the run's life
    /// cannot grow the set past what is on disk.
    #[test]
    fn a_poison_mark_goes_with_its_file() {
        use super::{RingDropped, Worker};
        use std::sync::{Arc, Mutex};
        let dir = crate::testing::scratch("poison-prune");
        let (_tx, rx) = std::sync::mpsc::sync_channel::<PathBuf>(1);
        let ring: RingDropped = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let stop = std::sync::Arc::new(std::sync::Mutex::new(None));
        let mut w = Worker::new(dir.clone(), rx, ring, stop, super::SHUTDOWN_SWEEP_BUDGET)
            .expect("the vendored schema parses");
        let poison = dir.join("seg-100-0.jsonl");
        std::fs::write(
            &poison,
            "{\"kind\":\"segment_open\"}\n{\"kind\":\"store\",\"n_tokens\":\"x\"}\n",
        )
        .expect("plant");
        w.sweep();
        assert!(w.poisoned.contains(&poison), "marked once");
        w.sweep();
        assert_eq!(
            w.poisoned.len(),
            1,
            "a second sweep neither retries nor re-marks"
        );
        std::fs::remove_file(&poison).expect("the ring reclaims it");
        w.sweep();
        assert!(w.poisoned.is_empty(), "the mark went with the file");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The shutdown deadline stops work on both paths, the queue and the sweep, and what
    /// it stops is reported. A worker whose deadline passed before its thread ran is
    /// handed a backlog on disk and the same backlog through the queue; it converts none
    /// of it and reports all of it. Were the deadline not enforced, the backlog would
    /// convert and the count would read zero. With the real budget the same backlog
    /// converts and nothing is reported.
    #[test]
    fn the_shutdown_deadline_stops_the_queue_and_the_sweep_and_reports_what_it_left() {
        use super::RingDropped;
        use std::sync::{Arc, Mutex};
        let line = "{\"kind\":\"segment_open\"}\n";
        let backlog = |tag: &str| -> PathBuf {
            let dir = crate::testing::scratch(tag);
            for i in 0..6 {
                std::fs::write(dir.join(format!("seg-100-{i}.jsonl")), line).unwrap();
            }
            dir
        };

        let dir = backlog("deadline-passed");
        let ring: RingDropped = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let mut w = ConvertWorker::spawn_stopped(dir.clone(), Arc::clone(&ring));
        for i in 0..6 {
            w.notify(&dir.join(format!("seg-100-{i}.jsonl")));
        }
        assert_eq!(
            w.shutdown().expect("the directory lists"),
            6,
            "past the deadline nothing converts, and everything is reported"
        );
        assert_eq!(super::unconverted_write_ahead(&dir).unwrap().len(), 6);
        assert!(
            !dir.join("seg-100-0.parquet").exists(),
            "no twin appeared past the deadline"
        );
        let _ = std::fs::remove_dir_all(&dir);

        let dir = backlog("budget-real");
        let mut w = ConvertWorker::spawn(dir.clone(), ring);
        assert_eq!(
            w.shutdown().expect("the directory lists"),
            0,
            "within the budget everything converts"
        );
        assert_eq!(super::unconverted_write_ahead(&dir).unwrap().len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A count the worker cannot establish is an error, never zero: a directory that
    /// cannot be listed after conversion leaves completion unknown, and shutdown says so.
    #[test]
    #[cfg(unix)]
    fn an_unlistable_directory_is_an_error_not_a_complete_close() {
        use super::RingDropped;
        use std::os::unix::fs::PermissionsExt as _;
        use std::sync::{Arc, Mutex};
        let dir = crate::testing::scratch("unlistable");
        let ring: RingDropped = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let mut w = ConvertWorker::spawn(dir.clone(), ring);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
        let result = w.shutdown();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = result.expect_err("an unlistable directory is not a count");
        assert!(
            err.to_string().contains("what was left is unknown"),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The handle never blocks and never fails the sink: a full queue counts the
    /// notification it could not hold, a disconnected one (the worker gone) counts
    /// nothing and says nothing more, and a handle that was shut down accepts a notify as
    /// a no-op.
    #[test]
    fn notify_counts_a_full_queue_and_survives_a_gone_worker() {
        let (mut w, rx) = ConvertWorker::detached(PathBuf::from("/nonexistent"));
        let sealed = Path::new("seg-1-0.jsonl");
        for _ in 0..QUEUE_SLOTS {
            w.notify(sealed);
        }
        assert_eq!(w.stranded(), 0, "the queue held every slot's worth");
        w.notify(sealed);
        w.notify(sealed);
        assert_eq!(w.stranded(), 2, "past the slots, each refusal is counted");
        drop(rx);
        w.notify(sealed);
        assert_eq!(w.stranded(), 2, "a gone worker is not a full queue");
        // A detached handle has no directory to count, and the count is not the point.
        let _ = w.shutdown();
        w.notify(sealed);
        assert_eq!(w.stranded(), 2, "after shutdown a notify is a no-op");
    }
}
