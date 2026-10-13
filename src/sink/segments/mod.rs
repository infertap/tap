// SPDX-License-Identifier: Apache-2.0
//! Segmented record storage, recovery, and retention.
//!
//! Records are appended to `active.jsonl`. Sealing creates an immutable WAL that a
//! background worker converts to Parquet. Only completed Parquet files are shipped.
//! An exclusive directory lock prevents concurrent writers.
//!
//! Each segment header identifies its incarnation and sequence. Recovery uses the
//! original header, preserves complete records, and discards a truncated tail.
//! Records are not individually synced; recovery after power loss is best effort.
//!
//! Retention removes the oldest sealed segments and records successful deletions in
//! `segments_dropped`. Sequence gaps also expose missing segments if a loss declaration
//! is itself removed. Missing files reclaim tracked space without declaring a new loss;
//! failed deletions do not reclaim space and are reported as retention failures.

use crate::record::RecordData;

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::clock::ProducerMs;
use crate::record::{Provenance, Record, SegmentOpen, SegmentRecovered};
use crate::sink::{Latch, Sink};

mod recovery;
mod ring;
mod worker;

use recovery::recover_orphan;
use ring::RingDropped;
pub use worker::SHUTDOWN_SWEEP_BUDGET;
use worker::{reconcile_shipped_form, ConvertWorker};

/// Name of the segment currently being written.
pub const ACTIVE: &str = "active.jsonl";

/// Prefix for a sealed, attributed segment. Contract: `conformance/delivery/layout.json`.
pub const SEG_PREFIX: &str = "seg-";

/// Record contract version written into each segment header.
/// See `spec/kv-cache-v2.md` in the pinned inference-events revision.
pub const CONTRACT_VERSION: &str = inference_events::CONTRACT_VERSION;

/// Suffix for every segment the tap writes.
pub const SEG_SUFFIX: &str = ".jsonl";

/// Suffix of the shipped-form twin a sealed segment converts to (contract section 2.1).
pub const PARQUET_SUFFIX: &str = ".parquet";

/// Suffix of the conversion temporary. Never terminal, never shipped; a stray one is
/// unlinked by the open-time reconcile.
pub const TMP_SUFFIX: &str = ".parquet.tmp";

/// The shutdown handshake's first file: the live incarnation's name, written at open. It
/// sits at the output root, above the stream directory, so the shipper never moves it. A
/// shipper draining at termination reads it to learn which [`CLOSED_MARKER_PREFIX`] file
/// to wait for, so a marker an earlier incarnation left cannot satisfy the wait.
pub const INCARNATION_MARKER: &str = "incarnation";

/// The handshake's second file, at the output root: `closed-<incarnation>`, written after
/// the incarnation's last segment is sealed and its conversion has settled, holding the
/// count of write-ahead segments left unconverted, `0` when everything converted. A
/// closed incarnation is not a converted one; the count says which. The tap removes any
/// earlier incarnation's marker at open.
pub const CLOSED_MARKER_PREFIX: &str = "closed-";

/// Default size bound on the active segment. Caps what a process crash can cost.
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Default age bound on the active segment. Also the cadence a consumer can hold the tap to:
/// no new sealed segment past this interval is staleness, not silence.
pub const DEFAULT_MAX_SECS: u64 = 300;

/// Default sealed-segment retention: 16 GiB.
/// Size retention from measured output rate and the required shipping-outage interval.
/// The limit excludes active output and temporary conversion files; allow additional
/// filesystem capacity for both. Oldest sealed segments are reclaimed with loss records.
pub const DEFAULT_MAX_DISK_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Sealing and retention policy.
#[derive(Debug)]
pub struct SegmentConfig {
    /// The stream directory: `active.jsonl` and the sealed segments live here.
    pub dir: PathBuf,
    /// Seal once the active segment reaches this size. Bounds what a crash can cost.
    pub max_bytes: u64,
    /// Seal once the active segment reaches this age, even if empty of cache records. This is
    /// the bound that matters for a *quiet* instance: without it a silent engine never seals,
    /// its heartbeats never ship, and it is indistinguishable from a dead one. A known sealing
    /// cadence is what turns arrival timing into a liveness signal.
    pub max_secs: u64,
    /// Total bytes of sealed segments to retain before dropping the oldest. `None` is
    /// unbounded, for one-shot runs.
    pub max_disk_bytes: Option<u64>,
    /// What this producer's traffic is for, in the operator's own vocabulary, declared on every
    /// segment header (contract section 2.7). A reader applies it to every record in the segment.
    ///
    /// `None` is a producer that says nothing, which a reader keeps distinct from a producer
    /// that serves no workload.
    pub workload_class: Option<String>,
    /// The declared heartbeat interval and maximum segment age (contract section 5.8), stated on every
    /// header. They are the entire basis on which a reader may call this producer stale, and an
    /// analysis window need not contain the run's start record.
    pub heartbeat_secs: u64,
    /// The epoch scoping this producer's identity space (contract section 3.3), declared on every header.
    /// `None` when running unkeyed, where there is no identity space to scope.
    pub key_epoch: Option<u32>,
}

impl SegmentConfig {
    /// Validate retention bounds and header size without touching the filesystem.
    ///
    /// # Errors
    /// Returns `InvalidInput` when retention cannot hold four segments or the header
    /// exceeds the record limit.
    pub fn validate(&self, provenance: &Provenance) -> io::Result<()> {
        // A cap that cannot hold several segments makes the ring thrash: seal, immediately
        // drop, repeat. Refuse it at the door rather than misbehave quietly.
        if let Some(cap) = self.max_disk_bytes {
            if cap < self.max_bytes.saturating_mul(4) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "disk cap {cap} is below 4x the segment size {}; the ring would drop \
                         segments as fast as it sealed them",
                        self.max_bytes
                    ),
                ));
            }
        }
        self.validate_header(provenance)
    }

    /// Validate the header size before creating or recovering any files.
    /// Reserve space for the largest sequence and generated incarnation identifier.
    ///
    /// # Errors
    /// Returns `InvalidInput` when the complete header could exceed the record limit.
    pub fn validate_header(&self, provenance: &Provenance) -> io::Result<()> {
        let header = Record::SegmentOpen(self.header(
            provenance.clone(),
            format!("{}-{}", u128::MAX, u32::MAX),
            u64::MAX,
        ));
        crate::sink::guard::check_size(&header)
    }

    fn header(&self, provenance: Provenance, incarnation: String, segment_seq: u64) -> SegmentOpen {
        SegmentOpen {
            provenance,
            at_ms: ProducerMs::now(),
            contract_version: CONTRACT_VERSION,
            incarnation,
            segment_seq,
            workload_class: self.workload_class.clone(),
            heartbeat_secs: self.heartbeat_secs,
            max_segment_secs: self.max_secs,
            key_epoch: self.key_epoch,
        }
    }

    /// The shipped defaults for `dir`: the default seal size and age, the default disk cap,
    /// no workload class, a 60-second heartbeat declaration, unkeyed. A caller
    /// sets the fields it means to change; a field added later takes its default here
    /// rather than breaking every constructor.
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            max_bytes: DEFAULT_MAX_BYTES,
            max_secs: DEFAULT_MAX_SECS,
            max_disk_bytes: Some(DEFAULT_MAX_DISK_BYTES),
            workload_class: None,
            heartbeat_secs: crate::lifecycle::DEFAULT_HEARTBEAT_SECS,
            key_epoch: None,
        }
    }
}

/// Writes the record stream as sealed segments.
#[derive(Debug)]
pub struct SegmentedSink {
    cfg: SegmentConfig,
    provenance: Provenance,
    incarnation: String,
    seq: u64,
    active: Option<BufWriter<File>>,
    active_bytes: u64,
    opened: Instant,
    written: u64,
    sealed: u64,
    dropped_segments: u64,
    error: Latch,
    /// The line being written, reused across records so a record costs no allocation
    /// beyond what its own strings need.
    line: Vec<u8>,
    converter: Option<ConvertWorker>,
    /// Written by [`Self::enforce_disk_cap`], read by the worker at its commit point.
    ring_dropped: RingDropped,
    // Dropped after the converter has stopped and all output handles have closed.
    _directory_lock: DirectoryLock,
}

/// Owns the lock until all segment output and conversion state has been dropped.
#[derive(Debug)]
struct DirectoryLock(File);

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        // Closing alone retains the lock while a forked child holds a duplicate.
        // Release it explicitly before the file closes. Keep the lock inode in place.
        if let Err(error) = fs2::FileExt::unlock(&self.0) {
            log::error!("cannot release segment directory lock: {error}");
        }
    }
}

/// Integer wall-clock milliseconds, for the incarnation identifier. Distinct from
/// [`ProducerMs::now`] because an identifier that round-trips through a float is an
/// identifier that can collide after enough uptime.
pub(super) fn now_ms_int() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

impl SegmentedSink {
    /// Open a segmented sink in `cfg.dir`, recovering any orphan left by a previous
    /// incarnation first. Every sealed segment converts to its Parquet twin, the only
    /// shipped form (contract section 2.1).
    ///
    /// # Errors
    /// Directory creation, orphan recovery, or opening the first active segment failing.
    pub fn open(cfg: SegmentConfig, provenance: Provenance) -> io::Result<Self> {
        Self::open_with(cfg, provenance, true)
    }

    /// [`open`](Self::open) with no conversion worker: sealed segments stay write-ahead
    /// JSONL. The test seam for the write-ahead invariants (byte identity, completeness,
    /// orphan attribution) that conversion's deletion of the JSONL would hide. Not a
    /// deployment shape: the write-ahead form never ships.
    ///
    /// # Errors
    /// As [`open`](Self::open).
    #[cfg(any(test, feature = "testing"))]
    pub fn open_write_ahead_only(cfg: SegmentConfig, provenance: Provenance) -> io::Result<Self> {
        Self::open_with(cfg, provenance, false)
    }

    fn open_with(cfg: SegmentConfig, provenance: Provenance, convert: bool) -> io::Result<Self> {
        cfg.validate(&provenance)?;
        fs::create_dir_all(&cfg.dir)?;
        // The WAL suffix keeps the lock out of the shipper's selection.
        // Keep this inode in place: unlinking it would allow two independent locks.
        let directory_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(cfg.dir.join(".writer.lock.jsonl"))?;
        fs2::FileExt::try_lock_exclusive(&directory_lock).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("cannot acquire segment directory lock: {e}"),
            )
        })?;
        let directory_lock = DirectoryLock(directory_lock);
        // Recover before opening: the new active file would otherwise collide with the orphan.
        // A headerless file is unlinked here and declared below, once there is a segment
        // for the declaration to land in.
        let discarded = recover_orphan(&cfg.dir)?;
        // Reconcile the shipped form beside it, idempotently, and only in a converting
        // deployment: with conversion off, a JSONL beside a twin is an operator transition
        // to untangle, not a redundant copy to delete.
        let ring_dropped: RingDropped = Arc::new(Mutex::new(HashSet::new()));
        let converter = if convert {
            let mut w = ConvertWorker::spawn(cfg.dir.clone(), Arc::clone(&ring_dropped));
            reconcile_shipped_form(&cfg.dir, &mut w)?;
            Some(w)
        } else {
            None
        };

        let incarnation = format!("{}-{}", now_ms_int(), std::process::id());
        let mut s = Self {
            cfg,
            provenance,
            incarnation,
            seq: 0,
            active: None,
            active_bytes: 0,
            opened: Instant::now(),
            written: 0,
            sealed: 0,
            dropped_segments: 0,
            error: Latch::default(),
            line: Vec::with_capacity(1024),
            converter,
            ring_dropped,
            _directory_lock: directory_lock,
        };
        s.open_active()?;
        // The discard of a headerless file is declared second in this incarnation's first
        // segment (contract section 4.3): the file was never durably written, its records belong
        // to no sequence, and no record could declare them dropped later.
        if let Some(dropped_bytes) = discarded {
            let marker = Record::SegmentRecovered(SegmentRecovered {
                at_ms: ProducerMs::now(),
                attributed: false,
                dropped_bytes,
            });
            s.write_line(&marker);
        }
        // Enforce retention after opening an active segment so losses can be recorded.
        // Startup enforcement also covers WALs left by repeated crashes. Active and
        // in-progress conversion files require space beyond the sealed-segment budget.
        s.enforce_disk_cap()?;
        Ok(s)
    }

    fn active_path(&self) -> PathBuf {
        self.cfg.dir.join(ACTIVE)
    }

    /// Open a fresh active segment and write its `segment_open` header, fsync'd immediately so
    /// that an orphan is always either self-describing or empty.
    fn open_active(&mut self) -> io::Result<()> {
        let header = Record::SegmentOpen(self.cfg.header(
            self.provenance.clone(),
            self.incarnation.clone(),
            self.seq,
        ));
        crate::sink::guard::check_size(&header)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.active_path())?;
        let mut w = BufWriter::new(file);

        Self::render(&mut self.line, &header);
        w.write_all(&self.line)?;
        w.flush()?;
        w.get_ref().sync_all()?;

        self.active_bytes = self.line.len() as u64;
        self.opened = Instant::now();
        self.written += 1;
        self.active = Some(w);
        Ok(())
    }

    fn should_seal(&self) -> bool {
        self.active_bytes >= self.cfg.max_bytes
            || self.opened.elapsed() >= Duration::from_secs(self.cfg.max_secs)
    }

    /// Flush and sync the active segment, then publish its sealed name without replacement.
    /// Sync the directory before removing the active name. Returns the sealed path,
    /// or `None` when no segment was active.
    fn seal_active(&mut self) -> io::Result<Option<PathBuf>> {
        let Some(mut w) = self.active.take() else {
            return Ok(None);
        };
        w.flush()?;
        w.get_ref().sync_all()?;
        drop(w);
        let dest = self.cfg.dir.join(format!(
            "{SEG_PREFIX}{}-{}{SEG_SUFFIX}",
            self.incarnation, self.seq
        ));
        seal_without_replace(&self.active_path(), &dest)?;
        self.sealed += 1;
        self.seq += 1;
        Ok(Some(dest))
    }

    /// Seal the active segment and open its successor.
    fn seal(&mut self) -> io::Result<()> {
        let Some(dest) = self.seal_active()? else {
            return Ok(());
        };
        // Open the next active segment before enforcing the cap: the drop record has to land
        // somewhere, and with no active segment `write_line` has nowhere to put it. The
        // reverse order turns a bounded, declared loss into a silent one.
        self.open_active()?;
        self.enforce_disk_cap()?;
        // Hand the sealed file to the converter last: the cap may have already reclaimed it,
        // and the worker treats a vanished file as a race it lost, not an error.
        if let Some(w) = &mut self.converter {
            w.notify(&dest);
        }
        Ok(())
    }

    /// Drop the oldest sealed segments past the cap, recording the loss in-band. The gap
    /// the drop leaves in `segment_seq` is what survives if the in-band record is itself
    /// dropped later.
    ///
    /// **Ordering is numeric, per (incarnation, seq).** A lexicographic sort over the path
    /// orders `seg-…-10` before `seg-…-9` and inverts retention exactly when the data
    /// matters (`docs/design/design-log.md`, "The retention order").
    fn enforce_disk_cap(&mut self) -> io::Result<()> {
        let Some(cap) = self.cfg.max_disk_bytes else {
            return Ok(());
        };
        let reclaim = ring::reclaim_over_cap(&self.cfg.dir, cap, &self.ring_dropped)?;
        if let Some((n, first)) = reclaim.undeletable {
            log::error!(
                "{n} sealed segment(s) could not be deleted (first: {first}); the \
                 disk cap is NOT fully enforceable and the segment directory may exceed \
                 --max-disk-bytes. Fix the directory's ownership or permissions."
            );
        }
        if reclaim.over_cap > 0 {
            log::error!(
                "{} byte(s) over --max-disk-bytes with no sealed segment left to reclaim: \
                 undeletable files alone exceed the cap. Fix the directory's ownership or \
                 permissions; the stream directory is over its bound until then.",
                reclaim.over_cap
            );
        }
        if reclaim.dropped.is_empty() {
            return Ok(());
        }
        if self.dropped_segments == 0 {
            // The ring engaging means shipping has been failing long enough to start losing
            // data. That belongs on stderr, not only in a record nobody is reading yet.
            log::warn!(
                "disk cap reached; dropping oldest sealed segments. The shipped \
                 stream is now INCOMPLETE by whole segments. Fix the shipper or raise \
                 --max-disk-bytes."
            );
        }
        self.dropped_segments += reclaim.dropped.len() as u64;
        for record in ring::dropped_declarations(&reclaim.dropped, ProducerMs::now()) {
            self.write_line(&record);
        }
        Ok(())
    }

    /// One record as one line, serialized straight into the reusable buffer.
    fn render<P: crate::record::Phase>(line: &mut Vec<u8>, record: &RecordData<P>) {
        line.clear();
        #[allow(clippy::expect_used)] // a derived Serialize over plain data cannot fail
        serde_json::to_writer(&mut *line, record).expect("a record serializes");
        line.push(b'\n');
    }

    fn write_line<P: crate::record::Phase>(&mut self, record: &RecordData<P>) {
        if self.error.is_set() {
            return;
        }
        Self::render(&mut self.line, record);
        if self.line.len() > crate::sink::guard::MAX_RECORD_BYTES {
            self.error.set(io::Error::new(
                io::ErrorKind::InvalidInput,
                "record exceeds the 1 MiB serialized size limit",
            ));
            return;
        }
        let Some(w) = self.active.as_mut() else {
            // Reject writes without an active segment so the caller can report output failure.
            self.error.set(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "record emitted with no active segment open",
            ));
            return;
        };
        if let Err(e) = w.write_all(&self.line) {
            self.error.set(e);
            return;
        }
        self.active_bytes += self.line.len() as u64;
        self.written += 1;
    }

    /// Records written across all segments of this incarnation, headers included.
    #[must_use]
    pub const fn written(&self) -> u64 {
        self.written
    }

    /// Segments sealed by this incarnation.
    #[must_use]
    pub const fn sealed(&self) -> u64 {
        self.sealed
    }

    /// Sealed segments dropped to stay under the disk cap. Nonzero means the shipped stream is
    /// incomplete by whole segments.
    #[must_use]
    pub const fn dropped_segments(&self) -> u64 {
        self.dropped_segments
    }

    /// This incarnation's identifier, `<run_start_ms>-<pid>`.
    #[must_use]
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    /// Seal the active segment and stop. Called on a clean exit so the final records ship.
    /// Returns what the incarnation left behind: the write-ahead segments the converter
    /// did not turn into their shipped form, which is the count of segments that will not
    /// ship from this incarnation. A closed sink is not a converted one; the caller reads
    /// the count.
    ///
    /// # Errors
    /// Flush, sync, or sealing failing; or the directory could not be listed after
    /// conversion, so what was left is unknown.
    pub fn close(mut self) -> io::Result<Closed> {
        // A write error makes the active segment terminal: the process has given up on it.
        // Seal what landed before reporting, or those records wait for a restart to be
        // recovered as an orphan, and terminal data that does not ship is the same mistake
        // as deleting it. No successor is opened: this is the end of the incarnation.
        let sealed = match self.seal_active() {
            Ok(sealed) => sealed,
            Err(e) => {
                if let Some(mut cw) = self.converter.take() {
                    // The seal failed first; that is the error, whatever conversion left.
                    let _ = cw.shutdown();
                }
                return Err(e);
            }
        };
        // Queue the final segment, then drain under the shutdown budget. The sweep
        // also considers files whose notifications were refused by a full queue.
        // Any unconverted files remain on disk and are reported by close().
        let mut unconverted = None;
        if let Some(mut cw) = self.converter.take() {
            if let Some(dest) = &sealed {
                cw.notify(dest);
            }
            let left = cw.shutdown();
            if cw.stranded() > 0 {
                log::info!(
                    "the conversion queue refused {} notification(s) during the run; the \
                     worker's sweeps converted those segments from disk",
                    cw.stranded()
                );
            }
            // Propagate directory-read errors; an unknown backlog must not report zero.
            unconverted = Some(left?);
        }
        self.error.check()?;
        Ok(Closed { unconverted })
    }
}

/// What a closed incarnation left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    /// Write-ahead segments still unconverted after the converter's shutdown: cut off by
    /// its budget, or failed. They do not ship from this incarnation; the next open's
    /// sweep converts them where there is a next open. `None` without a converter, where
    /// nothing was asked to convert.
    pub unconverted: Option<usize>,
}

impl<P: crate::record::Phase> Sink<RecordData<P>> for SegmentedSink {
    fn emit(&mut self, record: RecordData<P>) {
        if self.should_seal() {
            if let Err(e) = self.seal() {
                self.error.set(e);
                return;
            }
        }
        self.write_line(&record);
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Self::flush(self)
    }
    fn write_error(&self) -> Option<std::io::Error> {
        Self::write_error(self)
    }
}

impl SegmentedSink {
    /// Flush output and report any latched error.
    ///
    /// # Errors
    /// Returns a previous write error or the flush error.
    pub fn flush(&mut self) -> io::Result<()> {
        // The latch is permanent: a report is returned, the original stays, and every
        // later emit keeps dropping rather than writing after a torn line. Taking the error
        // here would un-latch the sink on the first flush that reported it.
        self.error.check()?;
        if let Some(w) = self.active.as_mut() {
            if let Err(e) = w.flush() {
                self.error.set(e);
                return self.error.check();
            }
        }
        Ok(())
    }

    /// The first output error, if any.
    #[must_use]
    pub fn write_error(&self) -> Option<io::Error> {
        self.error.report()
    }
}

pub(super) fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

// Hard-link creation publishes a name atomically and refuses an existing destination.
// A crash between linking and unlinking leaves both names; recovery preserves that
// ambiguity for the operator instead of modifying an already sealed file.
fn check_seal_destination(dest: &Path) -> io::Result<()> {
    if dest.try_exists()? || dest.with_extension("parquet").try_exists()? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "segment destination already exists; active file preserved",
        ));
    }
    Ok(())
}

fn seal_without_replace(active: &Path, dest: &Path) -> io::Result<()> {
    check_seal_destination(dest)?;
    fs::hard_link(active, dest)?;
    let dir = active
        .parent()
        .ok_or_else(|| io::Error::other("segment has no directory"))?;
    sync_dir(dir)?;
    fs::remove_file(active)?;
    sync_dir(dir)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod ring_seam_tests {
    use super::ring::{lock_ring, RingDropped, SegmentId};
    use super::worker::{convert_one, Settled};
    use super::{Provenance, SegmentConfig, SegmentedSink};
    use std::collections::HashSet;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    fn scratch(tag: &str) -> PathBuf {
        crate::testing::scratch(&format!("seam-{tag}"))
    }

    /// The worker half of the converter and ring seam: the commit guard consults the shared
    /// set immediately before the rename that commits a twin. A segment the ring declared
    /// dropped converts to nothing, neither twin nor temporary, even while its write-ahead
    /// segment is still readable, which is the state the unlink race leaves the worker in.
    #[test]
    fn a_declared_dropped_segment_never_commits_a_twin() {
        let dir = scratch("no-resurrection");
        let wal = dir.join("seg-100-0.jsonl");
        fs::write(&wal, "{\"kind\":\"segment_open\"}\n").expect("plant the write-ahead segment");
        let ring: RingDropped = Arc::new(Mutex::new(HashSet::new()));
        lock_ring(&ring).insert(SegmentId::parse("seg-100-0.jsonl"));
        let conv = crate::sink::convert::Converter::new().expect("the vendored schema parses");

        let settled = convert_one(&conv, &wal, &ring, &mut HashSet::new());

        assert_eq!(
            settled,
            Settled::Refused,
            "a dropped segment needs no retry"
        );
        assert!(
            !dir.join("seg-100-0.parquet").exists(),
            "no twin may appear for a segment the ring declared dropped"
        );
        assert!(
            !dir.join("seg-100-0.parquet.tmp").exists(),
            "the abort unlinks its temporary"
        );
        assert!(wal.exists(), "the abort deletes nothing but the temporary");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A malformed segment is marked once and retained for disk-cap reclamation.
    /// Later sweeps must not retry it or create another temporary.
    #[test]
    fn a_malformed_segment_is_poisoned_once_and_never_retried() {
        let dir = scratch("poison");
        let wal = dir.join("seg-100-0.jsonl");
        fs::write(
            &wal,
            "{\"kind\":\"segment_open\"}\n{\"kind\":\"store\",\"n_tokens\":\"not a number\"}\n",
        )
        .expect("plant the write-ahead segment");
        let ring: RingDropped = Arc::new(Mutex::new(HashSet::new()));
        let conv = crate::sink::convert::Converter::new().expect("the vendored schema parses");
        let mut poisoned = HashSet::new();

        assert_eq!(
            convert_one(&conv, &wal, &ring, &mut poisoned),
            Settled::Poisoned,
            "poison needs no retry"
        );
        assert!(poisoned.contains(&wal), "the segment is marked");
        assert!(
            wal.exists(),
            "the write-ahead segment stays for the ring to reclaim and declare"
        );
        assert!(!dir.join("seg-100-0.parquet").exists(), "no twin appears");
        assert!(
            !dir.join("seg-100-0.parquet.tmp").exists(),
            "no temporary is left"
        );
        // A retryable failure would leave the mark absent; a second call on a poison segment is
        // a lookup, not a conversion.
        assert_eq!(
            convert_one(&conv, &wal, &ring, &mut poisoned),
            Settled::Poisoned
        );
        assert_eq!(poisoned.len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The ring half of the seam: enforcement records each identity it reclaims into the
    /// shared set before the unlink, and removes an identity whose files all survived the
    /// attempt.
    #[test]
    fn enforcement_shares_the_identities_it_reclaims() {
        let dir = scratch("shared-ids");
        fs::write(dir.join("seg-100-0.jsonl"), vec![b'x'; 800]).expect("plant");
        fs::write(dir.join("seg-100-1.jsonl"), vec![b'x'; 800]).expect("plant");
        let s = SegmentedSink::open_write_ahead_only(
            SegmentConfig {
                dir: dir.clone(),
                max_bytes: 300,
                max_secs: 3600,
                max_disk_bytes: Some(1200),
                workload_class: None,
                heartbeat_secs: 60,
                key_epoch: None,
            },
            Provenance::default(),
        )
        .expect("open");

        let set = lock_ring(&s.ring_dropped).clone();
        assert!(
            !set.contains(&SegmentId::parse("seg-100-0.jsonl")),
            "a deleted WAL needs no retained cancellation: {set:?}"
        );
        assert!(
            !set.contains(&SegmentId::parse("seg-100-1.jsonl")),
            "an identity that stayed under the cap is not: {set:?}"
        );
        s.close().expect("close");
        let _ = fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod directory_lock_tests {
    use super::{Provenance, SegmentConfig, SegmentedSink};

    fn check_release(tag: &str, release: impl FnOnce(SegmentedSink)) {
        let dir = crate::testing::scratch(tag);
        let open = || {
            SegmentedSink::open_write_ahead_only(
                SegmentConfig::new(dir.clone()),
                Provenance::default(),
            )
        };
        let owner = open().unwrap();
        // A duplicate retains the same open file description as a forked child.
        let SegmentedSink {
            _directory_lock: lock,
            ..
        } = &owner;
        let inherited = lock.0.try_clone().unwrap();
        assert_eq!(open().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
        release(owner);
        let successor = open().expect("release ownership even while a duplicate is open");
        drop(inherited);
        assert_eq!(open().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
        // The lock assertions are complete. Closing would also test segment naming:
        // two sinks in this process can share the same millisecond incarnation.
        drop(successor);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn close_releases_directory_lock_with_a_duplicated_descriptor() {
        check_release("close-duplicated-lock", |owner| {
            owner.close().unwrap();
        });
    }

    #[test]
    fn drop_releases_directory_lock_with_a_duplicated_descriptor() {
        check_release("drop-duplicated-lock", drop);
    }

    #[test]
    fn failed_close_releases_directory_lock_with_a_duplicated_descriptor() {
        check_release("failed-close-duplicated-lock", |mut owner| {
            owner
                .error
                .set(std::io::Error::other("injected write failure"));
            assert!(owner.close().is_err());
        });
    }
}
