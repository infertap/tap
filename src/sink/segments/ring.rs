// SPDX-License-Identifier: Apache-2.0
//! The disk ring: what is on disk, what it costs, and what to reclaim when the cap is
//! reached. Reclamation runs oldest first, ordered by incarnation and sequence, and each
//! identity is declared and unlinked under one lock hold so the conversion worker cannot
//! resurrect it.

use super::PARQUET_SUFFIX;
use super::SEG_PREFIX;
use super::SEG_SUFFIX;
use super::TMP_SUFFIX;
use crate::clock::ProducerMs;
use crate::record::{Record, SegmentsDropped};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

/// Dropped segment identities shared with the conversion worker.
/// Retention records each identity before unlinking under this lock. Conversion
/// checks the set and WAL existence under the same lock before committing.
/// Retire identities once their WAL is gone; retain them after partial reclamation.
pub(super) type RingDropped = Arc<Mutex<HashSet<SegmentId>>>;

/// Lock the shared dropped-identity set. A poisoned lock yields the set anyway: refusing to
/// read it would reopen the resurrection window the set exists to close.
pub(super) fn lock_ring(set: &RingDropped) -> MutexGuard<'_, HashSet<SegmentId>> {
    set.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Bytes held by conversion temporaries. Real bytes, counted against the cap: ignoring them
/// would breach it during exactly the transient double storage a conversion needs.
pub(super) fn tmp_bytes(dir: &Path) -> io::Result<u64> {
    bytes_where(dir, |n| n.ends_with(TMP_SUFFIX))
}

fn bytes_where(dir: &Path, pick: impl Fn(&str) -> bool) -> io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let picked = path.file_name().and_then(|n| n.to_str()).is_some_and(&pick);
        if picked {
            total += path.metadata().map_or(0, |m| m.len());
        }
    }
    Ok(total)
}

/// The outcome of an unlink attempt. Only successful deletion counts as a loss;
/// a missing file was already removed, and an error reclaims no bytes.
pub(super) enum Unlink {
    /// This process deleted the file: bytes reclaimed, loss declared.
    Dropped,
    /// Already gone: the shipper removed it between listing and unlinking. Bytes are
    /// reclaimed, but the records shipped, and declaring them dropped would contradict
    /// the shipped file in the consumer's hands.
    ShipperWon,
    /// Still on disk: nothing reclaimed, nothing declared, cap enforcement degraded.
    Undeletable(io::Error),
}

pub(super) fn classify_unlink(r: io::Result<()>) -> Unlink {
    match r {
        Ok(()) => Unlink::Dropped,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Unlink::ShipperWon,
        Err(e) => Unlink::Undeletable(e),
    }
}

/// What reclaiming one identity's files came to.
pub(super) struct GroupReclaim {
    /// Bytes no longer on disk, whoever removed them.
    bytes: u64,
    /// Whether any file of the identity is gone: the signal that keeps the identity
    /// recorded in the shared dropped set.
    any_gone: bool,
    /// The first file this process deleted, carried into the declaration. `None` when the
    /// group shipped or could not be deleted, in which cases nothing is declared.
    declared: Option<Sealed>,
}

/// Attempt to unlink every file of one identity before the caller rechecks its budget.
/// Individual deletions can fail; the result accounts for partial reclamation.
pub(super) fn reclaim_group(
    files: &[Sealed],
    undeletable: &mut Option<(u64, io::Error)>,
) -> GroupReclaim {
    let mut out = GroupReclaim {
        bytes: 0,
        any_gone: false,
        declared: None,
    };
    for seg in files {
        match classify_unlink(fs::remove_file(&seg.path)) {
            Unlink::Dropped => {
                out.bytes = out.bytes.saturating_add(seg.size);
                out.any_gone = true;
                if out.declared.is_none() {
                    out.declared = Some(seg.clone());
                }
            }
            Unlink::ShipperWon => {
                out.bytes = out.bytes.saturating_add(seg.size);
                out.any_gone = true;
            }
            // Continue reclaiming other files and report deletion failures to the caller.
            Unlink::Undeletable(e) => match undeletable {
                Some((n, _)) => *n += 1,
                None => *undeletable = Some((1, e)),
            },
        }
    }
    out
}

/// A sealed file with its parsed identity and size.
/// Parse sequences numerically so retention orders sequence 9 before sequence 10.
#[derive(Clone)]
pub(super) struct Sealed {
    pub(super) path: PathBuf,
    pub(super) name: String,
    /// The identity parsed from the name; a write-ahead file and its Parquet twin share one.
    pub(super) id: SegmentId,
    pub(super) size: u64,
}

/// The producing incarnation and its segment sequence.
/// Retention orders by incarnation, then numeric sequence. Names with unparseable
/// sequences sort last.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SegmentId {
    /// `<run_start_unix_ms>-<pid>`, or the whole stem if the name does not fit the pattern.
    pub(super) incarnation: String,
    /// `None` when the trailing component is not an integer.
    pub(super) seq: Option<u64>,
}

impl SegmentId {
    /// Split `seg-<incarnation>-<seq>` (either suffix) into its parts.
    ///
    /// Split on the last `-`: the incarnation contains one of its own, as
    /// `<run_start_ms>-<pid>`, so splitting from the front would take the pid for the seq.
    pub(super) fn parse(name: &str) -> Self {
        let stem = name
            .strip_prefix(SEG_PREFIX)
            .and_then(|s| {
                s.strip_suffix(SEG_SUFFIX)
                    .or_else(|| s.strip_suffix(PARQUET_SUFFIX))
            })
            .unwrap_or(name);
        match stem.rsplit_once('-') {
            Some((incarnation, seq)) => match seq.parse::<u64>() {
                Ok(n) => Self {
                    incarnation: incarnation.to_string(),
                    seq: Some(n),
                },
                Err(_) => Self {
                    incarnation: stem.to_string(),
                    seq: None,
                },
            },
            None => Self {
                incarnation: stem.to_string(),
                seq: None,
            },
        }
    }

    /// The key the ordering sorts by: an unparsed seq after every parsed one.
    fn order(&self) -> (&str, bool, Option<u64>) {
        (&self.incarnation, self.seq.is_none(), self.seq)
    }
}

impl PartialOrd for SegmentId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SegmentId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order().cmp(&other.order())
    }
}

pub(super) fn sealed_segments(dir: &Path) -> io::Result<Vec<Sealed>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !(name.starts_with(SEG_PREFIX)
            && (name.ends_with(SEG_SUFFIX) || name.ends_with(PARQUET_SUFFIX)))
        {
            continue;
        }
        let id = SegmentId::parse(name);
        let size = path.metadata().map_or(0, |m| m.len());
        out.push(Sealed {
            name: name.to_string(),
            path,
            id,
            size,
        });
    }
    Ok(out)
}

/// What one enforcement pass reclaimed: the segments this process deleted, in order, the
/// count plus first error of the files it could not, and the bytes the directory still
/// holds past the cap once every reclaimable segment is gone.
#[derive(Default)]
pub(super) struct Reclaim {
    pub(super) dropped: Vec<Sealed>,
    pub(super) undeletable: Option<(u64, io::Error)>,
    /// Bytes over the cap with nothing left to reclaim: undeletable files alone exceed
    /// it. Zero when the pass brought the directory under the cap.
    pub(super) over_cap: u64,
}

/// Remove the oldest sealed segments until retained bytes fit `cap`.
/// Record identities and unlink files under the shared ring lock. Order by
/// incarnation and numeric sequence; retain unparseable names longest.
pub(super) fn reclaim_over_cap(
    dir: &Path,
    cap: u64,
    ring_dropped: &RingDropped,
) -> io::Result<Reclaim> {
    let mut segs = sealed_segments(dir)?;
    // Once a WAL is gone, the commit guard's existence check prevents resurrection.
    // Retain cancellation only for WALs that survived a partial deletion.
    lock_ring(ring_dropped).retain(|id| {
        segs.iter()
            .any(|s| &s.id == id && s.name.ends_with(SEG_SUFFIX) && s.path.exists())
    });
    let mut total: u64 = segs.iter().map(|s| s.size).sum();
    total = total.saturating_add(tmp_bytes(dir)?);
    if total <= cap {
        return Ok(Reclaim::default());
    }
    // Oldest first by identity order, with the sequence compared as a number.
    segs.sort_by(|a, b| a.id.cmp(&b.id));

    let mut dropped: Vec<Sealed> = Vec::new();
    // Reclaim all files for one identity before checking the budget again.
    // A WAL and its Parquet file count as one lost segment and two file sizes.
    let mut dropped_ids: HashSet<SegmentId> = HashSet::new();
    // Count plus first error: one warning per enforcement pass, not one per file, and
    // the first error is the diagnostic (a directory-wide condition repeats it anyway).
    let mut undeletable: Option<(u64, io::Error)> = None;
    // The identity groups: the sort above put a pair's files side by side.
    for group in segs.chunk_by(|a, b| a.id == b.id) {
        if total <= cap {
            break;
        }
        let identity = group[0].id.clone();
        // Declare and unlink under the same lock used to commit Parquet twins.
        // A committed twin is part of the segment and must be reclaimed with its WAL.
        let reclaim = {
            let mut ring = lock_ring(ring_dropped);
            ring.insert(identity.clone());
            let mut reclaim = reclaim_group(group, &mut undeletable);
            if reclaim.any_gone {
                for seg in group {
                    let twin = seg.path.with_extension("parquet");
                    if !group.iter().any(|s| s.path == twin) && twin.exists() {
                        if let Ok(meta) = twin.metadata() {
                            if fs::remove_file(&twin).is_ok() {
                                reclaim.bytes = reclaim.bytes.saturating_add(meta.len());
                            }
                        }
                    }
                }
            } else {
                // Every file of this identity survived its unlink attempt, so the
                // segment is still live. Un-record it: a recorded identity would make
                // the worker refuse forever to convert a segment nothing declared
                // dropped.
                ring.remove(&identity);
            }
            if !group
                .iter()
                .any(|s| s.name.ends_with(SEG_SUFFIX) && s.path.exists())
            {
                ring.remove(&identity);
            }
            reclaim
        };
        total = total.saturating_sub(reclaim.bytes);
        if let Some(file) = reclaim.declared {
            if dropped_ids.insert(identity) {
                dropped.push(file);
            }
        }
    }
    Ok(Reclaim {
        dropped,
        undeletable,
        over_cap: total.saturating_sub(cap),
    })
}

/// Build one loss record per producing incarnation, stamped at `at_ms`.
/// Use the incarnation whose data was removed, including segments from earlier runs.
/// The first and last sequence delimit the affected range, which may contain gaps.
pub(super) fn dropped_declarations(dropped: &[Sealed], at_ms: ProducerMs) -> Vec<Record> {
    let mut out = Vec::new();
    for group in dropped.chunk_by(|a, b| a.id.incarnation == b.id.incarnation) {
        let incarnation = group[0].id.incarnation.clone();
        // Include sequence bounds so consumers need not parse filenames. Preserve filenames
        // for upload reconciliation. Unparseable sequence values remain absent.
        out.push(Record::SegmentsDropped(SegmentsDropped {
            at_ms,
            incarnation,
            count: group.len() as u64,
            first: group[0].name.clone(),
            last: group[group.len() - 1].name.clone(),
            first_seq: group[0].id.seq,
            last_seq: group[group.len() - 1].id.seq,
        }));
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod unlink_tests {
    use super::{classify_unlink, Unlink};
    use std::io;

    /// The three-way rule, pinned: only a deletion this process performed is a declared
    /// drop; an already-gone file reclaims silently; any other failure reclaims nothing.
    #[test]
    fn only_a_performed_deletion_is_a_declared_drop() {
        assert!(matches!(classify_unlink(Ok(())), Unlink::Dropped));
        assert!(matches!(
            classify_unlink(Err(io::Error::from(io::ErrorKind::NotFound))),
            Unlink::ShipperWon
        ));
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::Other,
            io::ErrorKind::Interrupted,
        ] {
            assert!(
                matches!(
                    classify_unlink(Err(io::Error::from(kind))),
                    Unlink::Undeletable(_)
                ),
                "{kind:?} must reclaim nothing and declare nothing"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod bounded_tracking_tests {
    #[test]
    fn deleted_segments_do_not_accumulate_cancellation_state() {
        let dir = crate::testing::scratch("bounded-ring-tracking");
        let ring = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        for seq in 0..1000 {
            std::fs::write(dir.join(format!("seg-123-456-{seq}.jsonl")), b"record\n").unwrap();
            super::reclaim_over_cap(&dir, 0, &ring).unwrap();
            assert!(super::lock_ring(&ring).is_empty());
        }
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
