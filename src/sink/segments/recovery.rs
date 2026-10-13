// SPDX-License-Identifier: Apache-2.0
//! Recovery: an `active.jsonl` left by a dead incarnation is sealed under that incarnation's
//! own identity at the next open, or, when its header did not survive, discarded and
//! declared. Anything attributed ships; nothing headerless does.

use super::sync_dir;
use super::ACTIVE;
use super::SEG_PREFIX;
use super::SEG_SUFFIX;
use crate::clock::ProducerMs;
use crate::record::{Record, SegmentRecovered};
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

/// Recover an active WAL using its original segment identity.
/// Preserve complete records, discard a truncated tail, and append a recovery record.
/// A complete final JSON record without a newline is retained.
///
/// If no valid header remains, delete the file and return its nonzero size so the
/// caller can record the discard. Headerless files cannot be attributed safely.
pub(super) fn recover_orphan(dir: &Path) -> io::Result<Option<u64>> {
    let path = dir.join(ACTIVE);
    if !path.exists() {
        return Ok(None);
    }
    let mut file = File::open(&path)?;
    let size = file.metadata()?.len();
    let limit = crate::sink::guard::MAX_RECORD_BYTES;
    let mut header = Vec::new();
    BufReader::new((&mut file).take(limit as u64 + 1)).read_until(b'\n', &mut header)?;
    let identity = (header.len() <= limit && header.last() == Some(&b'\n'))
        .then(|| serde_json::from_slice::<Value>(&header).ok())
        .flatten()
        .filter(|v| v["kind"] == "segment_open")
        .and_then(|v| {
            Some((
                v["incarnation"].as_str()?.to_string(),
                v["segment_seq"].as_u64()?,
            ))
        });

    let Some((incarnation, seq)) = identity else {
        // Headerless: unlinked, and declared by the caller. An empty file declares nothing;
        // there is nothing it could have held.
        fs::remove_file(&path)?;
        sync_dir(dir)?;
        return Ok((size > 0).then_some(size));
    };

    if incarnation.is_empty()
        || !incarnation
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid incarnation in recovery header; active file preserved",
        ));
    }
    let dest = dir.join(format!("{SEG_PREFIX}{incarnation}-{seq}{SEG_SUFFIX}"));
    super::check_seal_destination(&dest)?;

    // Inspect only the final record. Earlier complete lines stay on disk.
    let start = size.saturating_sub(limit as u64 + 1);
    file.seek(SeekFrom::Start(start))?;
    let mut tail = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut tail)?;
    let (keep, needs_newline) = match tail.iter().rposition(|&b| b == b'\n') {
        Some(nl) if nl + 1 == tail.len() => (size, false),
        Some(nl) => {
            let last = &tail[nl + 1..];
            if last.len() < limit
                && serde_json::from_slice::<Value>(last).is_ok_and(|v| v.is_object())
            {
                (size, true)
            } else {
                (start + nl as u64 + 1, false)
            }
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery tail exceeds the record limit; active file preserved",
            ))
        }
    };

    // Attributed: the surviving segment_open stays first, per the contract; the marker
    // rides as a trailer. It carries no provenance, because the header already does.
    let marker = Record::SegmentRecovered(SegmentRecovered {
        at_ms: ProducerMs::now(),
        attributed: true,
        dropped_bytes: size - keep,
    });
    crate::sink::guard::check_size(&marker)?;
    let marker = marker.to_json_line();
    let mut f = OpenOptions::new().write(true).open(&path)?;
    f.set_len(keep)?;
    f.seek(SeekFrom::End(0))?;
    if needs_newline {
        f.write_all(b"\n")?;
    }
    f.write_all(format!("{marker}\n").as_bytes())?;
    f.sync_all()?;
    super::seal_without_replace(&path, &dest)?;
    Ok(None)
}
