// SPDX-License-Identifier: Apache-2.0
//! Convert sealed JSONL segments to Parquet.
//!
//! [`Converter::convert_guarded`] writes and syncs a temporary file, verifies its
//! footer and record count, and commits it through a caller-supplied closure. The
//! caller synchronizes commit with retention and removes the source WAL afterward.
//! A crash between commit and WAL removal can leave both files.
//!
//! Column definitions come from the vendored inference-events schema. Unknown fields
//! are retained in the `extra` map. Byte-limited row groups bound conversion buffers.
//! An invalid source line fails the conversion rather than producing a partial segment.

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use serde_json::Value;

use crate::sink::segments::{PARQUET_SUFFIX, SEG_SUFFIX, TMP_SUFFIX};

mod encoding;
mod error;
mod schema;
use encoding::{malformed, Batch};
pub use error::{ConvertError, Malformed};
use schema::ColumnPlan;
pub use schema::Prim;

fn io_err(what: &'static str, path: &Path, source: io::Error) -> ConvertError {
    ConvertError::Io {
        what,
        path: path.to_path_buf(),
        source,
    }
}

/// Close a row group once the accumulated buffers reach this many approximate bytes.
const DEFAULT_ROW_GROUP_BYTES: usize = 8 * 1024 * 1024;

/// Converts sealed JSONL segments to their Parquet twins. The schema is parsed once at
/// construction; one instance serves any number of conversions.
#[derive(Debug)]
pub struct Converter {
    plan: ColumnPlan,
    props: Arc<WriterProperties>,
    row_group_bytes: usize,
}

impl Converter {
    /// Parse the embedded contract schema and build the column plan.
    ///
    /// # Errors
    /// Returns the reason when the vendored schema is malformed, or when it types one
    /// field two ways across kinds.
    pub fn new() -> Result<Self, ConvertError> {
        Self::with_row_group_bytes(DEFAULT_ROW_GROUP_BYTES)
    }

    fn with_row_group_bytes(row_group_bytes: usize) -> Result<Self, ConvertError> {
        let plan = ColumnPlan::from_contract()?;
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        Ok(Self {
            plan,
            props,
            row_group_bytes,
        })
    }

    /// Convert a sealed JSONL segment and pass its verified temporary file to `commit`.
    /// The callback receives `(temporary, destination)` and performs the rename under
    /// any caller-owned synchronization. Return `Ok(true)` after renaming or `Ok(false)`
    /// to refuse publication. Refusal removes the temporary and returns `Ok(None)`.
    /// The source JSONL remains in place; the caller removes it after a successful commit.
    ///
    /// # Errors
    /// Returns an error for invalid filenames or records, I/O or encoding failures,
    /// footer count mismatches, and failures reported by the commit callback.
    pub fn convert_guarded(
        &self,
        jsonl: &Path,
        commit: &dyn Fn(&Path, &Path) -> io::Result<bool>,
    ) -> Result<Option<PathBuf>, ConvertError> {
        let name = jsonl
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| ConvertError::NotASegment(jsonl.to_path_buf()))?;
        let stem = name
            .strip_suffix(SEG_SUFFIX)
            .ok_or_else(|| ConvertError::NotASegment(jsonl.to_path_buf()))?;
        let parent = match jsonl.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let tmp = parent.join(format!("{stem}{TMP_SUFFIX}"));
        let dest = parent.join(format!("{stem}{PARQUET_SUFFIX}"));
        match self.convert_inner(jsonl, &tmp, &dest, &parent, commit) {
            Ok(true) => Ok(Some(dest)),
            Ok(false) => Ok(None),
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                Err(e)
            }
        }
    }

    /// Returns whether the twin was committed; `false` means the guard refused and the
    /// temporary was unlinked.
    fn convert_inner(
        &self,
        jsonl: &Path,
        tmp: &Path,
        dest: &Path,
        parent: &Path,
        commit: &dyn Fn(&Path, &Path) -> io::Result<bool>,
    ) -> Result<bool, ConvertError> {
        let rows = self.write_tmp(jsonl, tmp)?;
        verify_tmp(tmp, rows)?;
        // Commit only after verification. The caller synchronizes the final check
        // and rename with retention.
        let committed = commit(tmp, dest)
            .map_err(|e| io_err("renaming the verified twin into place", dest, e))?;
        if !committed {
            let _ = fs::remove_file(tmp);
            return Ok(false);
        }
        File::open(parent)
            .and_then(|d| d.sync_all())
            .map_err(|e| io_err("fsync of the directory", parent, e))?;
        Ok(true)
    }

    /// Stream the JSONL into the tmp file and fsync it. Returns the record count.
    fn write_tmp(&self, jsonl: &Path, tmp: &Path) -> Result<u64, ConvertError> {
        let src = File::open(jsonl).map_err(|e| io_err("opening the source", jsonl, e))?;
        let out = File::create(tmp).map_err(|e| io_err("creating the temporary", tmp, e))?;
        let mut writer =
            SerializedFileWriter::new(out, Arc::clone(&self.plan.schema), Arc::clone(&self.props))
                .map_err(ConvertError::Parquet)?;
        let mut batch = Batch::for_plan(&self.plan.leaves);
        let mut rows: u64 = 0;
        for (idx, line) in BufReader::new(src).lines().enumerate() {
            let line = line.map_err(|e| io_err("reading the source", jsonl, e))?;
            if line.trim().is_empty() {
                continue;
            }
            let n = idx + 1;
            let record: Value =
                serde_json::from_str(&line).map_err(|e| malformed(n, Malformed::NotJson(e)))?;
            let obj = record
                .as_object()
                .ok_or_else(|| malformed(n, Malformed::NotAnObject))?;
            self.plan.append(obj, n, &mut batch)?;
            rows += 1;
            if batch.bytes >= self.row_group_bytes {
                self.plan.write_group(&mut writer, &batch)?;
                batch.clear();
            }
        }
        if batch.rows > 0 {
            self.plan.write_group(&mut writer, &batch)?;
        }
        let file = writer.into_inner().map_err(ConvertError::Parquet)?;
        file.sync_all()
            .map_err(|e| io_err("fsync of the temporary", tmp, e))?;
        Ok(rows)
    }
}

/// Reopen the temporary file and compare its footer row count with the number
/// of records written. Reject unreadable files and count mismatches.
fn verify_tmp(tmp: &Path, rows: u64) -> Result<(), ConvertError> {
    let file = File::open(tmp).map_err(|e| io_err("re-opening the temporary", tmp, e))?;
    let reader = SerializedFileReader::new(file).map_err(ConvertError::Parquet)?;
    let n = reader.metadata().file_metadata().num_rows();
    if u64::try_from(n) == Ok(rows) {
        Ok(())
    } else {
        Err(ConvertError::RowCount {
            written: rows,
            reported: n,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::Converter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::{Field, Row};
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::fs::{self, File};
    use std::path::{Path, PathBuf};

    /// The unguarded conversion: rename and report it done.
    fn convert(conv: &Converter, jsonl: &Path) -> Result<PathBuf, super::ConvertError> {
        Ok(conv
            .convert_guarded(jsonl, &|tmp: &Path, dest: &Path| {
                fs::rename(tmp, dest)?;
                Ok(true)
            })?
            .expect("an unconditional commit never refuses"))
    }

    const CORPUS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/conformance/delivery/records.json"
    ));
    const TWIN: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/conformance/delivery/seg-1785153670000-4242-0.parquet"
    );

    fn scratch(tag: &str) -> PathBuf {
        crate::testing::scratch(&format!("convert-{tag}"))
    }

    fn segment_lines() -> Vec<Value> {
        let corpus: Value = serde_json::from_str(CORPUS).expect("the corpus parses");
        corpus["segment"]["lines"]
            .as_array()
            .expect("segment.lines is an array")
            .clone()
    }

    fn write_jsonl(path: &Path, lines: &[Value]) {
        let mut text = String::new();
        for line in lines {
            text.push_str(&line.to_string());
            text.push('\n');
        }
        fs::write(path, text).expect("write the segment");
    }

    /// Decode every row through the core reader's record API.
    fn rows_of(path: &Path) -> Vec<Row> {
        let reader = SerializedFileReader::new(File::open(path).expect("open the parquet"))
            .expect("the file opens as Parquet");
        reader
            .get_row_iter(None)
            .expect("row iterator")
            .collect::<Result<Vec<_>, _>>()
            .expect("every row decodes")
    }

    fn field<'a>(row: &'a Row, name: &str) -> &'a Field {
        row.get_column_iter()
            .find(|(n, _)| n.as_str() == name)
            .map(|(_, f)| f)
            .expect("the column exists")
    }

    fn extra_of(row: &Row) -> BTreeMap<String, String> {
        let Field::MapInternal(m) = field(row, "extra") else {
            panic!("extra is not a map");
        };
        m.entries()
            .iter()
            .map(|(k, v)| {
                let (Field::Str(k), Field::Str(v)) = (k, v) else {
                    panic!("an extra entry is not two strings");
                };
                (k.clone(), v.clone())
            })
            .collect()
    }

    /// Decode through the arrow reader, the second implementation, and count the rows.
    fn arrow_row_count(path: &Path) -> usize {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open the parquet"))
                .expect("the arrow reader accepts the file")
                .build()
                .expect("the arrow reader builds");
        reader
            .map(|b| b.expect("the batch decodes").num_rows())
            .sum()
    }

    #[test]
    fn a_sealed_segment_round_trips() {
        let dir = scratch("roundtrip");
        let lines = segment_lines();
        let src = dir.join("seg-1785153670000-4242-0.jsonl");
        write_jsonl(&src, &lines);

        let out = convert(&Converter::new().expect("the vendored schema parses"), &src)
            .expect("the conversion succeeds");
        assert_eq!(out, dir.join("seg-1785153670000-4242-0.parquet"));
        assert!(src.exists(), "the JSONL stays; the caller owns the delete");

        assert_eq!(arrow_row_count(&out), lines.len());

        let rows = rows_of(&out);
        let kinds: Vec<&str> = rows
            .iter()
            .map(|r| {
                let Field::Str(s) = field(r, "kind") else {
                    panic!("kind is not a string");
                };
                s.as_str()
            })
            .collect();
        // Every kind the corpus segment carries, in order: the heartbeat is the fixture's one
        // nested column (endpoints, a list of the closed element), and the tail exists so a
        // clear, a holder_reset and a second instance are reachable.
        let pinned: Vec<&str> = lines
            .iter()
            .map(|l| l["kind"].as_str().expect("kind"))
            .collect();
        assert_eq!(kinds, pinned);
        assert!(kinds.contains(&"heartbeat") && kinds.contains(&"holder_reset"));

        let extra = extra_of(&rows[0]);
        assert_eq!(extra.get("gpu").map(String::as_str), Some("fixture"));
        assert_eq!(
            extra.get("vllm_version").map(String::as_str),
            Some("0.26.0")
        );
        assert_eq!(
            extra.get("event_schema_version").map(String::as_str),
            Some("vllm-0.26.0-map")
        );

        assert!(matches!(field(&rows[1], "n_tokens"), Field::Long(16)));
        assert!(matches!(field(&rows[2], "n_tokens"), Field::Long(16)));
        assert!(matches!(field(&rows[3], "msgs_seen"), Field::Long(3)));
        assert!(matches!(field(&rows[4], "n_tokens"), Field::Null));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_committed_twin_and_our_writer_decode_alike() {
        let dir = scratch("interop");
        let lines = segment_lines();
        let src = dir.join("seg-1785153670000-4242-0.jsonl");
        write_jsonl(&src, &lines);
        let ours = convert(&Converter::new().expect("the vendored schema parses"), &src)
            .expect("the conversion succeeds");
        assert_eq!(arrow_row_count(&ours), lines.len());

        let theirs = rows_of(Path::new(TWIN));
        let mine = rows_of(&ours);
        let seq = |rows: &[Row]| {
            rows.iter()
                .map(|r| {
                    let Field::Str(kind) = field(r, "kind") else {
                        panic!("kind is not a string");
                    };
                    let Field::Double(ms) = field(r, "at_ms") else {
                        panic!("at_ms is not a double");
                    };
                    (kind.clone(), ms.to_bits())
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(seq(&theirs), seq(&mine));
        assert_eq!(extra_of(&theirs[0]), extra_of(&mine[0]));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn verification_style_reader_rejects_a_truncated_twin() {
        let dir = scratch("truncate");
        let src = dir.join("seg-1-0.jsonl");
        write_jsonl(&src, &segment_lines());
        let out = convert(&Converter::new().expect("the vendored schema parses"), &src)
            .expect("the conversion succeeds");

        let len = fs::metadata(&out).expect("stat the twin").len();
        let f = fs::OpenOptions::new()
            .write(true)
            .open(&out)
            .expect("open the twin for truncation");
        f.set_len(len - 10).expect("truncate the twin");
        drop(f);

        let opened = SerializedFileReader::new(File::open(&out).expect("open the twin"));
        assert!(opened.is_err(), "a partial Parquet must not open");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_torn_line_fails_closed() {
        let dir = scratch("torn");
        let src = dir.join("seg-1-0.jsonl");
        fs::write(
            &src,
            "{\"kind\":\"store\",\"at_ms\":1.0,\"n_tokens\":16}\n{\"kind\":\"evict\",\"at_ms\"",
        )
        .expect("write the torn segment");

        let result = convert(&Converter::new().expect("the vendored schema parses"), &src);
        assert!(result.is_err(), "a torn line must fail the conversion");
        assert!(!dir.join("seg-1-0.parquet").exists(), "no twin appears");
        assert!(!dir.join("seg-1-0.parquet.tmp").exists(), "no tmp remains");
        assert!(src.exists(), "the JSONL stays for the retry");

        let _ = fs::remove_dir_all(&dir);
    }

    /// A record kind must be a non-empty JSON string. Null, missing, and
    /// wrongly typed kinds fail the whole conversion.
    #[test]
    fn a_kind_that_is_not_a_nonempty_string_fails_closed() {
        let dir = scratch("kind-guard");
        for (tag, line) in [
            ("null", "{\"kind\":null,\"at_ms\":1.0}"),
            ("empty", "{\"kind\":\"\",\"at_ms\":1.0}"),
            ("missing", "{\"at_ms\":1.0}"),
            ("nonstring", "{\"kind\":7,\"at_ms\":1.0}"),
        ] {
            let src = dir.join(format!("seg-{tag}-0.jsonl"));
            fs::write(&src, format!("{line}\n")).expect("write the segment");
            let result = convert(&Converter::new().expect("the vendored schema parses"), &src);
            assert!(result.is_err(), "a {tag} kind must fail the conversion");
            assert!(
                !dir.join(format!("seg-{tag}-0.parquet")).exists(),
                "no twin appears for a {tag} kind"
            );
            assert!(
                !dir.join(format!("seg-{tag}-0.parquet.tmp")).exists(),
                "no tmp remains for a {tag} kind"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// The commit guard runs after verification and before the rename. A refusal leaves no
    /// twin, no temporary, and an untouched source; a permission commits normally.
    #[test]
    fn a_refused_commit_guard_aborts_without_a_twin() {
        let dir = scratch("guard");
        let src = dir.join("seg-1-0.jsonl");
        write_jsonl(&src, &segment_lines());
        let conv = Converter::new().expect("the vendored schema parses");

        let aborted = conv
            .convert_guarded(&src, &|_: &Path, _: &Path| Ok(false))
            .expect("an abort is not an error");
        assert!(aborted.is_none(), "a refused guard commits nothing");
        assert!(!dir.join("seg-1-0.parquet").exists(), "no twin appears");
        assert!(
            !dir.join("seg-1-0.parquet.tmp").exists(),
            "the abort unlinks the temporary"
        );
        assert!(src.exists(), "the source is untouched");

        let committed = convert(&conv, &src).expect("the conversion succeeds");
        assert_eq!(
            committed,
            dir.join("seg-1-0.parquet"),
            "a permitted guard commits the twin"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_large_segment_spans_row_groups() {
        let dir = scratch("rowgroups");
        let src = dir.join("seg-1-0.jsonl");
        let mut text = String::new();
        for i in 0..256 {
            use std::fmt::Write as _;
            writeln!(
                text,
                "{{\"kind\":\"store\",\"at_ms\":{i}.0,\"instance_id\":\"i0\",\
                 \"block_id\":\"{i:0>64}\",\"n_tokens\":16,\"tier\":\"gpu\"}}"
            )
            .expect("writing to a String cannot fail");
        }
        fs::write(&src, &text).expect("write the segment");

        let converter = Converter::with_row_group_bytes(4096).expect("the vendored schema parses");
        let out = convert(&converter, &src).expect("the conversion succeeds");

        let reader = SerializedFileReader::new(File::open(&out).expect("open the twin"))
            .expect("the file opens as Parquet");
        assert!(
            reader.metadata().num_row_groups() > 1,
            "the byte bound must close more than one row group"
        );
        assert_eq!(reader.metadata().file_metadata().num_rows(), 256);
        drop(reader);
        assert_eq!(arrow_row_count(&out), 256);
        assert_eq!(rows_of(&out).len(), 256);

        let _ = fs::remove_dir_all(&dir);
    }
}
