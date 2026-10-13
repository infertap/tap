// SPDX-License-Identifier: Apache-2.0
//! Conversion failures and retry classification.

use super::schema::Prim;
use std::io;
use std::path::PathBuf;

/// Why a conversion did not commit a twin.
///
/// [`is_retryable`](Self::is_retryable) is the policy hook: an I/O or writer failure may
/// clear on the next attempt, while a malformed source line cannot. That segment is
/// poison, and the worker stops retrying it.
#[derive(Debug)]
pub enum ConvertError {
    /// The vendored contract schema could not be turned into a column plan, or the plan
    /// disagrees with the writer. A build defect, never a property of one segment.
    Schema(String),
    /// The path is not a sealed `.jsonl` segment.
    NotASegment(PathBuf),
    /// An I/O failure on the source, the temporary, or the directory. `what` names the
    /// step; `path` the file it failed on.
    Io {
        /// The step that failed.
        what: &'static str,
        /// The file it failed on.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// The Parquet writer or reader refused.
    Parquet(parquet::errors::ParquetError),
    /// A line of the source that cannot be encoded. Retrying cannot help.
    Malformed {
        /// The offending line, 1-based.
        line: usize,
        /// What was wrong with it.
        why: Malformed,
    },
    /// The finished twin's footer disagrees with the rows written to it.
    RowCount {
        /// Records written to the temporary.
        written: u64,
        /// Rows the finished file's footer reports.
        reported: i64,
    },
}

impl ConvertError {
    /// Whether a later attempt on the same source could succeed.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Io { .. } | Self::Parquet(_) | Self::RowCount { .. }
        )
    }
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schema(why) => write!(f, "schema: {why}"),
            Self::NotASegment(p) => write!(f, "{} is not a sealed .jsonl segment", p.display()),
            Self::Io { what, path, source } => {
                write!(f, "{what} ({}) failed: {source}", path.display())
            }
            Self::Parquet(e) => write!(f, "parquet: {e}"),
            Self::Malformed { line, why } => write!(f, "line {line}: {why}"),
            Self::RowCount { written, reported } => write!(
                f,
                "the finished twin reports {reported} rows; {written} records were written"
            ),
        }
    }
}

impl std::error::Error for ConvertError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parquet(e) => Some(e),
            Self::Malformed { why, .. } => why.source(),
            Self::Schema(_) | Self::NotASegment(_) | Self::RowCount { .. } => None,
        }
    }
}

/// What made one line of a segment unencodable.
#[derive(Debug)]
pub enum Malformed {
    /// The line is not JSON.
    NotJson(serde_json::Error),
    /// The line is JSON but not an object.
    NotAnObject,
    /// The record's `kind` is missing, or is not a non-empty string.
    KindMissing,
    /// A field's value does not match its column's type.
    TypeMismatch {
        /// The field.
        field: String,
        /// The column's type.
        column: Prim,
    },
    /// A list column's value is not an array of strings.
    NotStringArray {
        /// The field.
        field: String,
    },
    /// `endpoints` is not an array.
    EndpointsNotArray,
    /// An `endpoints` element is not an object.
    EndpointNotObject,
    /// An `endpoints` element carries a field the closed element does not define.
    EndpointUnknownField(String),
    /// An `endpoints` element misses a required field.
    EndpointMissingField(String),
    /// A map value could not be rendered to its string cell.
    DoesNotRender {
        /// The field.
        field: String,
        /// The serializer's error.
        source: serde_json::Error,
    },
}

impl std::fmt::Display for Malformed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotJson(e) => write!(f, "not a JSON record: {e}"),
            Self::NotAnObject => f.write_str("record is not a JSON object"),
            Self::KindMissing => {
                f.write_str("the record's kind is missing, or is not a non-empty string")
            }
            Self::TypeMismatch { field, column } => {
                write!(f, "field {field:?} does not match its column type {column}")
            }
            Self::NotStringArray { field } => {
                write!(f, "field {field:?} is not an array of strings")
            }
            Self::EndpointsNotArray => f.write_str("field \"endpoints\" is not an array"),
            Self::EndpointNotObject => f.write_str("an endpoints element is not an object"),
            Self::EndpointUnknownField(key) => {
                write!(f, "endpoints element carries an unknown field {key:?}")
            }
            Self::EndpointMissingField(name) => {
                write!(f, "endpoints element misses required field {name:?}")
            }
            Self::DoesNotRender { field, source } => {
                write!(f, "field {field:?} does not render: {source}")
            }
        }
    }
}

impl std::error::Error for Malformed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotJson(e) | Self::DoesNotRender { source: e, .. } => Some(e),
            _ => None,
        }
    }
}
