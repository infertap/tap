// SPDX-License-Identifier: Apache-2.0
//! Why the tap did not run, or did not run to a clean stop: the one error type the
//! binary reports, with the library error underneath where one exists.

use infertap::record::StopReason;
use infertap::source::SourceError;
use std::io;
use std::path::PathBuf;

/// Why the tap did not run, or did not run to a clean stop.
///
/// [`Usage`](Self::Usage) exits 2 with the help text, the convention for a command line the
/// tap refuses; everything else exits 1 with the reason. Each variant names its cause and,
/// where one exists, carries the library error underneath so nothing is flattened to text
/// before the caller has read it. There is no conversion from a string: a usage error is
/// constructed where the command line is refused, and nowhere else.
#[derive(Debug)]
pub(crate) enum CliError {
    /// A flag, a value, or a combination the command line refuses.
    Usage(String),
    /// The egress key was refused.
    Key(infertap::pseudonym::KeyError),
    /// `--key-file` could not be read.
    KeyFile { path: String, source: io::Error },
    /// `--provenance` could not be read, parsed, or accepted. `origin` is the flag itself
    /// for inline JSON, or the file's path.
    Provenance {
        origin: String,
        source: ProvenanceError,
    },
    /// The engine adapter could not be built.
    Source(SourceError),
    /// The output could not be opened, sealed, or flushed.
    Output { what: String, source: io::Error },
    /// The termination signal handlers could not be registered.
    Signal(io::Error),
    /// The run closed with write-ahead segments the converter did not turn into their
    /// shipped form: they will not ship from this incarnation.
    Unconverted { count: usize, dir: PathBuf },
    /// The run ended on a fault the loop named.
    Run(RunFault),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(msg) => f.write_str(msg),
            Self::Key(e) => write!(f, "key: {e}"),
            Self::KeyFile { path, source } => write!(f, "reading --key-file {path}: {source}"),
            Self::Provenance { origin, source } => write!(f, "{origin}: {source}"),
            Self::Source(e) => write!(f, "{e}"),
            Self::Output { what, source } => write!(f, "{what}: {source}"),
            Self::Signal(e) => write!(f, "registering signal handler: {e}"),
            Self::Unconverted { count, dir } => write!(
                f,
                "{count} write-ahead segment(s) in {} left unconverted at close; they do not \
                 ship from this incarnation",
                dir.display()
            ),
            Self::Run(fault) => write!(f, "{fault} (stop reason: {})", fault.reason()),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Key(e) => Some(e),
            Self::KeyFile { source, .. } | Self::Output { source, .. } | Self::Signal(source) => {
                Some(source)
            }
            Self::Provenance { source, .. } => Some(source),
            Self::Source(e) => Some(e),
            Self::Run(fault) => fault.source(),
            Self::Usage(_) | Self::Unconverted { .. } => None,
        }
    }
}

/// Why `--provenance` was refused.
#[derive(Debug)]
pub(crate) enum ProvenanceError {
    /// The file could not be read.
    Read(io::Error),
    /// The text is not JSON.
    Parse(serde_json::Error),
    /// The JSON is not an object.
    NotAnObject,
    /// The object carries a key the stream forbids.
    Invalid(infertap::record::ProvenanceError),
}

impl std::fmt::Display for ProvenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(e) => write!(f, "reading it failed: {e}"),
            Self::Parse(e) => write!(f, "{e}"),
            Self::NotAnObject => f.write_str("provenance must be a JSON object"),
            Self::Invalid(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ProvenanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(e) => Some(e),
            Self::Parse(e) => Some(e),
            Self::Invalid(e) => Some(e),
            Self::NotAnObject => None,
        }
    }
}

/// A fault that ended the run. Each is a stop record's reason and a nonzero exit; the
/// clean stops (a signal, `--duration`, `--max-batches`) are not faults and are not here.
#[derive(Debug)]
pub(crate) enum RunFault {
    /// The resident set exceeded `--max-rss-bytes`.
    MemoryBound { rss: u64, bound: u64 },
    /// The output latched a write error, found by the loop's check or by a heartbeat flush.
    Write {
        during: WriteStep,
        source: io::Error,
    },
    /// The source failed and cannot continue.
    Stream(SourceError),
}

/// Which step found the latched write error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteStep {
    /// The loop's per-wake check of the sink's latch.
    Emit,
    /// The heartbeat flush.
    Flush,
}

impl RunFault {
    /// The stop record's reason for this fault.
    pub(crate) const fn reason(&self) -> StopReason {
        match self {
            Self::MemoryBound { .. } => StopReason::MemoryBound,
            Self::Write { .. } => StopReason::WriteError,
            Self::Stream(_) => StopReason::StreamError,
        }
    }
}

impl std::fmt::Display for RunFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryBound { rss, bound } => write!(
                f,
                "resident set {rss} bytes exceeds --max-rss-bytes {bound}; stopping fail-closed \
                 rather than endanger the node"
            ),
            Self::Write { during, source } => {
                let step = match during {
                    WriteStep::Emit => "write",
                    WriteStep::Flush => "flush",
                };
                write!(
                    f,
                    "output {step} failed ({source}); stopping rather than keep polling and \
                     discarding records"
                )
            }
            Self::Stream(e) => write!(f, "aborting on {e}"),
        }
    }
}

impl std::error::Error for RunFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Write { source, .. } => Some(source),
            Self::Stream(e) => Some(e),
            Self::MemoryBound { .. } => None,
        }
    }
}
