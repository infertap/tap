// SPDX-License-Identifier: Apache-2.0
//! Sinks: where the tap's records go.
//!
//! [`Sink`] accepts raw records by default. The pseudonymizing sink consumes raw records
//! and emits [`crate::record::PseudonymizedRecord`]; output writers accept either form.
//! [`GuardedSink`] checks serialized record size. The segment writer also checks records
//! it generates internally. The pseudonymizer scans its output for unexpected raw values
//! and panics before emission on a match. See [`noraw`] for scan exclusions.
//!
//! These checks do not classify arbitrary text as content. The adapter drops token IDs,
//! while operator labels and provenance remain operator-controlled metadata.

use std::io;

use crate::record::Record;

pub mod convert;
pub mod guard;
pub mod jsonl;
pub mod memory;
pub mod noraw;
pub mod pseudonymizing;
pub mod segments;

pub use guard::GuardedSink;
pub use jsonl::JsonlSink;
pub use memory::MemorySink;
pub use noraw::{assert_no_raw, LeakKind, RawLeak};
pub use pseudonymizing::{PseudonymConfig, PseudonymizingSink};
pub use segments::{SegmentConfig, SegmentedSink};

/// Accepts records of one representation. The default input is a raw [`Record`].
/// Writers may implement this trait for both raw and pseudonymized records.
pub trait Sink<R = Record> {
    /// Accept one record. A sink that cannot fail stores or writes it; one that can fail
    /// latches its first error for `write_error`.
    fn emit(&mut self, record: R);

    /// Flush buffered output. Default no-op; a streaming sink overrides it, and a decorator
    /// delegates to the sink it wraps, so a single `flush` reaches the writer through the chain.
    ///
    /// # Errors
    /// Returns the writer's I/O error if the flush (or a captured earlier write) failed.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Report the first captured write error without consuming it.
    /// The poll loop checks this after emission. Failed writers stop accepting records and
    /// retain the fault through subsequent flushes. Decorators delegate to the inner sink;
    /// infallible sinks return `None`.
    fn write_error(&self) -> Option<io::Error> {
        None
    }
}

/// The first write error a sink met, kept for the life of the sink.
///
/// A sink that latches never writes after a torn line: every later emit is dropped, and
/// every later flush reports the same fault. The fault is shared, not cloned, because
/// [`io::Error`] cannot be cloned and a rendering of it would lose the kind's inner error;
/// each report wraps the shared original, so a reader that inspects
/// [`io::Error::get_ref`] finds the fault itself.
#[derive(Debug, Default)]
pub(crate) struct Latch(Option<std::sync::Arc<io::Error>>);

impl Latch {
    /// Nothing latched.
    #[must_use]
    pub const fn new() -> Self {
        Self(None)
    }

    /// Latch `e` unless an earlier fault is already latched: the first fault is the cause,
    /// and every later one is its consequence.
    pub fn set(&mut self, e: io::Error) {
        if self.0.is_none() {
            self.0 = Some(std::sync::Arc::new(e));
        }
    }

    /// Whether a fault is latched.
    #[must_use]
    pub const fn is_set(&self) -> bool {
        self.0.is_some()
    }

    /// A reportable copy of the latched fault: the same kind, with the shared original as
    /// its inner error. `None` when nothing is latched.
    #[must_use]
    pub fn report(&self) -> Option<io::Error> {
        self.0
            .as_ref()
            .map(|e| io::Error::new(e.kind(), std::sync::Arc::clone(e)))
    }

    /// `Ok` when nothing is latched, else the report as an `Err`.
    ///
    /// # Errors
    /// The latched fault, as [`Self::report`] renders it.
    pub fn check(&self) -> io::Result<()> {
        self.report().map_or(Ok(()), Err)
    }
}

impl<R> Sink<R> for Box<dyn Sink<R>> {
    fn emit(&mut self, record: R) {
        (**self).emit(record);
    }
    fn flush(&mut self) -> io::Result<()> {
        (**self).flush()
    }
    fn write_error(&self) -> Option<io::Error> {
        (**self).write_error()
    }
}
