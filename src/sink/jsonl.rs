// SPDX-License-Identifier: Apache-2.0
//! `JsonlSink`: stream records as newline-delimited JSON to any writer, such as a file
//! or stdout.
//!
//! One compact JSON object per line: append-only and readable by line-oriented tools. A
//! write error is latched rather than raised, because a passive tap reports a full disk
//! instead of crashing on it. The caller reads the error through [`Sink::write_error`]
//! during the run, and `flush` reports it at the end.

use crate::record::RecordData;

use std::io::{self, Write};

use crate::sink::{Latch, Sink};

/// Writes each record as one line of JSON to `W`.
#[derive(Debug)]
pub struct JsonlSink<W: Write> {
    writer: W,
    written: u64,
    error: Latch,
}

impl<W: Write> JsonlSink<W> {
    /// Wrap a writer. Wrap it in a [`std::io::BufWriter`] first for a high-rate stream.
    #[must_use]
    pub const fn new(writer: W) -> Self {
        Self {
            writer,
            written: 0,
            error: Latch::new(),
        }
    }

    /// Records written so far.
    #[must_use]
    pub const fn written(&self) -> u64 {
        self.written
    }
}

impl<W: Write, P: crate::record::Phase> Sink<RecordData<P>> for JsonlSink<W> {
    fn emit(&mut self, record: RecordData<P>) {
        if self.error.is_set() {
            return; // already broken; do not write a torn stream
        }
        // Serialized straight from the record, one object per line: no intermediate value.
        let result = serde_json::to_writer(&mut self.writer, &record)
            .map_err(io::Error::other)
            .and_then(|()| self.writer.write_all(b"\n"));
        match result {
            Ok(()) => self.written += 1,
            Err(e) => self.error.set(e),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Self::flush(self)
    }
    fn write_error(&self) -> Option<std::io::Error> {
        Self::write_error(self)
    }
}

impl<W: Write> JsonlSink<W> {
    /// Flush output and report any latched error.
    ///
    /// # Errors
    /// Returns a previous write error or the flush error.
    pub fn flush(&mut self) -> io::Result<()> {
        // Surface a captured write error first: the stream is incomplete regardless of
        // flush. A report is returned and the latch stays, so a caller that flushes, sees
        // the error, and emits anyway cannot write a record after a torn line. A failed
        // flush latches the same way.
        self.error.check()?;
        if let Err(e) = self.writer.flush() {
            self.error.set(e);
            return self.error.check();
        }
        Ok(())
    }

    /// The first output error, if any.
    pub fn write_error(&self) -> Option<io::Error> {
        self.error.report()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::JsonlSink;
    use crate::record::Record;
    use crate::sink::Sink;
    use crate::testing::store;

    /// A writer where the disk fills after the first complete line. (One record is several
    /// `write` calls, so the cutoff keys on the newline, not a call count.)
    struct FailingWriter {
        saw_newline: bool,
    }

    impl std::io::Write for FailingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.saw_newline {
                return Err(std::io::Error::other("disk full"));
            }
            if buf.contains(&b'\n') {
                self.saw_newline = true;
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_write_error_is_captured_reported_and_non_crashing() {
        let mut s = JsonlSink::new(FailingWriter { saw_newline: false });
        s.emit(Record::Store(store(1))); // fits in the one good write
        s.emit(Record::Store(store(2))); // disk full: captured, not panicked
        s.emit(Record::Store(store(3))); // after the error: dropped, no torn stream
        assert_eq!(s.written(), 1);
        // Reading the error must not clear it or hide it from flush.
        let report = s
            .write_error()
            .expect("the failure is visible, not swallowed");
        assert_eq!(report.kind(), std::io::ErrorKind::Other);
        assert_eq!(
            report.get_ref().map(ToString::to_string).as_deref(),
            Some("disk full")
        );
        assert!(s.write_error().is_some());
        assert!(s.flush().is_err());
        // Further records stay refused after the error has been reported.
        assert!(s.write_error().is_some());
        s.emit(Record::Store(store(4)));
        assert_eq!(s.written(), 1);
        assert!(s.flush().is_err());
    }

    #[test]
    fn writes_one_json_object_per_line_with_the_kind_first() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut s = JsonlSink::new(&mut buf);
            s.emit(Record::Store(store(1)));
            s.emit(Record::Store(store(2)));
            assert_eq!(s.written(), 2);
            assert!(s.write_error().is_none());
        }
        let text = String::from_utf8(buf).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("{\"kind\":\"store\","));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(lines[1]).unwrap()["block_id"],
            "2"
        );
    }
}
