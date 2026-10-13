// SPDX-License-Identifier: Apache-2.0
//! Text and JSON diagnostics on stderr.
//!
//! Text records include a timestamp, level, target, and message. Under systemd,
//! priority prefixes replace timestamps so journald supplies time and severity.
//! JSON records expose `ts`, `level`, `target`, and `msg` fields.
//! The journal or container runtime manages retention.

use std::fmt::Write as _;
use std::io::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{Level, LevelFilter, Log, Metadata, Record};

pub(crate) const DEFAULT_LEVEL: LevelFilter = LevelFilter::Info;

/// How a line is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LogFormat {
    /// One human-readable line: timestamp (outside the journal), level, target, message.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Text => "text",
            Self::Json => "json",
        })
    }
}

impl std::str::FromStr for LogFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err("log format: expected text or json".into()),
        }
    }
}

/// Parse a level name the way the flags take it.
pub(crate) fn parse_level(s: &str) -> Result<LevelFilter, String> {
    match s {
        "error" => Ok(LevelFilter::Error),
        "warn" | "warning" => Ok(LevelFilter::Warn),
        "info" => Ok(LevelFilter::Info),
        "debug" => Ok(LevelFilter::Debug),
        "trace" => Ok(LevelFilter::Trace),
        _ => Err("log level: expected error, warn, info, debug or trace".into()),
    }
}

struct Logger {
    format: LogFormat,
    /// stderr is the journal: drop the timestamp the journal supplies and add the
    /// priority prefix it reads.
    journald: bool,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let line = format_line(
            self.format,
            self.journald,
            &rfc3339(SystemTime::now()),
            record.level(),
            record.target(),
            &record.args().to_string(),
        );
        // A failed write to stderr has nowhere to be reported; the line is lost, the
        // process is not.
        let stderr = std::io::stderr();
        let mut h = stderr.lock();
        let _ = h.write_all(line.as_bytes());
        let _ = h.write_all(b"\n");
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

/// Install the logger. Called once, after the command line is parsed and before anything
/// else runs; a second call is a no-op.
pub(crate) fn init(level: LevelFilter, format: LogFormat) {
    let journald = std::env::var_os("JOURNAL_STREAM").is_some();
    if log::set_boxed_logger(Box::new(Logger { format, journald })).is_ok() {
        log::set_max_level(level);
    }
}

/// The journal's priority for a level: syslog numbers, which is what the `<N>` prefix
/// carries.
const fn syslog_priority(level: Level) -> u8 {
    match level {
        Level::Error => 3,
        Level::Warn => 4,
        Level::Info => 6,
        Level::Debug | Level::Trace => 7,
    }
}

const fn level_name(level: Level) -> &'static str {
    match level {
        Level::Error => "error",
        Level::Warn => "warn",
        Level::Info => "info",
        Level::Debug => "debug",
        Level::Trace => "trace",
    }
}

/// One rendered line, without its newline. Pure, so the shape is testable.
fn format_line(
    format: LogFormat,
    journald: bool,
    ts: &str,
    level: Level,
    target: &str,
    msg: &str,
) -> String {
    match format {
        LogFormat::Json => {
            let mut s = String::with_capacity(msg.len() + 96);
            s.push_str("{\"ts\":\"");
            s.push_str(ts);
            s.push_str("\",\"level\":\"");
            s.push_str(level_name(level));
            s.push_str("\",\"target\":");
            s.push_str(&json_string(target));
            s.push_str(",\"msg\":");
            s.push_str(&json_string(msg));
            s.push('}');
            s
        }
        LogFormat::Text => {
            let mut s = String::with_capacity(msg.len() + 64);
            if journald {
                let _ = write!(s, "<{}>", syslog_priority(level));
            } else {
                s.push_str(ts);
                s.push(' ');
            }
            let _ = write!(
                s,
                "{:<5} {target}: {msg}",
                level_name(level).to_ascii_uppercase()
            );
            s
        }
    }
}

/// A JSON string literal, quotes included. `serde_json` is already a dependency and its
/// escaping is the one the readers expect.
fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// RFC 3339 in UTC with milliseconds, `2026-09-16T12:34:56.789Z`, from the wall clock,
/// without a date crate: the civil-from-days arithmetic is a dozen lines and the format
/// is fixed.
fn rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let millis = d.subsec_millis();
    let days = secs / 86_400;
    let (y, m, day) = civil_from_days(days);
    let rem = secs % 86_400;
    format!(
        "{y:04}-{m:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian civil date (Howard Hinnant's algorithm).
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)] // the day and month are small by construction
fn civil_from_days(days: u64) -> (i64, u32, u32) {
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::{format_line, parse_level, rfc3339, LogFormat};
    use log::{Level, LevelFilter};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn timestamps_are_rfc3339_utc_with_milliseconds() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123)),
            "2023-11-14T22:13:20.123Z"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(1_772_236_800)),
            "2026-02-28T00:00:00.000Z"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(1_772_323_200)),
            "2026-03-01T00:00:00.000Z"
        );
    }

    #[test]
    fn json_lines_carry_the_four_fields_and_escape_the_message() {
        let line = format_line(
            LogFormat::Json,
            false,
            "2026-09-16T00:00:00.000Z",
            Level::Warn,
            "infertap::cli::run",
            "dropped=3 \"quoted\"",
        );
        let v: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(v["ts"], "2026-09-16T00:00:00.000Z");
        assert_eq!(v["level"], "warn");
        assert_eq!(v["target"], "infertap::cli::run");
        assert_eq!(v["msg"], "dropped=3 \"quoted\"");
        assert!(!line.contains('\n'));
    }

    #[test]
    fn text_lines_stamp_outside_the_journal_and_prefix_a_priority_inside_it() {
        let plain = format_line(LogFormat::Text, false, "T", Level::Info, "t", "m");
        assert_eq!(plain, "T INFO  t: m");
        let journal = format_line(LogFormat::Text, true, "T", Level::Error, "t", "m");
        assert_eq!(journal, "<3>ERROR t: m");
        assert_eq!(
            format_line(LogFormat::Text, true, "T", Level::Warn, "t", "m"),
            "<4>WARN  t: m"
        );
    }

    #[test]
    fn levels_and_formats_parse_by_name() {
        assert_eq!(parse_level("warn").unwrap(), LevelFilter::Warn);
        assert_eq!(parse_level("warning").unwrap(), LevelFilter::Warn);
        assert!(parse_level("loud").is_err());
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert!("yaml".parse::<LogFormat>().is_err());
    }
}
