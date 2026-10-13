// SPDX-License-Identifier: Apache-2.0
//! Where records go: the output sink (sealed segments or one file), the egress chain
//! (pseudonymized or explicitly raw), and the key that decides between them.

use crate::cli::error::CliError;
use crate::cli::opts::{KeySource, Opts, Output, DEFAULT_KEY_EPOCH};
use infertap::pseudonym::{Key, BLOCK_ID_CONTEXT};
use infertap::record::{Provenance, Pseudonymized, PseudonymizedRecord};
use infertap::sink::segments::{
    SegmentConfig, SegmentedSink, CLOSED_MARKER_PREFIX, DEFAULT_MAX_BYTES, DEFAULT_MAX_DISK_BYTES,
    DEFAULT_MAX_SECS, INCARNATION_MARKER,
};
use infertap::sink::{JsonlSink, PseudonymConfig, PseudonymizingSink, Sink};

/// Resolve the configured pseudonymization key.
/// Return `None` only for explicit raw mode; reject missing key configuration.
/// Key files avoid exposing key material in process arguments.
pub(crate) fn resolve_key(options: &Opts) -> Result<Option<Key>, CliError> {
    let material = match &options.key {
        Some(KeySource::Argv(k)) => k.clone(),
        Some(KeySource::File(path)) => {
            std::fs::read_to_string(path).map_err(|e| CliError::KeyFile {
                path: path.display().to_string(),
                source: e,
            })?
        }
        Some(KeySource::Raw) => return Ok(None),
        None => {
            return Err(CliError::Usage(
                "no key configured: provide --key-file (or --key) to pseudonymize at egress, \
                 or pass --raw-ids to explicitly run keyless (raw content fingerprints and \
                 cleartext salts at rest; never for records that leave the host)"
                    .to_string(),
            ))
        }
    };
    let key = Key::from_hex_or_base64(&material)
        .map(Some)
        .map_err(CliError::Key);
    // Zero the decoded input buffer on both success and failure. This does not remove
    // an argv-supplied key from the process's original argument storage.
    let mut text = material.into_bytes();
    infertap::pseudonym::zero(&mut text);
    key
}

/// Where records land. Both forms refuse to destroy an existing stream: segments are sealed
/// and immutable, and the one-shot file is opened `create_new` so it can never truncate.
pub(crate) enum OutputSink {
    /// Service form: sealed, immutable segments. A crash costs at most the active one.
    /// Boxed: the sink carries the conversion worker, and the size gap to the one-shot
    /// variant would otherwise tax every enum move.
    Segments(Box<SegmentedSink>),
    /// One-shot form: a single file for a bounded run.
    File(JsonlSink<std::io::BufWriter<std::fs::File>>),
}

impl<P: infertap::record::Phase> Sink<infertap::record::RecordData<P>> for OutputSink {
    fn emit(&mut self, record: infertap::record::RecordData<P>) {
        match self {
            Self::Segments(s) => s.emit(record),
            Self::File(f) => f.emit(record),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Segments(s) => s.flush(),
            Self::File(f) => f.flush(),
        }
    }
    fn write_error(&self) -> Option<std::io::Error> {
        match self {
            Self::Segments(s) => s.write_error(),
            Self::File(f) => f.write_error(),
        }
    }
}

impl OutputSink {
    pub(crate) fn written(&self) -> u64 {
        match self {
            Self::Segments(s) => s.written(),
            Self::File(f) => f.written(),
        }
    }

    /// Seal or flush and report where the stream ended up. Sealing on a clean exit is what
    /// makes the final records shippable at all.
    pub(crate) fn finish(self, output: &Output) -> Result<(), CliError> {
        let target = target(output);
        match self {
            Self::Segments(s) => {
                let (sealed, dropped) = (s.sealed(), s.dropped_segments());
                let incarnation = s.incarnation().to_string();
                let closed = s.close().map_err(|e| CliError::Output {
                    what: format!("sealing final segment in {}", target.display()),
                    source: e,
                })?;
                log::info!(
                    "sealed {} segment(s); {dropped} dropped to stay under the disk cap",
                    sealed + 1
                );
                // The marker says what the incarnation left, so a shipper waiting on it
                // learns the truth; then the run fails for it, since in a pod those
                // segments are lost with the emptyDir.
                let unconverted = closed.unconverted.unwrap_or(0);
                if let Output::Segments(root) = output {
                    mark_closed(root, &incarnation, unconverted);
                }
                if unconverted > 0 {
                    return Err(CliError::Unconverted {
                        count: unconverted,
                        dir: target,
                    });
                }
                Ok(())
            }
            // The flush reports a latched write error before it flushes, so one call covers
            // both the stream's history and its tail.
            Self::File(mut f) => f.flush().map_err(|e| CliError::Output {
                what: format!("flushing {}", target.display()),
                source: e,
            }),
        }
    }
}

/// The stream this build writes: the inference-events KV-cache record stream. `--out-dir`
/// is a root the tap owns. Each stream is a subdirectory of it, and the shipper moves the
/// subdirectory under the same name in the bucket. A future telemetry type is a new
/// contract and a new stream beside this one, never new kinds in it (`docs/streams.md`).
pub(crate) const KV_STREAM: &str = "kv";

/// Build segment settings from resolved options and shared storage defaults.
pub(crate) fn segment_config(options: &Opts, dir: &std::path::Path) -> SegmentConfig {
    SegmentConfig {
        dir: dir.to_path_buf(),
        max_bytes: options.segment_bytes.unwrap_or(DEFAULT_MAX_BYTES),
        max_secs: options.segment_secs.unwrap_or(DEFAULT_MAX_SECS),
        max_disk_bytes: Some(options.max_disk_bytes.unwrap_or(DEFAULT_MAX_DISK_BYTES)),
        workload_class: options.workload_class.clone(),
        heartbeat_secs: options.heartbeat_secs,
        // Declared only where identities are keyed: unkeyed records have no identity space to
        // scope, and stating an epoch for one would name a thing that does not exist.
        key_epoch: options
            .key
            .as_ref()
            .is_some_and(KeySource::is_keyed)
            .then(|| options.key_epoch.unwrap_or(DEFAULT_KEY_EPOCH)),
    }
}

/// The path an [`Output`] writes: the stream directory under the root, or the file. What
/// the closing report names, and what a probe checks.
pub(crate) fn target(output: &Output) -> std::path::PathBuf {
    match output {
        // The root is the tap's; the stream lives one level down, so a second stream can
        // live beside it and anything else the tap ever writes stays out of both.
        Output::Segments(root) => root.join(KV_STREAM),
        Output::File(path) => path.clone(),
    }
}

/// The configured output, or the usage error a run without one gets.
pub(crate) fn configured_output(options: &Opts) -> Result<&Output, CliError> {
    options
        .output
        .as_ref()
        .ok_or_else(|| CliError::Usage("--out-dir or --out is required".to_string()))
}

/// Validate segment headers before connecting or opening output.
pub(crate) fn validate_output(options: &Opts, provenance: &Provenance) -> Result<(), CliError> {
    if let Output::Segments(_) = configured_output(options)? {
        let dir = target(configured_output(options)?);
        segment_config(options, &dir)
            .validate(provenance)
            .map_err(|e| CliError::Usage(e.to_string()))?;
    }
    Ok(())
}

/// Open output after socket setup succeeds. ZMQ connects asynchronously, so successful
/// setup does not establish that the publisher is reachable.
pub(crate) fn open_output(options: &Opts, provenance: &Provenance) -> Result<OutputSink, CliError> {
    let output = configured_output(options)?;
    let target = target(output);
    match output {
        Output::Segments(_) => {
            let cfg = segment_config(options, &target);
            let sink =
                SegmentedSink::open(cfg, provenance.clone()).map_err(|e| CliError::Output {
                    what: format!("opening segment directory {}", target.display()),
                    source: e,
                })?;
            log::info!(
                "writing sealed segments to {} (incarnation {})",
                target.display(),
                sink.incarnation()
            );
            if let Output::Segments(root) = output {
                mark_open(root, sink.incarnation());
            }
            Ok(OutputSink::Segments(Box::new(sink)))
        }
        Output::File(_) => {
            // Create a new file exclusively so an existing stream cannot be truncated.
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target)
                .map_err(|e| CliError::Output {
                    what: format!("creating {} (it must not already exist)", target.display()),
                    source: e,
                })?;
            Ok(OutputSink::File(JsonlSink::new(std::io::BufWriter::new(
                file,
            ))))
        }
    }
}

/// The shutdown handshake's opening half: at the output root, remove any earlier
/// incarnation's closed marker and name this one in [`INCARNATION_MARKER`], written
/// whole through a rename so a reader never sees a partial name. Best effort: the run
/// does not depend on it, and a shipper that cannot read it runs its wait's budget out.
fn mark_open(root: &std::path::Path, incarnation: &str) {
    let attempt = || -> std::io::Result<()> {
        for entry in std::fs::read_dir(root)?.flatten() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(CLOSED_MARKER_PREFIX))
            {
                std::fs::remove_file(entry.path())?;
            }
        }
        let tmp = root.join(format!("{INCARNATION_MARKER}.tmp"));
        std::fs::write(&tmp, incarnation)?;
        std::fs::rename(tmp, root.join(INCARNATION_MARKER))
    };
    if let Err(e) = attempt() {
        log::warn!(
            "could not write the incarnation marker in {} ({e}); a shipper draining at \
             termination will wait its full budget instead of the tap's close",
            root.display()
        );
    }
}

/// The handshake's closing half: `closed-<incarnation>` at the output root, after the
/// last segment is sealed and its conversion has settled, holding the count of segments
/// left unconverted, synced to disk. The shipper's drain waits on exactly this file and
/// reads the count: `0` is a complete incarnation, anything else is one whose last
/// segments will not ship.
fn mark_closed(root: &std::path::Path, incarnation: &str, unconverted: usize) {
    let attempt = || -> std::io::Result<()> {
        use std::io::Write as _;
        // Whole or absent: the count is written to a temporary, synced, then renamed
        // into the name the shipper waits on, so no reader sees an empty marker.
        let tmp = root.join(format!("{CLOSED_MARKER_PREFIX}{incarnation}.tmp"));
        let mut file = std::fs::File::create(&tmp)?;
        write!(file, "{unconverted}")?;
        file.sync_all()?;
        std::fs::rename(
            tmp,
            root.join(format!("{CLOSED_MARKER_PREFIX}{incarnation}")),
        )
    };
    if let Err(e) = attempt() {
        log::warn!(
            "could not write the closed marker in {} ({e}); a shipper draining at \
             termination will wait its full budget",
            root.display()
        );
    }
}

/// Select pseudonymized output when a key is configured, or explicit raw output.
/// Both variants delegate to a size-checked writer.
/// The pseudonymized variant retains key material and per-record scanner state.
/// Keeping it inline avoids an allocation and indirection in this single run-level value.
#[allow(clippy::large_enum_variant)]
pub(crate) enum EgressSink<S: Sink + Sink<PseudonymizedRecord>> {
    Raw(infertap::sink::GuardedSink<S>),
    Pseudonymized(PseudonymizingSink<infertap::sink::GuardedSink<S, Pseudonymized>>),
}

impl<S: Sink + Sink<PseudonymizedRecord>> EgressSink<S> {
    pub(crate) fn into_parts(self) -> (S, u64) {
        match self {
            Self::Raw(s) => {
                let refused = s.refused_oversized();
                (s.into_inner(), refused)
            }
            Self::Pseudonymized(p) => {
                let s = p.into_inner();
                let refused = s.refused_oversized();
                (s.into_inner(), refused)
            }
        }
    }
}

impl<S: Sink + Sink<PseudonymizedRecord>> Sink for EgressSink<S> {
    fn emit(&mut self, record: infertap::record::Record) {
        match self {
            Self::Raw(s) => s.emit(record),
            Self::Pseudonymized(p) => p.emit(record),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Raw(s) => Sink::flush(s),
            Self::Pseudonymized(p) => p.flush(),
        }
    }
    fn write_error(&self) -> Option<std::io::Error> {
        match self {
            Self::Raw(s) => Sink::write_error(s),
            Self::Pseudonymized(p) => p.write_error(),
        }
    }
}

/// Build the egress chain: pseudonymizing under `key`, raw (with the explicit local-only
/// note) when there is none. The key is [`resolve_key`]'s result, resolved once per start.
pub(crate) fn build_egress<S: Sink + Sink<PseudonymizedRecord>>(
    key: Option<Key>,
    key_epoch: Option<u32>,
    output: S,
) -> EgressSink<S> {
    if let Some(key) = key {
        let config = PseudonymConfig {
            key,
            epoch: key_epoch.unwrap_or(DEFAULT_KEY_EPOCH),
        };
        EgressSink::Pseudonymized(PseudonymizingSink::new(
            config,
            BLOCK_ID_CONTEXT,
            infertap::sink::GuardedSink::new(output),
        ))
    } else {
        log::warn!(
            "--raw-ids: records keep raw block ids, which are content fingerprints a key \
             holder can test candidate prompts against, plus cache salts and LoRA names in \
             the clear, at rest on this host. Never ship these records off the host."
        );
        EgressSink::Raw(infertap::sink::GuardedSink::new(output))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use crate::cli::opts::parse;
    use infertap::record::Provenance;

    #[test]
    fn invalid_retention_is_rejected_before_opening_output() {
        let root = infertap::testing::scratch("invalid-retention");
        let dir = root.join("output");
        let options = parse(&[
            "--raw-ids",
            "--out-dir",
            dir.to_str().expect("path"),
            "--segment-bytes",
            "1024",
            "--max-disk-bytes",
            "4095",
        ])
        .expect("parse");
        assert!(super::validate_output(&options, &Provenance::default()).is_err());
        assert!(!dir.exists());
        std::fs::remove_dir(root).expect("cleanup");
    }

    #[test]
    fn key_file_resolution_reads_validates_and_fails_closed() {
        let root = infertap::testing::scratch("cli-key-file");
        let path = root.join("key");
        // a valid 64-byte key, as hex with surrounding whitespace (files end in newlines)
        std::fs::write(
            &path,
            "696e6665727461702d636f6e666f726d616e63652d746573742d6b6579212121696e6665727461702d636f6e666f726d616e63652d6e6f6e6365212121212121\n",
        )
        .expect("write");
        let options = parse(&["--key-file", path.to_str().unwrap()]).expect("parse");
        assert!(super::resolve_key(&options).expect("resolve").is_some());
        // a short key in the file is rejected fail-closed, same as --key
        std::fs::write(&path, "abcd\n").expect("write");
        assert!(super::resolve_key(&options).is_err());
        std::fs::remove_file(&path).ok();
        // a missing file is an error, never silently keyless
        assert!(super::resolve_key(&options).is_err());
        std::fs::remove_dir(root).expect("cleanup");
    }

    /// An absent key is refused unless `--raw-ids` explicitly selects raw output.
    #[test]
    fn keyless_must_be_explicit() {
        // no key, no opt-out: refused before anything connects or opens
        let bare = parse(&[]).expect("parse");
        assert!(super::resolve_key(&bare).is_err());
        // the explicit opt-out is the one keyless path
        let raw = parse(&["--raw-ids"]).expect("parse");
        assert!(super::resolve_key(&raw).expect("resolve").is_none());
        // and it contradicts a configured key rather than silently losing to one
        assert!(parse(&["--raw-ids", "--key", "aa"]).is_err());
        assert!(parse(&["--raw-ids", "--key-file", "/dev/null"]).is_err());
    }

    /// `--out-dir` is a root; the KV stream is written one level down, so streams and
    /// anything else the tap writes never share a directory.
    #[test]
    fn the_kv_stream_is_a_subdirectory_of_the_out_dir() {
        let root = std::env::temp_dir().join(format!("infertap-stream-{}", std::process::id()));
        let args: Vec<String> = ["--raw-ids", "--out-dir", root.to_str().expect("utf8")]
            .iter()
            .map(ToString::to_string)
            .collect();
        let options = parse_owned(&args);
        let sink = super::open_output(&options, &Provenance::default()).expect("opens");
        assert_eq!(
            super::target(options.output.as_ref().expect("configured")),
            root.join(super::KV_STREAM)
        );
        assert!(root.join(super::KV_STREAM).join("active.jsonl").exists());
        drop(sink);
        let _ = std::fs::remove_dir_all(&root);
    }

    fn parse_owned(args: &[String]) -> super::Opts {
        super::Opts::parse_with(args, std::iter::empty(), &|p| {
            Err(std::io::Error::other(format!(
                "no config file in tests: {}",
                p.display()
            )))
        })
        .expect("parse")
    }

    /// Check that configured segment and heartbeat limits reach the emitted header.
    #[test]
    fn the_flags_that_shape_a_segment_reach_its_config() {
        let args: Vec<String> = [
            "--endpoints",
            "i0=tcp://127.0.0.1:5557",
            "--out-dir",
            "/tmp/x",
            "--heartbeat-secs",
            "45",
            "--segment-secs",
            "900",
            "--segment-bytes",
            "4096",
            "--workload-class",
            "agentic",
            "--key",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "--key-epoch",
            "7",
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        let options = super::Opts::parse(&args).expect("parse");
        let cfg = super::segment_config(&options, std::path::Path::new("/tmp/x"));

        assert_eq!(
            cfg.heartbeat_secs, 45,
            "the declared heartbeat bound, not a default"
        );
        assert_eq!(cfg.max_secs, 900, "the declared segment-age bound");
        assert_eq!(cfg.max_bytes, 4096);
        assert_eq!(cfg.workload_class.as_deref(), Some("agentic"));
        assert_eq!(
            cfg.key_epoch,
            Some(7),
            "the operator's declared epoch, not a default"
        );
    }
}
