// SPDX-License-Identifier: Apache-2.0
//! Connect sources, run collection, and close output.
//! The loop uses the engine-neutral source interface. Orderly exits attempt to emit
//! a stop record; a crash or output failure can prevent that record from being written.

use crate::cli::error::{CliError, RunFault, WriteStep};
use crate::cli::opts::{help, label_of, load_provenance, Opts, DEFAULT_KEY_EPOCH};
use crate::cli::output::{
    build_egress, configured_output, open_output, resolve_key, target, EgressSink,
};
use infertap::clock::ProducerMs;
use infertap::compute::Transducer;
use infertap::lifecycle;
use infertap::record::{Egress, PseudonymizedRecord, StopReason};
use infertap::sink::segments::{DEFAULT_MAX_BYTES, DEFAULT_MAX_SECS};
use infertap::sink::Sink;
use infertap::source::{ConnectConfig, EventSource, SourceKey, Sourced};
use std::process::ExitCode;

mod memory;
mod reporting;
use memory::{maybe_shed, rss_over_bound, SHED_CHECK_INTERVAL_EVENTS};
use reporting::{
    emit_identity_refusals, emit_start, envelope, report_connection, report_run, RunReport,
};

pub(crate) fn run_live(options: &Opts) -> ExitCode {
    match subscribe(options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e @ CliError::Usage(_)) => {
            eprintln!("infertap: {e}\n\n{}", help());
            ExitCode::from(2)
        }
        Err(e) => {
            log::error!("{e}");
            ExitCode::FAILURE
        }
    }
}

pub(crate) fn subscribe(options: &Opts) -> Result<(), CliError> {
    use std::time::Duration;

    if options.endpoints.is_empty() {
        return Err(CliError::Usage(
            "--endpoints is required for live subscribe".into(),
        ));
    }
    // Everything cheap and local is validated before anything is opened or connected.
    let provenance = load_provenance(options.provenance.as_deref())?;
    crate::cli::output::validate_output(options, &provenance)?;
    let key = resolve_key(options)?;

    // The one engine touchpoint: a name resolved through the adapter registry. Everything
    // after this line is engine-blind.
    //
    // Set up sockets before opening output. ZMQ connects asynchronously; an unreachable
    // publisher can still leave this tap running with an empty event stream.
    let (mut sub, connected) = infertap::source::connect(&ConnectConfig {
        engine: options.engine,
        endpoints: options.endpoints.clone(),
        replay: options.replay_endpoints.clone(),
        poll_timeout: Duration::from_millis(options.poll_timeout_ms),
        resident_blocks: options.resident_blocks,
        max_rss_bytes: options.max_rss_bytes,
    })
    .map_err(CliError::Source)?;
    report_connection(&connected);

    let configured = configured_output(options)?;
    let out = target(configured);
    let output = open_output(options, &provenance)?;
    let mut sink = build_egress(key, options.key_epoch, output);
    let egress = match sink {
        EgressSink::Pseudonymized(_) => Egress::Pseudonymized,
        EgressSink::Raw(_) => Egress::Raw,
    };
    // Derive the canary from the active sink so it identifies the key actually in use.
    let key_identity = match &sink {
        EgressSink::Pseudonymized(p) => Some(lifecycle::KeyIdentity { canary: p.canary() }),
        EgressSink::Raw(_) => None,
    };
    let mut transducer = Transducer::new();

    let mut run = RunState::new(key_identity);
    // Signal handlers set the termination flag. The loop then returns so shutdown
    // can attempt the stop record and close output.
    #[cfg(unix)]
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, std::sync::Arc::clone(&run.term))
            .map_err(CliError::Signal)?;
    }

    emit_start(
        &mut sink,
        options,
        egress,
        run.key.as_ref(),
        sub.reuse_reporting(),
    );

    let stop = run_loop(options, sub.as_mut(), &mut transducer, &mut sink, &mut run);
    // Refusals accumulated since the last heartbeat are declared before the run closes:
    // a loss the stop record's counters imply must also exist scoped, or the last window's
    // degradation would be unattributable.
    emit_identity_refusals(options, sub.drain_identity_refusals(), &mut sink);
    let stats = envelope(sub.as_ref(), &transducer, &sink, options, &run.last_event);
    sink.emit(lifecycle::stop_record(
        stop.reason(),
        &stats,
        run.key.as_ref(),
        ProducerMs::now(),
    ));

    let closing = sub.stats();
    let unknown_fields = sub.unknown_fields();
    // Count values checked by the post-mapping scanner. Scope and exclusions are
    // documented in sink::noraw; an unexpected match aborts emission.
    let noraw_scanned = match &sink {
        EgressSink::Pseudonymized(p) => Some(p.scanned_total()),
        EgressSink::Raw(_) => None,
    };
    let (output, refused_oversized) = sink.into_parts();
    let written = output.written();
    // Seal (or flush) before reporting: on the segment path this is what makes the final
    // records shippable, so a report printed before it would describe a stream still on disk
    // in an unshipped active segment.
    output.finish(configured)?;

    report_run(
        &RunReport {
            source: closing,
            events_ingested: transducer.events_ingested(),
            unknown_fields,
            refused_oversized,
            noraw_scanned,
            written,
        },
        &out,
    );
    match stop {
        Stop::Fault(fault) => Err(CliError::Run(fault)),
        Stop::Signal | Stop::Duration | Stop::MaxBatches => Ok(()),
    }
}

/// Validate configuration without connecting or opening output.
/// Read the key and provenance inputs and check endpoint syntax and output limits.
/// Connectivity, filesystem access for output, and platform transport support are
/// checked during startup. Return 0 on success and 2 for invalid configuration.
pub(crate) fn check_config(options: &Opts) -> ExitCode {
    match config_summary(options) {
        Ok(summary) => {
            println!("{summary}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("infertap: {e}");
            ExitCode::from(2)
        }
    }
}

fn config_summary(options: &Opts) -> Result<String, CliError> {
    if options.endpoints.is_empty() {
        return Err(CliError::Usage(
            "--endpoints or --engine-endpoint is required".into(),
        ));
    }
    let provenance = load_provenance(options.provenance.as_deref())?;
    crate::cli::output::validate_output(options, &provenance)?;
    let key = resolve_key(options)?;
    let output = target(configured_output(options)?);
    let egress = match key {
        Some(_) => format!(
            "pseudonymized (epoch {})",
            options.key_epoch.unwrap_or(DEFAULT_KEY_EPOCH)
        ),
        None => "raw (--raw-ids)".to_string(),
    };
    let mut sources: Vec<&str> = options
        .endpoints
        .iter()
        .map(|e| e.source.as_str())
        .collect();
    sources.sort_unstable();
    Ok(format!(
        "config ok: engine {} with {} source(s) [{}]; output {}; egress {egress}; \
         heartbeat {}s; segments {}B/{}s; provenance {} key(s)",
        options.engine,
        options.endpoints.len(),
        sources.join(", "),
        output.display(),
        options.heartbeat_secs,
        options.segment_bytes.unwrap_or(DEFAULT_MAX_BYTES),
        options.segment_secs.unwrap_or(DEFAULT_MAX_SECS),
        provenance.len()
    ))
}

/// What one run carries besides its inputs: the termination flag the signal handlers set,
/// the key identity every lifecycle record stamps (the key in force, derived from the sink),
/// and the producer clock of the last message per source, for the heartbeat's endpoints.
pub(crate) struct RunState {
    /// Set by SIGTERM/SIGINT; the loop reads it on every wake.
    pub(crate) term: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The canary a consumer compares across nodes; `None` on a raw run.
    pub(crate) key: Option<lifecycle::KeyIdentity>,
    /// When the producer last heard each source.
    pub(crate) last_event: std::collections::HashMap<SourceKey, ProducerMs>,
}

impl RunState {
    pub(crate) fn new(key: Option<lifecycle::KeyIdentity>) -> Self {
        Self {
            term: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            key,
            last_event: std::collections::HashMap::new(),
        }
    }
}

/// How the loop ended. Every arm labels the stop record; a fault also fails the run.
#[derive(Debug)]
pub(crate) enum Stop {
    /// SIGTERM or SIGINT.
    Signal,
    /// `--duration` elapsed.
    Duration,
    /// `--max-batches` was reached.
    MaxBatches,
    /// The loop stopped on a fault it must report.
    Fault(RunFault),
}

impl Stop {
    /// The stop record's reason.
    pub(crate) const fn reason(&self) -> StopReason {
        match self {
            Self::Signal => StopReason::Signal,
            Self::Duration => StopReason::Duration,
            Self::MaxBatches => StopReason::MaxBatches,
            Self::Fault(fault) => fault.reason(),
        }
    }
}

/// Poll sources, drain events, and emit heartbeats until a stop condition.
/// Return faults to the caller for shutdown and reporting.
pub(crate) fn run_loop<S: Sink + Sink<PseudonymizedRecord>>(
    options: &Opts,
    sub: &mut dyn EventSource,
    transducer: &mut Transducer,
    sink: &mut EgressSink<S>,
    run: &mut RunState,
) -> Stop {
    use std::time::{Duration, Instant};
    let started = Instant::now();
    let hb_deadline = Duration::from_millis(heartbeat_deadline_ms(
        options.heartbeat_secs,
        options.poll_timeout_ms,
    ));
    let mut last_hb = Instant::now();
    // The shed's rearm mark: RSS at the last sweep. Zero = never swept, so the first
    // trigger is the threshold alone.
    let mut last_shed_rss: u64 = 0;
    // Messages the adapter refused (a frame or payload it could not read). Skipped, never
    // fatal. The closing report counts them; later sequence numbers can expose gaps.
    let mut skipped: u64 = 0;
    loop {
        if run.term.load(std::sync::atomic::Ordering::Relaxed) {
            return Stop::Signal;
        }
        if options.duration.is_some_and(|d| started.elapsed() >= d) {
            return Stop::Duration;
        }
        if options
            .max_batches
            .is_some_and(|m| sub.stats().messages_seen >= m)
        {
            return Stop::MaxBatches;
        }
        // Check RSS at every wake. Socket high-water marks limit message counts,
        // not bytes, so large messages can still exceed the memory budget.
        // Stop collection on overflow. The systemd MemoryMax adds an OS-level limit.
        if let Some(rss) = rss_over_bound(options.max_rss_bytes, lifecycle::rss_bytes()) {
            return Stop::Fault(RunFault::MemoryBound {
                rss,
                bound: options.max_rss_bytes,
            });
        }
        // Drain queued messages without decoding when RSS crosses the shed threshold.
        // Subsequent sequence gaps expose discarded messages. The RSS guard remains
        // an upper limit if shedding cannot contain growth.
        //
        // Rearm on RSS growth since the previous sweep. Waiting for RSS to fall
        // would cause continuous discarding when the allocator retains freed pages.
        // A disabled memory guard also disables shedding.
        maybe_shed(options.max_rss_bytes, &mut *sub, &mut last_shed_rss);
        // Stop on a latched write error. Sinks reject subsequent writes to avoid
        // appending to a damaged stream, so continuing would discard incoming records.
        if let Some(source) = sink.write_error() {
            return Stop::Fault(RunFault::Write {
                during: WriteStep::Emit,
                source,
            });
        }
        match sub.poll() {
            Ok(_) => {
                // The shed check also runs inside the drain: one shared-prefix message
                // carries tens of thousands of events, and a check only between
                // messages lets the queues grow for a whole message's processing time.
                let mut drained: u32 = 0;
                // One clock read per drain, and one map touch per change of source: a
                // message carries thousands of events from one source, and a clone plus
                // a clock read per event was the run loop's own hot-path cost.
                let now = ProducerMs::now();
                let mut stamped: Option<(SourceKey, infertap::record::InstanceLabel)> = None;
                while let Some(Sourced { source, event, seq }) = sub.next_event() {
                    if stamped.as_ref().is_none_or(|(s, _)| *s != source) {
                        run.last_event.insert(source.clone(), now);
                        // The record carries the instance label: every rank of one engine
                        // is one instance, and the per-rank source key is transport
                        // bookkeeping.
                        let label = label_of(&options.labels, &source);
                        stamped = Some((source, label));
                    }
                    let Some((_, label)) = &stamped else {
                        unreachable!("stamped on this or an earlier event");
                    };
                    transducer.ingest(label, &event, Some(seq), sink);
                    drained += 1;
                    if drained % SHED_CHECK_INTERVAL_EVENTS == 0 {
                        maybe_shed(options.max_rss_bytes, &mut *sub, &mut last_shed_rss);
                    }
                }
            }
            // SIGTERM can interrupt the blocking poll with EINTR. Check the signal
            // flag before interpreting the result as a transport fault.
            Err(_) if run.term.load(std::sync::atomic::Ordering::Relaxed) => {
                return Stop::Signal;
            }
            // Skip malformed messages and count them. Log at doubling intervals to
            // bound diagnostic volume; later sequence gaps can expose the rejected data.
            Err(e) if !e.is_fatal() => {
                skipped += 1;
                if skipped.is_power_of_two() {
                    log::warn!(
                        "{e} ({skipped} message(s) skipped so far; output may be incomplete)"
                    );
                }
            }
            Err(e) => return Stop::Fault(RunFault::Stream(e)),
        }
        if options.heartbeat_secs > 0 && last_hb.elapsed() >= hb_deadline {
            // Emit one refusal record per affected holder at heartbeat cadence.
            emit_identity_refusals(options, sub.drain_identity_refusals(), sink);
            let stats = envelope(&*sub, transducer, sink, options, &run.last_event);
            sink.emit(lifecycle::heartbeat_record(
                &stats,
                run.key.as_ref(),
                ProducerMs::now(),
            ));
            // Push buffered records toward the disk on the heartbeat cadence. Detection of
            // a dead output is otherwise bounded by when a buffer happens to fill, which on
            // a quiet stream can take many heartbeats; this makes it one interval. The
            // flush error is the same latched error `write_error` reads.
            if let Err(source) = sink.flush() {
                return Stop::Fault(RunFault::Write {
                    during: WriteStep::Flush,
                    source,
                });
            }
            last_hb = Instant::now();
        }
    }
}

/// Schedule heartbeats early by one poll interval to allow for idle-poll delay.
/// This accounts for configured poll granularity; scheduling and processing delays can
/// still affect observed emission time.
pub(crate) const fn heartbeat_deadline_ms(heartbeat_secs: u64, poll_timeout_ms: u64) -> u64 {
    heartbeat_secs
        .saturating_mul(1000)
        .saturating_sub(poll_timeout_ms)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Schedule heartbeats one poll interval before the declared deadline to allow
    /// for poll latency. Intervals at or below the poll timeout emit on each wake.
    #[test]
    fn the_heartbeat_deadline_absorbs_the_wake_jitter() {
        assert_eq!(heartbeat_deadline_ms(60, 250), 59_750);
        assert_eq!(
            heartbeat_deadline_ms(60, 250) + 250,
            60_000,
            "worst-case wake lands exactly on the declared bound, never past it"
        );
        assert_eq!(heartbeat_deadline_ms(1, 5_000), 0);
        // an absurd bound saturates to effectively-never rather than wrapping to a
        // nonsense cadence (release builds wrap silently; this must not depend on profile)
        assert_eq!(heartbeat_deadline_ms(u64::MAX, 250), u64::MAX - 250);
    }
}

// The run loop off the live path: scripted sources and dead outputs.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod run_loop_tests {
    use std::sync::atomic::Ordering;

    use crate::cli::error::RunFault;
    use infertap::clock::EngineMs;
    use infertap::compute::Transducer;
    use infertap::event_model::{Event, Origin, Parent, Stored};
    use infertap::record::StopReason;
    use infertap::sink::JsonlSink;
    use infertap::source::{SourceKey, Sourced};
    use infertap::testing::{ScriptedSource, Step};

    #[test]
    fn refusal_emission_preserves_labels_scopes_and_windows() {
        let options = crate::cli::opts::parse(&[
            "--engine-endpoint",
            "engine=tcp://host:5557",
            "--dp-local",
            "2",
        ])
        .expect("parse");
        let refusals = ["engine#1", "manual"].map(|source| infertap::source::IdentityRefusal {
            source: source.into(),
            dp_rank: 1,
            group_idx: 2,
            window_start_ms: EngineMs(10.0),
            window_end_ms: EngineMs(20.0),
            refused: 3,
        });
        let mut sink = infertap::sink::MemorySink::new(None);
        super::emit_identity_refusals(&options, refusals, &mut sink);
        let rows = &sink.records;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["instance_id"], "engine");
        assert_eq!(rows[1]["instance_id"], "manual");
        for row in rows {
            assert_eq!(row["dp_rank"], 1);
            assert_eq!(row["group_idx"], 2);
            assert_eq!(row["refused"], 3);
            assert_eq!(row["window_start_ms"], 10.0);
            assert_eq!(row["window_end_ms"], 20.0);
        }
        super::emit_identity_refusals(&options, [], &mut sink);
        assert_eq!(sink.records.len(), 2);
    }

    /// One store event per poll, forever: the stream never ends on its own, so whatever
    /// stops the loop is the thing under test.
    fn one_store_per_poll() -> ScriptedSource {
        let store = Event::Stored(Stored {
            origin: Origin::default(),
            block_hash: 12_955_354_968_414_679_641,
            parent: Parent::Root,
            block_size: 16,
            tier: Some("GPU".into()),
            extra_keys: None,
            lora_id: None,
            lora_name: None,
            spec_kind: None,
            reused: None,
            spec_sliding_window: None,
            locality: None,
            backend_id: None,
            content_id: None,
            at_ms: EngineMs(1_785_153_670_000.0),
        });
        ScriptedSource::new(vec![Step::Events(vec![Sourced {
            source: SourceKey::from("i0"),
            event: store,
            seq: 1,
        }])])
        .repeating_last()
    }

    /// A disk that is full from the first byte.
    struct DeadDisk;
    impl std::io::Write for DeadDisk {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk full"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A disk that takes every byte and fails on flush: the fault a buffered writer meets
    /// only when its buffer reaches the device.
    struct DeadOnFlush;
    impl std::io::Write for DeadOnFlush {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("device gone"))
        }
    }

    /// The heartbeat's flush is where a buffered output's fault surfaces on a quiet
    /// stream: the write succeeded into the buffer, and only the flush meets the device.
    /// The loop stops with the flush step named and the I/O error carried.
    #[test]
    fn a_heartbeat_flush_error_stops_the_run_and_names_the_flush() {
        let args: Vec<String> = [
            "--max-batches",
            "1000",
            "--raw-ids",
            "--heartbeat-secs",
            "1",
            "--poll-timeout-ms",
            "1000",
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        let options = super::Opts::parse(&args).expect("parse");
        let mut sub = one_store_per_poll();
        let mut transducer = Transducer::new();
        let mut sink = super::EgressSink::Raw(infertap::sink::GuardedSink::new(JsonlSink::new(
            DeadOnFlush,
        )));
        let stop = super::run_loop(
            &options,
            &mut sub,
            &mut transducer,
            &mut sink,
            &mut super::RunState::new(None),
        );
        let super::Stop::Fault(RunFault::Write { during, source }) = stop else {
            panic!("the flush fault must stop the run: {stop:?}");
        };
        assert_eq!(during, super::WriteStep::Flush);
        assert_eq!(
            source.get_ref().map(ToString::to_string).as_deref(),
            Some("device gone")
        );
        assert!(
            sub.polls < 1000,
            "the loop kept polling: {} rounds",
            sub.polls
        );
    }

    /// A message the adapter cannot read is skipped, not fatal: the loop keeps polling
    /// past a skippable error and stops only on a fatal one.
    #[test]
    fn a_skippable_source_error_does_not_end_the_run() {
        let args: Vec<String> = ["--raw-ids"].iter().map(ToString::to_string).collect();
        let options = super::Opts::parse(&args).expect("parse");
        let mut transducer = Transducer::new();
        let mut sink =
            super::EgressSink::Raw(infertap::sink::GuardedSink::new(JsonlSink::new(Vec::new())));
        let mut run = super::RunState::new(None);
        let bad = || Step::Skippable("not msgpack".to_string());
        let mut source = ScriptedSource::new(vec![
            bad(),
            bad(),
            bad(),
            Step::Fatal(std::io::ErrorKind::Other, "socket gone".to_string()),
        ]);
        let stop = super::run_loop(&options, &mut source, &mut transducer, &mut sink, &mut run);
        assert_eq!(
            source.polls, 4,
            "three skippable errors were survived; the fatal fourth stopped the run"
        );
        assert_eq!(stop.reason(), StopReason::StreamError);
        let super::Stop::Fault(fault) = stop else {
            panic!("a stream fault must be reported: {stop:?}");
        };
        assert!(fault.to_string().contains("socket gone"), "{fault}");
    }

    /// `check-config` validates exactly what a live start validates and connects to
    /// nothing: a complete configuration summarizes, an incomplete one fails as usage.
    #[test]
    fn check_config_summarizes_a_valid_configuration_and_refuses_an_incomplete_one() {
        let ok = crate::cli::opts::parse(&[
            "--engine-endpoint",
            "eng=tcp://127.0.0.1:5557",
            "--raw-ids",
            "--out-dir",
            "/var/lib/infertap",
        ])
        .expect("parse");
        let summary = super::config_summary(&ok).expect("valid");
        assert!(
            summary.starts_with("config ok: engine vllm with 1 source(s) [eng]"),
            "{summary}"
        );
        assert!(summary.contains("/var/lib/infertap/kv"), "{summary}");
        assert!(summary.contains("raw (--raw-ids)"), "{summary}");
        let no_output = crate::cli::opts::parse(&[
            "--engine-endpoint",
            "eng=tcp://127.0.0.1:5557",
            "--raw-ids",
        ])
        .expect("parse");
        let err = super::config_summary(&no_output).expect_err("refused");
        assert!(matches!(err, super::CliError::Usage(_)), "{err}");
        let no_endpoint =
            crate::cli::opts::parse(&["--raw-ids", "--out-dir", "/x"]).expect("parse");
        assert!(super::config_summary(&no_endpoint).is_err());
    }

    /// A termination signal outranks an interrupted poll error (EINTR).
    #[test]
    fn a_poll_error_with_the_term_flag_raised_is_a_signal_stop_not_a_stream_error() {
        let interrupted = || {
            ScriptedSource::new(vec![Step::Fatal(
                std::io::ErrorKind::Interrupted,
                "poll interrupted".to_string(),
            )])
            .repeating_last()
        };
        let args: Vec<String> = ["--raw-ids"].iter().map(ToString::to_string).collect();
        let options = super::Opts::parse(&args).expect("parse");
        let mut transducer = Transducer::new();
        let mut sink =
            super::EgressSink::Raw(infertap::sink::GuardedSink::new(JsonlSink::new(Vec::new())));
        // Flag raised, as the handler leaves it when SIGTERM interrupts the poll: the
        // reason must be the signal, and the run must not carry a stream error.
        let mut run = super::RunState::new(None);
        run.term.store(true, Ordering::Relaxed);
        let stop = super::run_loop(
            &options,
            &mut interrupted(),
            &mut transducer,
            &mut sink,
            &mut run,
        );
        assert!(
            matches!(stop, super::Stop::Signal),
            "a clean stop must not exit as a failure: {stop:?}"
        );
        // Without the flag, the same poll error stays what it is: a stream fault.
        let mut run = super::RunState::new(None);
        let stop = super::run_loop(
            &options,
            &mut interrupted(),
            &mut transducer,
            &mut sink,
            &mut run,
        );
        assert!(
            matches!(stop, super::Stop::Fault(RunFault::Stream(_))),
            "{stop:?}"
        );
    }

    #[test]
    fn an_extreme_duration_does_not_overflow_the_clock() {
        let mut options =
            crate::cli::opts::parse(&["--raw-ids", "--max-batches", "0"]).expect("parse");
        options.duration = Some(std::time::Duration::MAX);
        let mut sink =
            super::EgressSink::Raw(infertap::sink::GuardedSink::new(JsonlSink::new(Vec::new())));
        let stop = super::run_loop(
            &options,
            &mut one_store_per_poll(),
            &mut Transducer::new(),
            &mut sink,
            &mut super::RunState::new(None),
        );
        assert!(matches!(stop, super::Stop::MaxBatches));
    }

    #[test]
    fn a_dead_output_stops_the_run_instead_of_polling_and_discarding() {
        // max_batches bounds the loop so a detection regression fails this test rather
        // than hanging it
        let args: Vec<String> = ["--max-batches", "1000", "--raw-ids"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let options = super::Opts::parse(&args).expect("parse");
        let mut sub = one_store_per_poll();
        let mut transducer = Transducer::new();
        let mut sink =
            super::EgressSink::Raw(infertap::sink::GuardedSink::new(JsonlSink::new(DeadDisk)));
        let stop = super::run_loop(
            &options,
            &mut sub,
            &mut transducer,
            &mut sink,
            &mut super::RunState::new(None),
        );
        let super::Stop::Fault(fault @ RunFault::Write { .. }) = stop else {
            panic!("the stop must carry the underlying I/O error: {stop:?}");
        };
        assert!(
            fault.to_string().contains("disk full"),
            "unhelpful error: {fault}"
        );
        assert!(
            sub.polls < 1000,
            "the loop kept polling a dead output for {} rounds",
            sub.polls
        );
    }
}
