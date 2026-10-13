// SPDX-License-Identifier: Apache-2.0
//! Lifecycle records and collection diagnostics.

use crate::cli::opts::{label_of, Opts};
use crate::cli::output::EgressSink;
use infertap::clock::ProducerMs;
use infertap::compute::Transducer;
use infertap::lifecycle;
use infertap::record::{Egress, PseudonymizedRecord, ReuseReporting};
use infertap::sink::Sink;
use infertap::source::{EventSource, SourceKey, SourceStats, MAX_PAYLOAD_BYTES};

/// Report replay completeness and any capacity reduction applied at startup.
pub(super) fn report_connection(c: &infertap::source::ConnectReport) {
    if c.replay_incomplete > 0 {
        log::warn!(
            "replay incomplete on {} endpoint(s); proceeding with the history that \
             arrived. The reader's guards declare the rest",
            c.replay_incomplete
        );
    }
    if let Some(c) = c.resident_cap_clamped {
        // Report when the RSS budget reduces the configured identity cap.
        log::warn!(
            "resident cap clamped: {} declared per origin, {} enforced; {} \
             source(s) x {} B/entry must fit a quarter of --max-rss-bytes {}",
            c.declared,
            c.enforced,
            c.sources,
            c.entry_bytes,
            c.max_rss_bytes
        );
    }
}

/// Emit drained refusal windows in source order before the next lifecycle record.
pub(super) fn emit_identity_refusals(
    options: &Opts,
    refusals: impl IntoIterator<Item = infertap::source::IdentityRefusal>,
    sink: &mut impl Sink,
) {
    for refusal in refusals {
        let label = label_of(&options.labels, &refusal.source);
        sink.emit(lifecycle::identity_refused_record(
            &refusal,
            &label,
            ProducerMs::now(),
        ));
    }
}

/// Emit the start record with the active configuration and key identity.
pub(super) fn emit_start<S: Sink>(
    sink: &mut S,
    options: &Opts,
    egress: Egress,
    key_identity: Option<&lifecycle::KeyIdentity>,
    reuse_reporting: ReuseReporting,
) {
    sink.emit(lifecycle::start_record(
        env!("CARGO_PKG_VERSION"),
        &lifecycle::StartConfig {
            endpoints: options.endpoints.len(),
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            reuse_reporting,
            egress,
            key_identity: key_identity.cloned(),
        },
        ProducerMs::now(),
    ));
}

/// Gather the run's envelope stats: the pipeline's counters, the agent's working-set view,
/// and per-endpoint sub-stats keyed by the tap's own subscription targets.
pub(super) fn envelope<S: Sink + Sink<PseudonymizedRecord>>(
    sub: &dyn EventSource,
    transducer: &Transducer,
    sink: &EgressSink<S>,
    options: &Opts,
    last_event: &std::collections::HashMap<SourceKey, ProducerMs>,
) -> lifecycle::EnvelopeStats {
    let endpoints = &options.endpoints;
    let stats = sub.stats();
    lifecycle::EnvelopeStats {
        msgs_seen: stats.messages_seen,
        dropped: stats.dropped,
        oversized: stats.oversized,
        noraw_scanned: match sink {
            EgressSink::Pseudonymized(p) => Some(p.scanned_total()),
            EgressSink::Raw(_) => None,
        },
        unknown_types: stats.unknown_types,
        events_ingested: transducer.events_ingested(),
        content_unresolved: stats.identity.unresolved,
        content_bridge_entries: stats.identity.entries,
        content_bridge_refused: stats.identity.refused,
        publisher_restarts: stats.identity.publisher_restarts,
        rss_bytes: lifecycle::rss_bytes(),
        reuse_reporting: sub.reuse_reporting(),
        endpoints: endpoints
            .iter()
            .map(|e| lifecycle::EndpointStats {
                // Use the record instance_id as the source label. Data-parallel ranks can
                // share this label; their counters remain separate entries.
                source: label_of(&options.labels, &e.source),
                endpoint: e.address.clone(),
                msgs_seen: sub.seen(&e.source),
                dropped: sub.dropped(&e.source),
                last_msg_at_ms: last_event.get(&e.source).copied(),
                topic: sub.topic_for(&e.source).map(str::to_string),
                publisher_restarts: Some(sub.restarts(&e.source)),
            })
            .collect(),
    }
}

/// Closing pipeline counters, logged to stderr with a warning for each nonzero loss counter.
pub(super) struct RunReport {
    /// The source's closing accounting: messages seen, lost, refused, skipped, and the
    /// identity health.
    pub(super) source: SourceStats,
    /// Events the transducer turned into records.
    pub(super) events_ingested: u64,
    /// Wire fields the adapter saw but does not model, `Kind.field`.
    pub(super) unknown_fields: Vec<String>,
    /// Records the guard refused for exceeding the spec's record size bound.
    pub(super) refused_oversized: u64,
    /// Values the no-raw guard scanned, under a key.
    pub(super) noraw_scanned: Option<u64>,
    /// Records the output wrote.
    pub(super) written: u64,
}

pub(super) fn report_run(report: &RunReport, out: &std::path::Path) {
    let source_stats = &report.source;
    let scanned_note = report
        .noraw_scanned
        .map_or_else(String::new, |n| format!(" noraw_scanned={n}"));
    // Report transport and output counters. Consumer analysis uses the record stream.
    log::info!(
        "run complete: events={} msgs={} dropped={} oversized={} unknown_types={}\
         {scanned_note} records={} out={}",
        report.events_ingested,
        source_stats.messages_seen,
        source_stats.dropped,
        source_stats.oversized,
        source_stats.unknown_types,
        report.written,
        out.display(),
    );
    if source_stats.dropped > 0 {
        log::warn!(
            "{} messages dropped (seq gaps); the record stream is INCOMPLETE and \
             downstream analysis undercounts. Raise the engine's kv-events queue or lower load \
             and re-run.",
            source_stats.dropped
        );
    }
    if source_stats.oversized > 0 {
        log::warn!(
            "{} messages rejected as oversized (> payload cap); the record stream is \
             INCOMPLETE. Check the payload cap and publisher batch size before trusting \
             this window.",
            source_stats.oversized
        );
    }
    if source_stats.unknown_types > 0 {
        log::warn!(
            "{} events skipped as unmodelled kinds; this engine emits something this \
             build does not understand. These events are absent from the record stream. \
             Check the engine version against the adapter's pinned capture.",
            source_stats.unknown_types
        );
    }
    if source_stats.undecodable > 0 {
        log::warn!(
            "{} message(s) could not be framed or decoded and were skipped. Output may \
             be incomplete; sequence gaps require later messages to be observed. Check \
             the engine version against the adapter's pinned capture.",
            source_stats.undecodable
        );
    }
    if report.refused_oversized > 0 {
        log::warn!(
            "{} record(s) REFUSED for exceeding the 1 MiB record bound; the stream \
             is incomplete by that many records, and something upstream is emitting \
             pathological values.",
            report.refused_oversized
        );
    }
    if !report.unknown_fields.is_empty() {
        log::warn!(
            "the engine sent {} field(s) this build does not model: {}. Check the engine \
             version against the adapter's pinned capture for added or renamed fields.",
            report.unknown_fields.len(),
            report.unknown_fields.join(", ")
        );
    }
    // Derivations refused because they disagreed with the identity already held for the
    // same block. Local to this line by design: it describes the engine's payloads, not
    // the record stream, so it is not a completeness figure and is not on the contract.
    if source_stats.identity.identity_conflicts > 0 {
        log::warn!(
            "{} identity derivations disagreed with an identity already held for the \
             same block. Further identities for those entries are omitted until release. \
             Check the engine's token and extra-key consistency; affected content \
             comparisons require investigation.",
            source_stats.identity.identity_conflicts
        );
    }
}
