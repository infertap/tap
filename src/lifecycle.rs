// SPDX-License-Identifier: Apache-2.0
//! Process lifecycle and collection counters.
//!
//! Start, heartbeat, and stop records use producer-clock timestamps and share the
//! cache records' output pipeline. They describe the tap process rather than a single
//! engine instance. Orderly exits attempt to emit a stop record; crashes and output
//! failures may prevent it.
//!
//! Heartbeats include source counters, identity-state usage, and available process
//! metrics. Optional values are omitted when unavailable.

/// Default interval between lifecycle heartbeats, in seconds.
pub const DEFAULT_HEARTBEAT_SECS: u64 = 60;

use crate::clock::ProducerMs;
use crate::record::{
    AgentStart, AgentStop, Egress, Heartbeat, IdentityRefused, InstanceLabel, Record,
    ReuseReporting, StopReason,
};

/// Key-space identifier for comparing pseudonymized records across producers.
///
/// Start and heartbeat records repeat the current canary so comparisons do not require
/// observing startup. Rotation emits a new canary without linking old and new spaces.
#[derive(Debug, Clone)]
pub struct KeyIdentity {
    /// The canary value: the key file's nonce MAC'd under the key and epoch, hex.
    pub canary: String,
}

/// Config summary stamped on `agent_start`.
#[derive(Debug)]
pub struct StartConfig {
    /// Configured subscription endpoints, counted.
    pub endpoints: usize,
    /// The payload bound in force.
    pub max_payload_bytes: usize,
    /// Whether egress pseudonymizes, or runs explicitly raw.
    pub egress: Egress,
    /// Absent when running unkeyed, where there is no key to disagree about.
    pub key_identity: Option<KeyIdentity>,
    /// See [`EnvelopeStats::reuse_reporting`]. Declared at boot as well as on every
    /// heartbeat and the stop record.
    pub reuse_reporting: ReuseReporting,
}

/// Per-subscription counters and last-observed-message time.
///
/// Subscriptions are keyed by configured endpoint. An idle SUB socket cannot distinguish
/// an unreachable publisher from one with no traffic, so no connection status is inferred.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct EndpointStats {
    /// The instance label this entry's records carry.
    pub source: InstanceLabel,
    /// The address the tap dialed.
    pub endpoint: String,
    /// Messages observed on this subscription.
    pub msgs_seen: u64,
    /// Messages known lost on this subscription.
    pub dropped: u64,
    /// When the producer last heard this source; absent until it has.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_msg_at_ms: Option<ProducerMs>,
    /// The topic this source published under, verbatim. Absent until a message has been
    /// seen, and absent where the frame was not valid UTF-8. The producer never
    /// synthesizes one; recognizing a naming convention is the reader's job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Sequence regressions observed on this subscription.
    /// This producer emits a measured count, including zero. `None` preserves the
    /// absence of a measurement when deserializing older records.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publisher_restarts: Option<u64>,
}

/// The run's envelope stats, carried in-band on heartbeats and the stop record. The
/// counters are cumulative: a consumer takes deltas between heartbeats, and a span whose
/// deltas show no drops or rejections is a span whose records are complete.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct EnvelopeStats {
    /// Messages observed across all sources.
    pub msgs_seen: u64,
    /// Messages known lost across all sources.
    pub dropped: u64,
    /// Messages refused as oversized.
    pub oversized: u64,
    /// Values the no-raw guard scanned, under a key; absent when running raw.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noraw_scanned: Option<u64>,
    /// Events skipped because the adapter does not model their kind: an engine-drift signal,
    /// and the count by which this span's stream is incomplete.
    pub unknown_types: u64,
    /// Events the transducer ingested.
    pub events_ingested: u64,
    /// Blocks that reached egress with no content identity. A completeness figure like
    /// `dropped` and `unknown_types`, not a fault. Nonzero after a tap restart against a
    /// warm cache, and expected to return to zero as the engine's cache turns over.
    pub content_unresolved: u64,
    /// Live entries in the adapter's identity state.
    pub content_bridge_entries: u64,
    /// Identity promises the bridge refused at its bounds. A rising count means the bound
    /// sits below the engine's block count. The wire keeps the field's original name,
    /// `content_bridge_evicted`; in code the count is a refusal everywhere.
    #[serde(rename = "content_bridge_evicted")]
    pub content_bridge_refused: u64,
    /// Publisher sequence regressions observed. Each regression resets adapter identity state.
    pub publisher_restarts: u64,
    /// Resident set, where the platform exposes it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    /// What a `store` means on this stream (contract section 2.3). Constant for a run,
    /// and repeated on all three lifecycle kinds for the same reason as the canary: an
    /// analysis window need not contain a start record.
    pub reuse_reporting: ReuseReporting,
    /// Per-subscription entries, keyed by the tap's own targets.
    pub endpoints: Vec<EndpointStats>,
}

/// The boot record.
#[must_use]
pub fn start_record(agent_version: &str, cfg: &StartConfig, at_ms: ProducerMs) -> Record {
    Record::AgentStart(AgentStart {
        at_ms,
        agent_version: agent_version.to_string(),
        // A count rather than the array: `endpoints` names the per-endpoint array on
        // heartbeat and stop records, and a field name must not change type across kinds.
        endpoint_count: cfg.endpoints as u64,
        max_payload_bytes: cfg.max_payload_bytes as u64,
        egress: cfg.egress,
        reuse_reporting: cfg.reuse_reporting,
        canary: cfg.key_identity.as_ref().map(|k| k.canary.clone()),
    })
}

/// The completeness clock: cumulative envelope stats at `at_ms`.
#[must_use]
pub fn heartbeat_record(s: &EnvelopeStats, key: Option<&KeyIdentity>, at_ms: ProducerMs) -> Record {
    Record::Heartbeat(Heartbeat {
        at_ms,
        stats: s.clone(),
        canary: key.map(|k| k.canary.clone()),
    })
}

/// The departure label: why the run ended, with closing stats.
#[must_use]
pub fn stop_record(
    reason: StopReason,
    s: &EnvelopeStats,
    key: Option<&KeyIdentity>,
    at_ms: ProducerMs,
) -> Record {
    Record::AgentStop(AgentStop {
        at_ms,
        reason,
        stats: s.clone(),
        canary: key.map(|k| k.canary.clone()),
    })
}

/// Build an identity-refusal record for one origin and engine-clock window.
/// Refusals can result from entry capacity, scope limits, or saturated residency counts.
/// The record identifies the affected instance, rank, group, and observed store window.
#[must_use]
pub fn identity_refused_record(
    r: &crate::source::IdentityRefusal,
    instance: &InstanceLabel,
    at_ms: ProducerMs,
) -> Record {
    Record::IdentityRefused(IdentityRefused {
        at_ms,
        instance_id: instance.clone(),
        dp_rank: r.dp_rank,
        group_idx: r.group_idx,
        refused: r.refused,
        window_start_ms: r.window_start_ms,
        window_end_ms: r.window_end_ms,
    })
}

/// Read process RSS from Linux `VmRSS`, whose units are KiB regardless of page size.
/// Return `None` when the platform or status file does not provide a usable value.
#[must_use]
pub fn rss_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_vm_rss(&text)
}

/// The `VmRSS:` line of a `/proc/<pid>/status` file, in bytes.
fn parse_vm_rss(status: &str) -> Option<u64> {
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let kb: u64 = fields.next()?.parse().ok()?;
    match fields.next() {
        Some("kB") => kb.checked_mul(1024),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::parse_vm_rss;

    #[test]
    fn vm_rss_is_read_in_kb_independent_of_page_size() {
        let status = "Name:\tinfertap\nVmPeak:\t  812345 kB\nVmRSS:\t   65536 kB\nThreads:\t2\n";
        assert_eq!(parse_vm_rss(status), Some(65536 * 1024));
    }

    #[test]
    fn a_status_without_vm_rss_or_with_a_strange_unit_yields_nothing() {
        assert_eq!(parse_vm_rss("Name:\tx\nThreads:\t1\n"), None);
        assert_eq!(parse_vm_rss("VmRSS:\t 12 MB\n"), None);
        assert_eq!(parse_vm_rss("VmRSS:\t abc kB\n"), None);
    }
}
