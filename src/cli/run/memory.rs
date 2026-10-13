// SPDX-License-Identifier: Apache-2.0
//! RSS guard and receive-queue shedding policy.

use infertap::lifecycle;
use infertap::source::EventSource;

/// Events processed between RSS checks within a decoded message.
pub(super) const SHED_CHECK_INTERVAL_EVENTS: u32 = 4096;

/// Start shedding at three quarters of the RSS limit, leaving headroom for
/// conversion buffers and incoming messages.
pub(super) const fn shed_start(max_rss: u64) -> u64 {
    max_rss - max_rss / 4
}

/// RSS growth required to rearm shedding below the high-water threshold.
pub(super) const SHED_REARM_BYTES: u64 = 32 * 1024 * 1024;

/// Above seven eighths of the RSS limit, shed on every check regardless of
/// rearm growth. This prevents the rearm band from delaying urgent shedding.
pub(super) const fn shed_high_water(max_rss: u64) -> u64 {
    max_rss - max_rss / 8
}

/// Whether a sweep is due at `rss`: past the threshold, and either grown by the rearm
/// band since the last sweep or above the high water where the rearm no longer gates. A
/// bound of 0 is the opt-out, and an unreadable RSS decides nothing.
pub(super) fn shed_due(max_rss: u64, rss: Option<u64>, last_shed_rss: u64) -> bool {
    if max_rss == 0 {
        return false;
    }
    let Some(rss) = rss else {
        return false;
    };
    rss >= shed_start(max_rss)
        && (rss >= last_shed_rss.saturating_add(SHED_REARM_BYTES)
            || rss >= shed_high_water(max_rss))
}

/// One shed, callable from every place the loop can afford a check, when [`shed_due`]
/// says so.
pub(super) fn maybe_shed(max_rss: u64, sub: &mut dyn EventSource, last_shed_rss: &mut u64) {
    let rss = lifecycle::rss_bytes();
    if !shed_due(max_rss, rss, *last_shed_rss) {
        return;
    }
    let Some(rss) = rss else {
        return;
    };
    let shed = sub.shed_queue();
    *last_shed_rss = rss;
    if shed.messages > 0 {
        log::warn!(
            "memory shed at rss {rss}: {} message(s) / {} bytes discarded undecoded; the \
             loss is declared by the sequence gaps the next processed messages reveal",
            shed.messages,
            shed.bytes
        );
    }
}

pub(super) const fn rss_over_bound(bound: u64, rss: Option<u64>) -> Option<u64> {
    match rss {
        Some(r) if bound != 0 && r > bound => Some(r),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    /// The memory bound's decision rule: 0 disables, an unreadable RSS is not a failure,
    /// and only an rss strictly past a nonzero bound trips it.
    #[test]
    fn the_memory_bound_trips_only_past_a_nonzero_bound_with_a_readable_rss() {
        use super::rss_over_bound;
        assert_eq!(rss_over_bound(0, Some(u64::MAX)), None); // disabled
        assert_eq!(rss_over_bound(100, None), None); // no RSS surface: MemoryMax's job
        assert_eq!(rss_over_bound(100, Some(100)), None); // at the bound is within it
        assert_eq!(rss_over_bound(100, Some(101)), Some(101)); // past it: fail closed
    }
    /// The shed's decision rule as a function of three numbers: the bound, the resident
    /// set, and the RSS at the last sweep. Below the threshold nothing sweeps; past it,
    /// a sweep repeats only after the rearm band of growth, except above the high water,
    /// where every check sweeps.
    #[test]
    fn the_shed_decides_on_the_threshold_the_rearm_band_and_the_high_water() {
        use super::{shed_due, shed_high_water, shed_start, SHED_REARM_BYTES};
        let bound = 1 << 30;
        assert!(
            !shed_due(0, Some(u64::MAX), 0),
            "a bound of 0 is the opt-out"
        );
        assert!(
            !shed_due(bound, None, 0),
            "an unreadable RSS decides nothing"
        );
        assert!(!shed_due(bound, Some(shed_start(bound) - 1), 0));
        assert!(
            shed_due(bound, Some(shed_start(bound)), 0),
            "the first sweep"
        );
        let swept_at = shed_start(bound);
        assert!(
            !shed_due(bound, Some(swept_at + SHED_REARM_BYTES - 1), swept_at),
            "inside the rearm band: retained pages do not re-trigger"
        );
        assert!(
            shed_due(bound, Some(swept_at + SHED_REARM_BYTES), swept_at),
            "grown by the band: re-armed"
        );
        assert!(
            shed_due(bound, Some(shed_high_water(bound)), shed_high_water(bound)),
            "at the high water the rearm no longer gates"
        );
    }
}
