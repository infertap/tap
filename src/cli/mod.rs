// SPDX-License-Identifier: Apache-2.0
//! The binary's own modules, one concern each: the command line, where the output goes,
//! the live run, the built-in scenario, and the one error type they share.

pub(crate) mod demo;
pub(crate) mod error;
pub(crate) mod logging;
pub(crate) mod opts;
pub(crate) mod output;
pub(crate) mod probe;
pub(crate) mod run;

/// `infertap <version> (spec <tag>)`: the crate version and the contract pin, the two
/// numbers a bug report needs. The pin is read from `SPEC_PIN` at build time.
pub(crate) fn version_line() -> String {
    let pin = include_str!("../../SPEC_PIN");
    let tag = pin
        .lines()
        .find_map(|l| l.strip_prefix("tag="))
        .unwrap_or("unknown");
    format!("infertap {} (spec {tag})", env!("CARGO_PKG_VERSION"))
}
