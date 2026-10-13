// SPDX-License-Identifier: Apache-2.0
//! Option spelling, arity, and permitted configuration layers.

use crate::cli::error::CliError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    Flag,
    Scalar,
    List,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OptionId {
    Endpoints,
    EngineEndpoint,
    EngineReplayEndpoint,
    DpStartRank,
    DpLocal,
    DpMax,
    Engine,
    KeyFile,
    RawIds,
    KeyEpoch,
    Provenance,
    Out,
    OutDir,
    SegmentBytes,
    SegmentSecs,
    WorkloadClass,
    MaxDiskBytes,
    Duration,
    MaxBatches,
    HeartbeatSecs,
    PollTimeoutMs,
    ResidentBlocks,
    MaxRssBytes,
    LogLevel,
    LogFormat,
    Help,
    Version,
    Key,
    Config,
}

impl OptionId {
    pub(super) const fn short(self) -> Option<&'static str> {
        match self {
            Self::Help => Some("-h"),
            Self::Version => Some("-V"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OptionSpec {
    pub id: OptionId,
    pub name: &'static str,
    pub shape: Shape,
    pub layered: bool,
    pub value_name: &'static str,
    pub help: &'static str,
}

pub(super) const OPTIONS: &[OptionSpec] = &[
    OptionSpec {
        id: OptionId::Endpoints,
        name: "endpoints",
        shape: Shape::List,
        layered: true,
        value_name: "MAP",
        help: "Comma-separated source=endpoint subscriptions.",
    },
    OptionSpec {
        id: OptionId::EngineEndpoint,
        name: "engine-endpoint",
        shape: Shape::List,
        layered: true,
        value_name: "ENDPOINT",
        help: "Engine label=tcp://host:port; repeat or comma-separate. Expand ports by data-parallel rank.",
    },
    OptionSpec {
        id: OptionId::EngineReplayEndpoint,
        name: "engine-replay-endpoint",
        shape: Shape::List,
        layered: true,
        value_name: "ENDPOINT",
        help: "Replay base endpoint for a configured engine label. Replay is bounded and best effort.",
    },
    OptionSpec {
        id: OptionId::DpStartRank,
        name: "dp-start-rank",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "First data-parallel rank.",
    },
    OptionSpec {
        id: OptionId::DpLocal,
        name: "dp-local",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Number of local data-parallel ranks.",
    },
    OptionSpec {
        id: OptionId::DpMax,
        name: "dp-max",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Rank capacity; defaults to --dp-local.",
    },
    OptionSpec {
        id: OptionId::Engine,
        name: "engine",
        shape: Shape::Scalar,
        layered: true,
        value_name: "NAME",
        help: "Engine adapter: vllm (the default) or sglang.",
    },
    OptionSpec {
        id: OptionId::KeyFile,
        name: "key-file",
        shape: Shape::Scalar,
        layered: true,
        value_name: "PATH",
        help: "Read a 64-byte hex/base64 key from a file. Preferred over --key.",
    },
    OptionSpec {
        id: OptionId::RawIds,
        name: "raw-ids",
        shape: Shape::Flag,
        layered: true,
        value_name: "",
        help: "Bypass pseudonymization for local inspection. Do not ship raw-mode output.",
    },
    OptionSpec {
        id: OptionId::KeyEpoch,
        name: "key-epoch",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Pseudonymization epoch; requires --key or --key-file.",
    },
    OptionSpec {
        id: OptionId::Provenance,
        name: "provenance",
        shape: Shape::Scalar,
        layered: true,
        value_name: "PATH|JSON",
        help: "Segment metadata as a JSON object or a path to one. Keep prompts and secrets out.",
    },
    OptionSpec {
        id: OptionId::Out,
        name: "out",
        shape: Shape::Scalar,
        layered: true,
        value_name: "PATH",
        help: "Create a single JSONL output file; refuse an existing file.",
    },
    OptionSpec {
        id: OptionId::OutDir,
        name: "out-dir",
        shape: Shape::Scalar,
        layered: true,
        value_name: "DIR",
        help: "Write segments under DIR/kv. Only sealed Parquet files may ship.",
    },
    OptionSpec {
        id: OptionId::SegmentBytes,
        name: "segment-bytes",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Seal when the active segment reaches this byte size.",
    },
    OptionSpec {
        id: OptionId::SegmentSecs,
        name: "segment-secs",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Seal when the active segment reaches this age in seconds.",
    },
    OptionSpec {
        id: OptionId::WorkloadClass,
        name: "workload-class",
        shape: Shape::Scalar,
        layered: true,
        value_name: "NAME",
        help: "Operator-defined workload label, stored in each segment header.",
    },
    OptionSpec {
        id: OptionId::MaxDiskBytes,
        name: "max-disk-bytes",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Retention cap for sealed segments. Allow extra space for active and conversion files.",
    },
    OptionSpec {
        id: OptionId::Duration,
        name: "duration",
        shape: Shape::Scalar,
        layered: true,
        value_name: "SECS",
        help: "Stop after a finite, non-negative duration.",
    },
    OptionSpec {
        id: OptionId::MaxBatches,
        name: "max-batches",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Stop after this many observed messages.",
    },
    OptionSpec {
        id: OptionId::HeartbeatSecs,
        name: "heartbeat-secs",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Lifecycle heartbeat interval; 0 disables heartbeats.",
    },
    OptionSpec {
        id: OptionId::PollTimeoutMs,
        name: "poll-timeout-ms",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Socket poll timeout in milliseconds; must be positive.",
    },
    OptionSpec {
        id: OptionId::ResidentBlocks,
        name: "resident-blocks",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Identity-state capacity per origin, subject to source and memory caps. See docs/install.md.",
    },
    OptionSpec {
        id: OptionId::MaxRssBytes,
        name: "max-rss-bytes",
        shape: Shape::Scalar,
        layered: true,
        value_name: "N",
        help: "Process RSS limit in bytes; 0 disables it. Exceeding it stops collection.",
    },
    OptionSpec {
        id: OptionId::LogLevel,
        name: "log-level",
        shape: Shape::Scalar,
        layered: true,
        value_name: "LEVEL",
        help: "Diagnostic level: error, warn, info, debug, or trace.",
    },
    OptionSpec {
        id: OptionId::LogFormat,
        name: "log-format",
        shape: Shape::Scalar,
        layered: true,
        value_name: "FORMAT",
        help: "Diagnostic format: text or json. Diagnostics use stderr.",
    },
    OptionSpec {
        id: OptionId::Help,
        name: "help",
        shape: Shape::Flag,
        layered: false,
        value_name: "",
        help: "Show this help.",
    },
    OptionSpec {
        id: OptionId::Version,
        name: "version",
        shape: Shape::Flag,
        layered: false,
        value_name: "",
        help: "Print the version and specification pin.",
    },
    OptionSpec {
        id: OptionId::Key,
        name: "key",
        shape: Shape::Scalar,
        layered: false,
        value_name: "KEY",
        help: "Inline hex/base64 key; process arguments may expose it. Prefer --key-file.",
    },
    OptionSpec {
        id: OptionId::Config,
        name: "config",
        shape: Shape::Scalar,
        layered: false,
        value_name: "PATH",
        help: "TOML configuration file. Keys use option names with underscores.",
    },
];

pub(super) fn lookup(name: &str) -> Option<&'static OptionSpec> {
    OPTIONS.iter().find(|spec| spec.name == name)
}

/// Walk flags and pair each with its value. A value is the next argument, or the text
/// after the first `=` in `--name=value`; either form accepts a value that starts with
/// `--`. Error messages name the option and never echo a value.
pub(super) fn entries(
    args: &[String],
) -> Result<Vec<(&'static OptionSpec, Option<&str>)>, CliError> {
    let mut entries = Vec::new();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let (name, inline) = match arg.strip_prefix("--").and_then(|rest| rest.split_once('=')) {
            Some((name, value)) => (format!("--{name}"), Some(value)),
            None => (arg.clone(), None),
        };
        let spec = OPTIONS
            .iter()
            .find(|spec| spec.id.short() == Some(name.as_str()))
            .or_else(|| name.strip_prefix("--").and_then(lookup))
            .ok_or_else(|| CliError::Usage("unknown argument; see --help".into()))?;
        let value = match (spec.shape, inline) {
            (Shape::Flag, None) => None,
            (Shape::Flag, Some(_)) => {
                return Err(CliError::Usage(format!("--{} takes no value", spec.name)))
            }
            (_, Some(value)) => Some(value),
            (_, None) => Some(
                args.next()
                    .ok_or_else(|| CliError::Usage(format!("--{} needs a value", spec.name)))?
                    .as_str(),
            ),
        };
        entries.push((spec, value));
    }
    Ok(entries)
}
