// SPDX-License-Identifier: Apache-2.0
//! Parse command modes and runtime options.
//! Configuration layering and endpoint expansion live in private submodules.
//! Parsing may read a configuration file but does not open output or connect sockets.

mod endpoints;
mod spec;
use endpoints::{expand_engine_endpoints, parse_endpoints, parse_engine_endpoints};

use crate::cli::error::{CliError, ProvenanceError};
use infertap::record::{InstanceLabel, Provenance};
use infertap::source::{Endpoint, Engine, SourceError, SourceKey};
use std::collections::HashMap;
use std::path::PathBuf;

mod help;
pub(crate) use help::help;
use spec::OptionId;

pub(crate) use infertap::lifecycle::DEFAULT_HEARTBEAT_SECS;
pub(crate) const DEFAULT_POLL_TIMEOUT_MS: u64 = 250;
pub(crate) const DEFAULT_KEY_EPOCH: u32 = 0;
pub(crate) const DEFAULT_DP_LOCAL: u32 = 1;

/// The operation selected by the command word, or by `--help` and `--version`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Command {
    /// Live collection. The default when no command word is given.
    #[default]
    Run,
    Help,
    Version,
    ShowPayload,
    CheckConfig,
    Probe,
}

impl Command {
    /// The command words, in help order.
    const WORDS: &[(&str, Self)] = &[
        ("run", Self::Run),
        ("probe", Self::Probe),
        ("check-config", Self::CheckConfig),
        ("show-payload", Self::ShowPayload),
        ("help", Self::Help),
        ("version", Self::Version),
    ];

    /// Split the command word off the front of `args`. A first argument that is not a
    /// flag must be a command word; without one the command is `run`. `--help` and
    /// `--version` among the flags select their command.
    fn split(args: &[String]) -> Result<(Self, &[String]), CliError> {
        let (mut command, rest) = match args.first() {
            Some(word) if !word.starts_with('-') => {
                let command = Self::WORDS
                    .iter()
                    .find(|(w, _)| w == word)
                    .map(|(_, c)| *c)
                    .ok_or_else(|| CliError::Usage("unknown command; see --help".into()))?;
                (command, &args[1..])
            }
            _ => (Self::Run, args),
        };
        for (spec, _) in spec::entries(rest)? {
            match spec.id {
                OptionId::Help => command = Self::Help,
                OptionId::Version if command != Self::Help => command = Self::Version,
                _ => {}
            }
        }
        Ok((command, rest))
    }
}

#[derive(Default)]
pub(crate) struct Opts {
    pub(crate) command: Command,
    /// Every subscription the tap dials: the manual `--endpoints` map plus the data-parallel
    /// expansion of every `--engine-endpoint`.
    pub(crate) endpoints: Vec<Endpoint>,
    /// Engine base endpoints, expanded into per-rank sources by the `--dp-*` flags below
    /// and merged into `endpoints` at the end of parse.
    pub(crate) engine_endpoints: Vec<EngineBase>,
    /// Replay endpoints, mirroring the engine's `kv_events_config.replay_endpoint` and
    /// expanded per rank exactly like the main endpoints (both engines offset both identically).
    /// Labels must be a subset of those in `engine_endpoints`; a replay for an unconfigured
    /// engine is a stated mistake.
    pub(crate) engine_replay_endpoints: Vec<EngineBase>,
    pub(crate) replay_endpoints: Vec<Endpoint>,
    pub(crate) dp_start_rank: u32,
    pub(crate) dp_local: Option<u32>,
    pub(crate) dp_max: Option<u32>,
    /// Source key to instance label, for every source in `endpoints`. A record carries the
    /// label; the tracker and the bridge key on the source, so two ranks of one engine
    /// never interleave their sequences.
    pub(crate) labels: HashMap<SourceKey, InstanceLabel>,
    pub(crate) engine: Engine,
    /// Where the egress key comes from; `None` until the operator says, which a live run
    /// refuses.
    pub(crate) key: Option<KeySource>,
    pub(crate) key_epoch: Option<u32>,
    pub(crate) provenance: Option<String>,
    /// Where records go; `None` until the operator says, which a live run refuses.
    pub(crate) output: Option<Output>,
    pub(crate) segment_bytes: Option<u64>,
    pub(crate) segment_secs: Option<u64>,
    pub(crate) workload_class: Option<String>,
    pub(crate) max_disk_bytes: Option<u64>,
    pub(crate) duration: Option<std::time::Duration>,
    pub(crate) max_batches: Option<u64>,
    pub(crate) heartbeat_secs: u64,
    pub(crate) poll_timeout_ms: u64,
    pub(crate) resident_blocks: Option<usize>,
    pub(crate) max_rss_bytes: u64,
    /// `None` is the default, info.
    pub(crate) log_level: Option<log::LevelFilter>,
    pub(crate) log_format: crate::cli::logging::LogFormat,
}

/// One `--engine-endpoint` as given: the instance label its ranks roll up to, and the
/// base address rank 0 publishes on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EngineBase {
    pub(crate) label: InstanceLabel,
    pub(crate) address: String,
}

/// Select the egress key source or explicit raw mode. Subscription requires one choice.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum KeySource {
    /// `--key`: key material on argv may be exposed through process inspection.
    Argv(String),
    /// `--key-file`: read key material from a credential file or secret mount.
    /// Key parsing zeroes the temporary read buffer.
    File(PathBuf),
    /// `--raw-ids`: emit raw identities. Intended for local inspection; output is sensitive.
    Raw,
}

impl std::fmt::Debug for KeySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Argv(_) => f.write_str("Argv([REDACTED])"),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
            Self::Raw => f.write_str("Raw"),
        }
    }
}

impl KeySource {
    /// The one choice three flags make, or the usage error for a contradiction.
    fn from_flags(
        key: Option<String>,
        key_file: Option<PathBuf>,
        raw_ids: bool,
    ) -> Result<Option<Self>, CliError> {
        match (key, key_file, raw_ids) {
            (Some(_), Some(_), _) => Err(CliError::Usage(
                "--key and --key-file are mutually exclusive".to_string(),
            )),
            (None, Some(_), true) | (Some(_), None, true) => Err(CliError::Usage(
                "--raw-ids contradicts --key/--key-file: a run is keyed or explicitly \
                 keyless, never both"
                    .to_string(),
            )),
            (Some(k), None, false) => Ok(Some(Self::Argv(k))),
            (None, Some(path), false) => Ok(Some(Self::File(path))),
            (None, None, true) => Ok(Some(Self::Raw)),
            (None, None, false) => Ok(None),
        }
    }

    /// Whether identities are mapped under a key.
    pub(crate) const fn is_keyed(&self) -> bool {
        matches!(self, Self::Argv(_) | Self::File(_))
    }
}

/// Where records go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Output {
    /// `--out-dir`: the service form, sealed segments under this root.
    Segments(PathBuf),
    /// `--out`: the one-shot form, a single file that must not already exist.
    File(PathBuf),
}

impl Output {
    /// The one choice two flags make, or the usage error for both.
    fn from_flags(
        out: Option<PathBuf>,
        out_dir: Option<PathBuf>,
    ) -> Result<Option<Self>, CliError> {
        match (out, out_dir) {
            (Some(_), Some(_)) => Err(CliError::Usage(
                "--out and --out-dir are mutually exclusive".to_string(),
            )),
            (Some(path), None) => Ok(Some(Self::File(path))),
            (None, Some(root)) => Ok(Some(Self::Segments(root))),
            (None, None) => Ok(None),
        }
    }
}

/// Default process RSS limit: 768 MiB.
/// The packaged service also enforces a 1 GiB cgroup limit, which includes file cache.
pub(crate) const DEFAULT_MAX_RSS_BYTES: u64 = 768 * 1024 * 1024;

/// Smallest nonzero `--max-rss-bytes` accepted. A bound below the tap's own healthy working
/// set would kill a correct run at startup; refusing it at the door is the same rule the
/// segment ring applies to a disk cap under 4x the segment size.
pub(crate) const MIN_MAX_RSS_BYTES: u64 = 256 * 1024 * 1024;

impl Opts {
    /// Parse the real command line, layered over `INFERTAP_*` in the environment and the
    /// config file `--config` (or `INFERTAP_CONFIG`) names. A flag beats the environment,
    /// which beats the file; nothing else is consulted.
    pub(crate) fn parse(args: &[String]) -> Result<Self, CliError> {
        Self::parse_with(args, std::env::vars(), &|path| {
            std::fs::read_to_string(path)
        })
    }

    /// [`parse`](Self::parse) with the environment and the file reader supplied, so a test
    /// is hermetic. `env` is every variable; only `INFERTAP_*` names are read.
    pub(crate) fn parse_with(
        args: &[String],
        env: impl Iterator<Item = (String, String)>,
        read_file: &dyn Fn(&std::path::Path) -> std::io::Result<String>,
    ) -> Result<Self, CliError> {
        let (command, args) = Command::split(args)?;
        if matches!(command, Command::Help | Command::Version) {
            return Ok(Self {
                command,
                ..Self::default()
            });
        }
        let layered = config::layer(args, env, read_file)?;
        Self::parse_argv(&layered, command)
    }

    /// The flag parser proper, over one synthetic argv the layers have already merged.
    fn parse_argv(args: &[String], command: Command) -> Result<Self, CliError> {
        let mut options = Opts {
            command,
            heartbeat_secs: DEFAULT_HEARTBEAT_SECS,
            poll_timeout_ms: DEFAULT_POLL_TIMEOUT_MS,
            max_rss_bytes: DEFAULT_MAX_RSS_BYTES,
            ..Opts::default()
        };
        // The flags that combine into one choice are staged, then judged together below.
        let (mut key, mut key_file, mut raw_ids) = (None, None, false);
        let (mut out, mut out_dir) = (None, None);
        for (spec, raw_value) in spec::entries(args)? {
            let arg = format!("--{}", spec.name);
            let value = || -> Result<String, CliError> {
                raw_value
                    .map(str::to_owned)
                    .ok_or_else(|| CliError::Usage(format!("{arg} needs a value")))
            };
            match spec.id {
                OptionId::Help | OptionId::Version => {}
                OptionId::Endpoints => options.endpoints = parse_endpoints(&value()?)?,
                OptionId::EngineEndpoint => {
                    options
                        .engine_endpoints
                        .extend(parse_engine_endpoints(&value()?)?);
                }
                OptionId::EngineReplayEndpoint => {
                    options
                        .engine_replay_endpoints
                        .extend(parse_engine_endpoints(&value()?)?);
                }
                OptionId::DpStartRank => options.dp_start_rank = parse_num(&value()?, &arg)?,
                OptionId::DpLocal => options.dp_local = Some(parse_num(&value()?, &arg)?),
                OptionId::DpMax => options.dp_max = Some(parse_num(&value()?, &arg)?),
                OptionId::Engine => {
                    options.engine = value()?.parse().map_err(|_: SourceError| {
                        CliError::Usage("--engine: unsupported engine".into())
                    })?;
                }
                OptionId::Key => key = Some(value()?),
                OptionId::KeyFile => key_file = Some(PathBuf::from(value()?)),
                OptionId::RawIds => raw_ids = true,
                OptionId::KeyEpoch => options.key_epoch = Some(parse_num(&value()?, &arg)?),
                OptionId::Provenance => options.provenance = Some(value()?),
                OptionId::Out => out = Some(PathBuf::from(value()?)),
                OptionId::OutDir => out_dir = Some(PathBuf::from(value()?)),
                OptionId::SegmentBytes => options.segment_bytes = Some(parse_num(&value()?, &arg)?),
                OptionId::SegmentSecs => options.segment_secs = Some(parse_num(&value()?, &arg)?),
                OptionId::WorkloadClass => options.workload_class = Some(value()?),
                OptionId::MaxDiskBytes => {
                    options.max_disk_bytes = Some(parse_num(&value()?, &arg)?);
                }
                OptionId::Duration => {
                    // Reject negative, non-finite, and out-of-range durations before conversion.
                    let raw = value()?;
                    let secs: f64 = parse_num(&raw, &arg)?;
                    options.duration =
                        Some(std::time::Duration::try_from_secs_f64(secs).map_err(|_| {
                            CliError::Usage(
                                "--duration: expected a finite, non-negative duration".into(),
                            )
                        })?);
                }
                OptionId::MaxBatches => options.max_batches = Some(parse_num(&value()?, &arg)?),
                OptionId::HeartbeatSecs => options.heartbeat_secs = parse_num(&value()?, &arg)?,
                OptionId::PollTimeoutMs => options.poll_timeout_ms = parse_num(&value()?, &arg)?,
                OptionId::ResidentBlocks => {
                    options.resident_blocks = Some(parse_num(&value()?, &arg)?);
                }
                OptionId::MaxRssBytes => options.max_rss_bytes = parse_num(&value()?, &arg)?,
                OptionId::LogLevel => {
                    options.log_level =
                        Some(crate::cli::logging::parse_level(&value()?).map_err(CliError::Usage)?);
                }
                OptionId::LogFormat => {
                    options.log_format = value()?.parse().map_err(CliError::Usage)?;
                }
                // The configuration file has already been read by the layering step.
                OptionId::Config => {
                    value()?;
                }
            }
        }
        options.key = KeySource::from_flags(key, key_file, raw_ids)?;
        options.output = Output::from_flags(out, out_dir)?;
        options.validate()?;
        // Probes inspect local output and need no subscription endpoints.
        if options.command != Command::Probe {
            options.resolve_dp()?;
        }
        Ok(options)
    }

    /// The bounds the command line refuses once every flag is read.
    fn validate(&self) -> Result<(), CliError> {
        if self.key_epoch.is_some() && !self.key.as_ref().is_some_and(KeySource::is_keyed) {
            return Err(CliError::Usage(
                "--key-epoch requires --key or --key-file".to_string(),
            ));
        }
        // Reject zero poll timeouts to prevent an idle busy loop.
        if self.poll_timeout_ms == 0 {
            return Err(CliError::Usage(
                "--poll-timeout-ms 0 is a busy-spin, not a poll; the minimum is 1".to_string(),
            ));
        }
        // Reject nonzero RSS limits below the minimum working-set allowance.
        // Zero explicitly disables the guard.
        if self.max_rss_bytes != 0 && self.max_rss_bytes < MIN_MAX_RSS_BYTES {
            return Err(CliError::Usage(format!(
                "--max-rss-bytes: the nonzero minimum is {MIN_MAX_RSS_BYTES} (0 disables the bound)"
            )));
        }
        Ok(())
    }

    /// The derived DP endpoints (docs/install.md, "Data parallelism"): each engine base
    /// expands into per-rank sources, then merges with any manual --endpoints map.
    /// Defaults of start 0, local 1, max 1 reproduce the single-socket shape exactly.
    fn resolve_dp(&mut self) -> Result<(), CliError> {
        if self.engine_endpoints.is_empty()
            && (self.dp_start_rank != 0 || self.dp_local.is_some() || self.dp_max.is_some())
        {
            return Err(CliError::Usage(
                "--dp-start-rank/--dp-local/--dp-max derive ports for --engine-endpoint; \
                 with --endpoints you are stating each rank's port yourself"
                    .to_string(),
            ));
        }
        let local = self.dp_local.unwrap_or(DEFAULT_DP_LOCAL);
        let expanded = expand_engine_endpoints(
            &self.engine_endpoints,
            self.dp_start_rank,
            local,
            self.dp_max.unwrap_or(local),
        )?;
        for replay in &self.engine_replay_endpoints {
            if !self
                .engine_endpoints
                .iter()
                .any(|e| e.label == replay.label)
            {
                return Err(CliError::Usage("--engine-replay-endpoint names no --engine-endpoint; configure its engine label first".into()));
            }
        }
        // Replay expands over the configured ranks only, never the headroom. A rank that
        // appears later under elastic scaling starts a fresh ring at sequence zero, so
        // there is no history to drain, and dialing the empty slot would cost a timeout
        // and a false incomplete count per headroom rank.
        self.replay_endpoints = expand_engine_endpoints(
            &self.engine_replay_endpoints,
            self.dp_start_rank,
            local,
            local,
        )?
        .endpoints;
        // A manual endpoint's source is its own label.
        for e in &self.endpoints {
            self.labels
                .insert(e.source.clone(), InstanceLabel::from(&e.source));
        }
        self.endpoints.extend(expanded.endpoints);
        self.labels.extend(expanded.labels);
        let mut seen = std::collections::HashSet::new();
        for e in &self.endpoints {
            if !seen.insert(&e.source) {
                return Err(CliError::Usage(
                    "a source appears twice across --endpoints/--engine-endpoint".into(),
                ));
            }
        }
        Ok(())
    }
}

pub(crate) fn parse_num<T: std::str::FromStr>(s: &str, flag: &str) -> Result<T, CliError> {
    s.parse()
        .map_err(|_| CliError::Usage(format!("{flag}: invalid numeric value")))
}

/// The instance label a source key rolls up to: what the expansion recorded, or the key
/// itself for a source no configuration labelled (a test source, say).
pub(crate) fn label_of(
    labels: &HashMap<SourceKey, InstanceLabel>,
    source: &SourceKey,
) -> InstanceLabel {
    labels
        .get(source)
        .cloned()
        .unwrap_or_else(|| InstanceLabel::from(source))
}

pub(crate) fn load_provenance(arg: Option<&str>) -> Result<Provenance, CliError> {
    let Some(arg) = arg else {
        return Ok(Provenance::default());
    };
    // Inline JSON or a path to it. The sidecar passes the object inline, because argv is
    // the only place the downward API can compose the pod name into. A file suits
    // operators who keep provenance out of process lists.
    let (origin, text) = if arg.trim_start().starts_with('{') {
        ("--provenance", arg.to_string())
    } else {
        (
            arg,
            std::fs::read_to_string(arg).map_err(|e| CliError::Provenance {
                origin: arg.to_string(),
                source: ProvenanceError::Read(e),
            })?,
        )
    };
    match serde_json::from_str::<serde_json::Value>(&text) {
        // Reject recognized content-bearing keys and keys reserved by the header
        // before connecting or opening output. Values remain operator-controlled.
        Ok(serde_json::Value::Object(m)) => {
            Provenance::try_from(m).map_err(|e| CliError::Provenance {
                origin: origin.to_string(),
                source: ProvenanceError::Invalid(e),
            })
        }
        Ok(_) => Err(CliError::Provenance {
            origin: origin.to_string(),
            source: ProvenanceError::NotAnObject,
        }),
        Err(e) => Err(CliError::Provenance {
            origin: origin.to_string(),
            source: ProvenanceError::Parse(e),
        }),
    }
}

/// The configuration layers beneath the command line: a TOML file and `INFERTAP_*` in the
/// environment, both expressed in the flag vocabulary and merged into one synthetic argv
/// so the flag parser stays the single definition of every option.
///
/// Precedence is flag over environment over file, per key: a key the command line sets is
/// dropped from both lower layers, and a key the environment sets is dropped from the
/// file. A list flag such as `engine_endpoint` is therefore replaced by a higher layer,
/// never extended by it. `key` is argv-only: key material does not belong in a file that
/// is often world-readable and checked in, nor in an environment a process tree inherits.
/// Both lower layers name `key_file` instead. `help`, `show_payload`, and `config` itself
/// are argv-only too.
mod config;

/// Parse a borrowed argv in tests, with the error flattened to text for assertions.
#[cfg(test)]
pub(crate) fn parse(args: &[&str]) -> Result<Opts, String> {
    let owned: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    Opts::parse_with(&owned, std::iter::empty(), &|p| {
        Err(std::io::Error::other(format!(
            "no config file in tests: {}",
            p.display()
        )))
    })
    .map_err(|e| e.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn informational_commands_ignore_invalid_runtime_configuration() {
        for flag in ["--help", "--version"] {
            let args = vec![flag.into(), "--config".into(), "/missing/config".into()];
            let env = vec![("INFERTAP_POLL_TIMEOUT_MS".into(), "0".into())];
            let result =
                Opts::parse_with(&args, env.into_iter(), &|_| panic!("must not read config"));
            assert!(result.is_ok());
        }
        assert!(parse(&["probe", "check-config"])
            .err()
            .expect("a second word is not a flag")
            .contains("unknown argument"));
        let args = vec!["check-config".into()];
        let env = vec![("INFERTAP_POLL_TIMEOUT_MS".into(), "0".into())];
        assert!(Opts::parse_with(&args, env.into_iter(), &|_| unreachable!()).is_err());
    }

    #[test]
    fn option_values_are_not_interpreted_as_flags() {
        let args = vec!["--workload-class".into(), "--help".into()];
        let options =
            Opts::parse_with(&args, std::iter::empty(), &|_| unreachable!()).expect("parse");
        assert_eq!(options.command, Command::Run);
        assert_eq!(options.workload_class.as_deref(), Some("--help"));
        let env = vec![("INFERTAP_RAW_IDS".into(), "true".into())];
        let args = vec!["--workload-class".into(), "--raw-ids".into()];
        let options = Opts::parse_with(&args, env.into_iter(), &|_| unreachable!()).expect("parse");
        assert_eq!(options.key, Some(KeySource::Raw));
    }

    /// The command is the first argument. Without one the tap runs; `--help` and
    /// `--version` work as flags too, and an unknown word is refused.
    #[test]
    fn the_command_word_is_the_first_argument() {
        assert_eq!(parse(&["--raw-ids"]).expect("parse").command, Command::Run);
        assert_eq!(
            parse(&["run", "--raw-ids"]).expect("parse").command,
            Command::Run
        );
        assert_eq!(
            parse(&["check-config"]).expect("parse").command,
            Command::CheckConfig
        );
        assert_eq!(parse(&["probe"]).expect("parse").command, Command::Probe);
        assert_eq!(
            parse(&["show-payload"]).expect("parse").command,
            Command::ShowPayload
        );
        assert_eq!(parse(&["help"]).expect("parse").command, Command::Help);
        assert_eq!(
            parse(&["version"]).expect("parse").command,
            Command::Version
        );
        assert_eq!(
            parse(&["--version"]).expect("parse").command,
            Command::Version
        );
        assert_eq!(parse(&["-V"]).expect("parse").command, Command::Version);
        let e = parse(&["frobnicate"]).err().expect("unknown word");
        assert!(e.contains("unknown command"), "{e}");
    }

    /// Reject an expanded rank range that exceeds u32, before deriving ports.
    #[test]
    fn a_start_rank_at_the_top_of_the_rank_space_is_refused_not_wrapped() {
        let Err(e) = parse(&[
            "--engine-endpoint",
            "e=tcp://10.0.0.7:5557",
            "--dp-start-rank",
            "4294967295",
            "--dp-max",
            "2",
        ]) else {
            panic!("a start rank at u32::MAX must be refused");
        };
        assert!(e.contains("rank space"), "{e}");
        // The same arithmetic one below the top is a plain port-range refusal.
        let Err(e) = parse(&[
            "--engine-endpoint",
            "e=tcp://10.0.0.7:5557",
            "--dp-start-rank",
            "4294967294",
            "--dp-max",
            "2",
        ]) else {
            panic!("a rank past the port range must be refused");
        };
        assert!(e.contains("past 65535"), "{e}");
    }

    #[test]
    fn dp_defaults_reproduce_the_single_socket_shape() {
        let options = parse(&["--engine-endpoint", "i0=tcp://127.0.0.1:5557"]).expect("parse");
        assert_eq!(
            options.endpoints,
            vec![Endpoint {
                source: SourceKey::from("i0"),
                address: "tcp://127.0.0.1:5557".to_string()
            }]
        );
        assert_eq!(
            options.labels.get("i0").map(InstanceLabel::as_str),
            Some("i0")
        );
    }

    /// The install guide's arithmetic, verified against the flags: rank N publishes on
    /// base+N with N global, the bound widens the range past the local count for elastic
    /// headroom, and every rank of one engine carries one label.
    #[test]
    fn dp_expansion_derives_global_rank_ports_under_one_label() {
        let options = parse(&[
            "--engine-endpoint",
            "e=tcp://10.0.0.7:5557",
            "--dp-start-rank",
            "4",
            "--dp-local",
            "2",
            "--dp-max",
            "4",
        ])
        .expect("parse");
        let expect: Vec<Endpoint> = (4..8)
            .map(|r| Endpoint {
                source: SourceKey::from(format!("e#{r}")),
                address: format!("tcp://10.0.0.7:{}", 5557 + r),
            })
            .collect();
        assert_eq!(options.endpoints, expect);
        for e in &options.endpoints {
            assert_eq!(
                options.labels.get(&e.source).map(InstanceLabel::as_str),
                Some("e")
            );
        }
    }

    /// Wildcard addresses are for engine binding. The tap needs a concrete connect address.
    #[test]
    fn a_wildcard_engine_endpoint_is_refused_as_the_engines_bind_shape() {
        let e = parse(&["--engine-endpoint", "i0=tcp://*:5557"])
            .err()
            .expect("must refuse");
        assert!(e.contains("bind shape"), "{e}");
    }

    /// The subscription capacity must cover all configured local ranks.
    #[test]
    fn dp_max_below_dp_local_is_refused() {
        let e = parse(&[
            "--engine-endpoint",
            "i0=tcp://h:5557",
            "--dp-local",
            "4",
            "--dp-max",
            "2",
        ])
        .err()
        .expect("must refuse");
        assert!(e.contains("below --dp-local"), "{e}");
    }

    /// Rank expansion flags require --engine-endpoint; they do not apply to manual maps.
    #[test]
    fn dp_flags_without_an_engine_endpoint_are_refused() {
        let e = parse(&["--endpoints", "i0=tcp://h:5557", "--dp-local", "2"])
            .err()
            .expect("must refuse");
        assert!(e.contains("--engine-endpoint"), "{e}");
    }

    #[test]
    fn a_replay_endpoint_for_an_unconfigured_engine_is_refused() {
        let e = parse(&[
            "--engine-endpoint",
            "i0=tcp://h:5557",
            "--engine-replay-endpoint",
            "i1=tcp://h:5657",
        ])
        .err()
        .expect("must refuse");
        assert!(e.contains("names no --engine-endpoint"), "{e}");
    }

    #[test]
    fn replay_endpoints_expand_onto_the_same_source_keys() {
        let options = parse(&[
            "--engine-endpoint",
            "e=tcp://h:5557",
            "--engine-replay-endpoint",
            "e=tcp://h:5657",
            "--dp-local",
            "2",
        ])
        .expect("parse");
        let expect: Vec<Endpoint> = (0..2)
            .map(|r| Endpoint {
                source: SourceKey::from(format!("e#{r}")),
                address: format!("tcp://h:{}", 5657 + r),
            })
            .collect();
        assert_eq!(options.replay_endpoints, expect);
    }

    /// Replay expands over the configured ranks only: the headroom has no ring to drain,
    /// and dialing an empty slot costs a timeout and a false "incomplete" each.
    #[test]
    fn replay_expansion_stops_at_the_configured_ranks_not_the_bound() {
        let options = parse(&[
            "--engine-endpoint",
            "e=tcp://h:5557",
            "--engine-replay-endpoint",
            "e=tcp://h:5657",
            "--dp-local",
            "2",
            "--dp-max",
            "4",
        ])
        .expect("parse");
        assert_eq!(
            options.endpoints.len(),
            4,
            "the SUB set covers the headroom"
        );
        assert_eq!(
            options.replay_endpoints.len(),
            2,
            "the drain covers only what exists"
        );
    }

    /// The sidecar passes provenance inline: argv is where the pod name composes in.
    #[test]
    fn provenance_accepts_inline_json_as_the_chart_passes_it() {
        let m = super::load_provenance(Some(r#"{"model": "m", "pod_identity": "p"}"#))
            .expect("inline object");
        assert_eq!(m.as_map().get("model").and_then(|v| v.as_str()), Some("m"));
        let e =
            super::load_provenance(Some("{not json")).expect_err("malformed inline must refuse");
        assert!(e.to_string().contains("--provenance"), "{e}");
    }

    /// One source key, one socket: a collision across the manual map and the expansion
    /// would silently merge two streams' sequence spaces.
    #[test]
    fn duplicate_sources_across_the_two_maps_are_refused() {
        let e = parse(&[
            "--endpoints",
            "i0=tcp://h:6000",
            "--engine-endpoint",
            "i0=tcp://h:5557",
        ])
        .err()
        .expect("must refuse");
        assert!(e.contains("twice"), "{e}");
    }

    #[test]
    fn wrong_typed_values_are_rejected_not_coerced() {
        assert!(parse(&["--key-epoch", "-1"]).is_err());
        assert!(parse(&["--duration", "soon"]).is_err());
        assert!(parse(&["--heartbeat-secs", "1m"]).is_err());
        assert!(parse(&["--poll-timeout-ms", "250ms"]).is_err());
    }

    #[test]
    fn degenerate_bounds_are_rejected_at_parse() {
        // 0 would busy-spin the poll loop on the node the engine serves from
        assert!(parse(&["--poll-timeout-ms", "0"]).is_err());
        // Reject non-finite and negative durations during option parsing.
        assert!(parse(&["--duration", "-1"]).is_err());
        assert!(parse(&["--duration", "NaN"]).is_err());
        assert!(parse(&["--duration", "inf"]).is_err());
        // Zero selects an empty collection window.
        assert!(parse(&["--duration", "0"]).is_ok());
        // Reject a memory limit below the supported minimum.
        assert!(parse(&["--max-rss-bytes", "1048576"]).is_err());
        assert!(parse(&["--max-rss-bytes", "0"]).is_ok()); // the explicit opt-out
        assert!(parse(&["--max-rss-bytes", "805306368"]).is_ok());
    }

    #[test]
    fn key_epoch_without_key_is_rejected() {
        assert!(parse(&["--key-epoch", "1"]).is_err());
        // key VALIDITY (length, encoding) is checked where the key is used, not at arg parse
        assert!(parse(&["--key", "aa", "--key-epoch", "1"]).is_ok());
        assert!(parse(&["--key-file", "/dev/null", "--key-epoch", "1"]).is_ok());
    }

    #[test]
    fn key_and_key_file_are_mutually_exclusive() {
        assert!(parse(&["--key", "aa", "--key-file", "/dev/null"]).is_err());
    }

    #[test]
    fn malformed_endpoints_are_rejected() {
        assert!(parse(&["--endpoints", "no-equals-sign"]).is_err());
        assert!(parse(&["--endpoints", "i0=tcp://h:1,broken"]).is_err());
    }

    /// `--name=value` is the form unit files and manifests reach for. It reads the same
    /// as the two-argument form, splits at the first `=` only, and a flag refuses a value.
    #[test]
    fn the_equals_form_pairs_a_value_with_its_option() {
        let options = parse(&[
            "--raw-ids",
            "--out-dir=/var/lib/infertap",
            "--segment-secs=30",
            "--workload-class=a=b",
        ])
        .expect("parse");
        assert_eq!(
            options.output,
            Some(Output::Segments(PathBuf::from("/var/lib/infertap")))
        );
        assert_eq!(options.segment_secs, Some(30));
        assert_eq!(options.workload_class.as_deref(), Some("a=b"));
        let e = parse(&["--raw-ids=true"])
            .err()
            .expect("a flag takes no value");
        assert!(e.contains("takes no value"), "{e}");
        assert!(!e.contains("true"), "the value is not echoed: {e}");
        let e = parse(&["--frobnicate=1"]).err().expect("unknown");
        assert!(!e.contains('1'), "the value is not echoed: {e}");
    }

    #[test]
    fn unknown_arguments_and_missing_values_are_rejected() {
        assert!(parse(&["--frobnicate"]).is_err());
        assert!(parse(&["--key-epoch"]).is_err()); // flag with no value
    }

    #[test]
    fn non_object_provenance_is_rejected_not_coerced() {
        let root = infertap::testing::scratch("cli-provenance");
        let path = root.join("provenance.json");
        std::fs::write(&path, "[1, 2, 3]").expect("write");
        assert!(load_provenance(path.to_str()).is_err());
        std::fs::write(&path, r#"{"gpu": "H100"}"#).expect("write");
        assert!(load_provenance(path.to_str()).is_ok());
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
