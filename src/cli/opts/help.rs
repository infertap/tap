// SPDX-License-Identifier: Apache-2.0
//! Render option help from the parser's option registry and shared defaults.

use std::fmt::Write as _;

use super::spec::{OptionId, Shape, OPTIONS};
use super::{
    DEFAULT_DP_LOCAL, DEFAULT_HEARTBEAT_SECS, DEFAULT_KEY_EPOCH, DEFAULT_MAX_RSS_BYTES,
    DEFAULT_POLL_TIMEOUT_MS, MIN_MAX_RSS_BYTES,
};
use infertap::sink::segments::{DEFAULT_MAX_BYTES, DEFAULT_MAX_DISK_BYTES, DEFAULT_MAX_SECS};

/// The command words and what each does, in help order.
const COMMANDS: &[(&str, &str)] = &[
    (
        "run",
        "Collect live events. The default when no command is given.",
    ),
    (
        "probe",
        "Check output freshness. Exit 0 for fresh output, 1 for stale or missing output.",
    ),
    (
        "check-config",
        "Validate options, key material, and provenance without connecting. Exit 0 or 2.",
    ),
    (
        "show-payload",
        "Print sample records using a test key. Does not validate a deployment.",
    ),
];

pub(crate) fn help() -> String {
    let mut text = String::from(
        "infertap - KV-cache telemetry for inference engines\n\n\
         USAGE:\n  infertap [run] --engine-endpoint <LABEL=ADDRESS> --out-dir <DIR> [options]\n  \
         infertap [run] --endpoints <MAP> --out <PATH> [options]\n  \
         infertap probe [options]\n  infertap check-config [options]\n  infertap show-payload\n\n\
         COMMANDS:\n",
    );
    for (word, description) in COMMANDS {
        let _ = writeln!(text, "  {word:<14} {description}");
    }
    text.push_str(
        "\nLive collection requires endpoints, one output target, and a key or --raw-ids.\n\
         --key, --key-file, and --raw-ids are mutually exclusive.\n\n\
         Options take a value as the next argument or as --name=value.\n\
         Configuration precedence: command line, INFERTAP_<OPTION>, then TOML file.\n\
         Environment names use uppercase and underscores. INFERTAP_CONFIG selects a file.\n\
         Key material (--key) and the command word are command-line only.\n\nOPTIONS:\n",
    );
    for spec in OPTIONS {
        // Writing into a String cannot fail.
        text.push_str("  ");
        if let Some(short) = spec.id.short() {
            let _ = write!(text, "{short}, ");
        }
        let _ = write!(text, "--{}", spec.name);
        if spec.shape != Shape::Flag {
            let _ = write!(text, " <{}>", spec.value_name);
        }
        text.push('\n');
        let mut description = spec.help.to_owned();
        if let Some(default) = default_value(spec.id) {
            let _ = write!(description, " Default: {default}.");
        }
        write_description(&mut text, &description);
    }
    let _ = writeln!(
        text,
        "\nThe nonzero --max-rss-bytes minimum is {MIN_MAX_RSS_BYTES}."
    );
    text
}

fn write_description(text: &mut String, description: &str) {
    const LINE_WIDTH: usize = 88;
    const INDENT: &str = "      ";
    text.push_str(INDENT);
    let mut width = INDENT.len();
    for word in description.split_whitespace() {
        if width > INDENT.len() {
            if width + 1 + word.len() > LINE_WIDTH {
                text.push('\n');
                text.push_str(INDENT);
                width = INDENT.len();
            } else {
                text.push(' ');
                width += 1;
            }
        }
        text.push_str(word);
        width += word.len();
    }
    text.push('\n');
}

fn default_value(option: OptionId) -> Option<String> {
    Some(match option {
        OptionId::HeartbeatSecs => DEFAULT_HEARTBEAT_SECS.to_string(),
        OptionId::PollTimeoutMs => DEFAULT_POLL_TIMEOUT_MS.to_string(),
        OptionId::KeyEpoch => DEFAULT_KEY_EPOCH.to_string(),
        OptionId::DpLocal => DEFAULT_DP_LOCAL.to_string(),
        OptionId::DpStartRank => super::Opts::default().dp_start_rank.to_string(),
        OptionId::MaxRssBytes => DEFAULT_MAX_RSS_BYTES.to_string(),
        OptionId::SegmentBytes => DEFAULT_MAX_BYTES.to_string(),
        OptionId::SegmentSecs => DEFAULT_MAX_SECS.to_string(),
        OptionId::MaxDiskBytes => DEFAULT_MAX_DISK_BYTES.to_string(),
        OptionId::Engine => super::Engine::default().to_string(),
        OptionId::LogLevel => crate::cli::logging::DEFAULT_LEVEL
            .to_string()
            .to_ascii_lowercase(),
        OptionId::LogFormat => crate::cli::logging::LogFormat::default().to_string(),
        _ => return None,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::help;
    use crate::cli::opts::parse;
    use crate::cli::output::segment_config;
    use std::path::Path;

    #[test]
    fn displayed_defaults_match_parsed_runtime_and_segment_settings() {
        let options = parse(&["--raw-ids", "--out-dir", "output"]).unwrap();
        let segments = segment_config(&options, Path::new("output/kv"));
        let text = help();
        for (name, value) in [
            ("heartbeat-secs", options.heartbeat_secs),
            ("poll-timeout-ms", options.poll_timeout_ms),
            ("max-rss-bytes", options.max_rss_bytes),
            ("segment-bytes", segments.max_bytes),
            ("segment-secs", segments.max_secs),
            ("max-disk-bytes", segments.max_disk_bytes.unwrap()),
        ] {
            let section = text.split_once(&format!("  --{name} <N>\n")).unwrap().1;
            let description = section
                .split("\n  --")
                .next()
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            assert!(
                description.contains(&format!("Default: {value}.")),
                "{name}: {description}"
            );
        }
    }
}
