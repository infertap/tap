// SPDX-License-Identifier: Apache-2.0
//! Merge configuration files, environment variables, and command-line options.

use std::collections::BTreeMap;
use std::path::Path;

use crate::cli::error::CliError;

use super::spec::{self, OptionId, Shape};

/// One layered value, already in argv vocabulary: the tokens that flag contributes.
type Layer = BTreeMap<&'static str, Vec<String>>;

fn key_of(name: &str) -> Option<&'static str> {
    spec::lookup(name)
        .filter(|spec| spec.layered)
        .map(|spec| spec.name)
}

/// Merge the file, the environment and the command line into one argv: file tokens
/// first, then environment tokens, then the real arguments, with every key the command
/// line names removed from both lower layers and every key the environment names
/// removed from the file.
pub(super) fn layer(
    args: &[String],
    env: impl Iterator<Item = (String, String)>,
    read_file: &dyn Fn(&Path) -> std::io::Result<String>,
) -> Result<Vec<String>, CliError> {
    let env: Vec<(String, String)> = env.filter(|(k, _)| k.starts_with("INFERTAP_")).collect();
    let entries = spec::entries(args)?;
    let argv_keys: Vec<_> = entries
        .iter()
        .filter(|(spec, _)| spec.layered)
        .map(|(spec, _)| spec.name)
        .collect();
    let config_path = entries
        .iter()
        .find(|(spec, _)| spec.id == OptionId::Config)
        .and_then(|(_, value)| value.map(str::to_owned))
        .or_else(|| {
            env.iter()
                .find(|(k, _)| k == "INFERTAP_CONFIG")
                .map(|(_, v)| v.clone())
        });
    let mut file_layer = match config_path {
        Some(path) => {
            let text = read_file(Path::new(&path))
                .map_err(|e| CliError::Usage(format!("--config {path}: reading it failed: {e}")))?;
            from_toml(&path, &text)?
        }
        None => Layer::new(),
    };
    let mut env_layer = from_env(&env)?;
    for k in &argv_keys {
        env_layer.remove(k);
        file_layer.remove(k);
    }
    for k in env_layer.keys() {
        file_layer.remove(k);
    }
    let mut out: Vec<String> = Vec::new();
    for tokens in file_layer.into_values() {
        out.extend(tokens);
    }
    for tokens in env_layer.into_values() {
        out.extend(tokens);
    }
    out.extend(args.iter().cloned());
    Ok(out)
}

fn tokens(key: &'static str, value: &toml::Value, origin: &str) -> Result<Vec<String>, CliError> {
    let flag = format!("--{key}");
    if spec::lookup(key).is_some_and(|spec| spec.shape == Shape::Flag) {
        return match value {
            toml::Value::Boolean(true) => Ok(vec![flag]),
            toml::Value::Boolean(false) => Ok(Vec::new()),
            other => Err(CliError::Usage(format!(
                "{origin}: {key} must be true or false, found {}",
                other.type_str()
            ))),
        };
    }
    match value {
        toml::Value::String(s) => Ok(vec![flag, s.clone()]),
        toml::Value::Integer(n) => Ok(vec![flag, n.to_string()]),
        toml::Value::Float(f) => Ok(vec![flag, f.to_string()]),
        toml::Value::Array(items)
            if spec::lookup(key).is_some_and(|spec| spec.shape == Shape::List) =>
        {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    toml::Value::String(s) => parts.push(s.clone()),
                    other => {
                        return Err(CliError::Usage(format!(
                            "{origin}: {key} must be a list of strings, found {}",
                            other.type_str()
                        )))
                    }
                }
            }
            Ok(vec![flag, parts.join(",")])
        }
        other => Err(CliError::Usage(format!(
            "{origin}: {key} has an unsupported type {}",
            other.type_str()
        ))),
    }
}

fn from_toml(path: &str, text: &str) -> Result<Layer, CliError> {
    let table: toml::Table = text.parse().map_err(|e: toml::de::Error| {
        // Parser displays include source text, which may contain a misplaced secret.
        let location = e
            .span()
            .map_or(String::new(), |s| format!(" near byte {}", s.start));
        CliError::Usage(format!("--config {path}: invalid TOML{location}"))
    })?;
    let origin = format!("--config {path}");
    let mut layer = Layer::new();
    for (name, value) in table {
        let flag_name = name.replace('_', "-");
        let Some(key) = key_of(&flag_name) else {
            return Err(CliError::Usage(format!(
                "{origin}: unknown key {name:?} (the keys are the flags in --help, with \
                     underscores; `key`, `config`, `help` and `show_payload` are argv-only)"
            )));
        };
        layer.insert(key, tokens(key, &value, &origin)?);
    }
    Ok(layer)
}

fn from_env(env: &[(String, String)]) -> Result<Layer, CliError> {
    let mut layer = Layer::new();
    for (name, value) in env {
        let Some(rest) = name.strip_prefix("INFERTAP_") else {
            continue;
        };
        if rest == "CONFIG" {
            continue;
        }
        let flag_name = rest.to_ascii_lowercase().replace('_', "-");
        let Some(key) = key_of(&flag_name) else {
            return Err(CliError::Usage(format!(
                "{name}: no such option (the names are the flags in --help, upper-cased \
                     with underscores; INFERTAP_KEY is refused: name INFERTAP_KEY_FILE)"
            )));
        };
        let value = if spec::lookup(key).is_some_and(|spec| spec.shape == Shape::Flag) {
            match value.trim() {
                "true" | "1" | "yes" => toml::Value::Boolean(true),
                "false" | "0" | "no" | "" => toml::Value::Boolean(false),
                _ => return Err(CliError::Usage(format!("{name}: must be true or false"))),
            }
        } else {
            toml::Value::String(value.clone())
        };
        layer.insert(key, tokens(key, &value, name)?);
    }
    Ok(layer)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::{Endpoint, KeySource, Opts, Output, SourceKey};
    use std::path::{Path, PathBuf};

    #[test]
    fn malformed_config_does_not_echo_source_text() {
        let error = super::from_toml("tap.toml", "key = \"do-not-print-this-secret").unwrap_err();
        let message = error.to_string();
        assert!(message.contains("invalid TOML"));
        assert!(!message.contains("do-not-print-this-secret"));
    }

    #[test]
    fn rejected_configuration_values_are_redacted() {
        const SECRET: &str = "SYNTHETIC_PRIVATE_VALUE";
        for text in [
            format!("raw_ids = \"{SECRET}\""),
            format!("raw_ids = {{ token = \"{SECRET}\" }}"),
            format!("endpoints = [{{ token = \"{SECRET}\" }}]"),
            format!("segment_secs = {{ token = \"{SECRET}\" }}"),
        ] {
            let error = super::from_toml("tap.toml", &text).unwrap_err().to_string();
            assert!(error.contains("tap.toml"));
            assert!(!error.contains(SECRET), "{error}");
        }
        let env = vec![("INFERTAP_RAW_IDS".into(), SECRET.into())];
        let error = super::from_env(&env).unwrap_err().to_string();
        assert!(error.contains("INFERTAP_RAW_IDS"));
        assert!(!error.contains(SECRET));
        assert!(!format!("{:?}", KeySource::Argv(SECRET.into())).contains(SECRET));
    }

    fn no_env() -> std::iter::Empty<(String, String)> {
        std::iter::empty()
    }
    fn no_file(_: &Path) -> std::io::Result<String> {
        Err(std::io::Error::other("no file in this test"))
    }
    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_file_sets_what_the_command_line_does_not() {
        let file = |_: &Path| -> std::io::Result<String> {
            Ok(r#"
engine_endpoint = ["i0=tcp://10.0.0.1:5557", "i1=tcp://10.0.0.2:5557"]
out_dir = "/var/lib/infertap"
segment_secs = 30
raw_ids = true
"#
            .to_string())
        };
        let o = Opts::parse_with(
            &argv(&["--config", "/etc/infertap/tap.toml"]),
            no_env(),
            &file,
        )
        .expect("parses");
        assert_eq!(o.endpoints.len(), 2);
        assert_eq!(
            o.output,
            Some(Output::Segments(PathBuf::from("/var/lib/infertap")))
        );
        assert_eq!(o.segment_secs, Some(30));
        assert_eq!(o.key, Some(KeySource::Raw));
    }

    #[test]
    fn a_flag_beats_the_environment_which_beats_the_file() {
        let file = |_: &Path| -> std::io::Result<String> {
            Ok(r#"
segment_secs = 30
heartbeat_secs = 10
engine_endpoint = ["file=tcp://1.1.1.1:1"]
raw_ids = true
"#
            .to_string())
        };
        let env = vec![
            ("INFERTAP_CONFIG".to_string(), "tap.toml".to_string()),
            ("INFERTAP_SEGMENT_SECS".to_string(), "20".to_string()),
            (
                "INFERTAP_ENGINE_ENDPOINT".to_string(),
                "env=tcp://2.2.2.2:2".to_string(),
            ),
            ("HOME".to_string(), "/nowhere".to_string()),
        ];
        let o = Opts::parse_with(&argv(&["--heartbeat-secs", "5"]), env.into_iter(), &file)
            .expect("parses");
        assert_eq!(o.segment_secs, Some(20), "environment beats file");
        assert_eq!(o.heartbeat_secs, 5, "flag beats file");
        assert_eq!(
            o.endpoints,
            vec![Endpoint {
                source: SourceKey::from("env"),
                address: "tcp://2.2.2.2:2".to_string()
            }],
            "a list is replaced by a higher layer, never extended"
        );
        assert_eq!(
            o.key,
            Some(KeySource::Raw),
            "an untouched file key still applies"
        );
    }

    #[test]
    fn key_material_is_refused_from_the_file_and_the_environment() {
        let file = |_: &Path| -> std::io::Result<String> { Ok("key = \"deadbeef\"\n".to_string()) };
        let err = Opts::parse_with(&argv(&["--config", "t.toml"]), no_env(), &file)
            .err()
            .expect("refused");
        assert!(err.to_string().contains("argv-only"), "{err}");
        let env = vec![("INFERTAP_KEY".to_string(), "deadbeef".to_string())];
        let err = Opts::parse_with(&argv(&[]), env.into_iter(), &no_file)
            .err()
            .expect("refused");
        assert!(err.to_string().contains("INFERTAP_KEY_FILE"), "{err}");
    }

    #[test]
    fn an_unknown_key_or_a_bad_type_names_the_file_and_the_key() {
        let file = |_: &Path| -> std::io::Result<String> {
            Ok("segment_sec = 30
"
            .to_string())
        };
        let err = Opts::parse_with(&argv(&["--config", "t.toml"]), no_env(), &file)
            .err()
            .expect("refused");
        assert!(err.to_string().contains("segment_sec"), "{err}");
        let file = |_: &Path| -> std::io::Result<String> { Ok("raw_ids = \"yes\"\n".to_string()) };
        let err = Opts::parse_with(&argv(&["--config", "t.toml"]), no_env(), &file)
            .err()
            .expect("refused");
        assert!(err.to_string().contains("true or false"), "{err}");
        let err = Opts::parse_with(&argv(&["--config", "missing.toml"]), no_env(), &no_file)
            .err()
            .expect("refused");
        assert!(err.to_string().contains("missing.toml"), "{err}");
    }

    #[test]
    fn without_a_file_or_environment_the_command_line_is_all_there_is() {
        let o = Opts::parse_with(&argv(&["--raw-ids"]), no_env(), &no_file).expect("parses");
        assert_eq!(o.key, Some(KeySource::Raw));
        assert!(o.endpoints.is_empty());
    }
}
