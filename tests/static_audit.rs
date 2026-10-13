// SPDX-License-Identifier: Apache-2.0
//! Source-text checks for lint settings, selected API spellings, and dependency policy.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn unsafe_is_forbidden_at_the_crate_level() {
    let cargo = fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("Cargo.toml");
    assert!(
        cargo.contains("unsafe_code = \"forbid\""),
        "Cargo.toml must keep [lints.rust] unsafe_code = \"forbid\""
    );
    // Check for explicit attempts to re-enable unsafe code.
    let mut sources = Vec::new();
    rust_sources(&manifest_dir().join("src"), &mut sources);
    for path in &sources {
        let text = fs::read_to_string(path).expect("read source");
        assert!(
            !text.contains("allow(unsafe_code)"),
            "{} re-allows unsafe_code",
            path.display()
        );
    }
}

#[test]
fn no_listeners_and_no_unsanctioned_outbound_in_the_source_tree() {
    // Check selected socket API spellings and keep direct ZMQ calls in transport.rs.
    // This source-text check does not analyze aliases or dependency behavior.
    let mut sources = Vec::new();
    rust_sources(&manifest_dir().join("src"), &mut sources);
    for path in &sources {
        let text = fs::read_to_string(path).expect("read source");
        for forbidden in ["TcpListener", "UdpSocket", "TcpStream", "std::net::"] {
            assert!(
                !text.contains(forbidden),
                "{} contains {forbidden:?}",
                path.display()
            );
        }
        if !path.ends_with("transport.rs") {
            assert!(
                !text.contains("zmq::"),
                "{} touches the socket layer outside the transport seam",
                path.display()
            );
        }
    }
}

#[test]
fn dependency_set_is_the_reviewed_allowlist() {
    // Record the purpose of each direct dependency. Additions require review and an
    // allowlist update. The list does not constrain transitive dependency behavior.
    let cargo = fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("Cargo.toml");
    let deps: Vec<String> = cargo
        .lines()
        .skip_while(|l| l.trim() != "[dependencies]")
        .skip(1)
        .take_while(|l| !l.trim().starts_with('['))
        .filter_map(|l| l.split('=').next())
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
        .map(str::to_string)
        .collect();
    let expected = [
        "inference-events", // shared wire contract and generated column schema
        "serde",            // record/value model
        "serde_json",       // the record format
        "sha2",             // HMAC-SHA256 pseudonyms
        "hmac",             // HMAC-SHA256 pseudonyms
        "base64",           // key decoding
        "hex",              // key/pseudonym encoding
        "rmp-serde",        // vLLM msgpack wire decode
        "signal-hook",      // SIGTERM/SIGINT flag registration for the agent_stop record
        "zmq",              // the SUB socket, optional, feature-gated
        "parquet", // the shipped-form encoder, default features off: no arrow, no C toolchain
        "toml",    // the --config file, parse only: the flags stay the single definition
        "log",     // the logging facade: the library logs, the binary formats, nothing prints
        "fs2",     // process-held exclusive segment-directory locks on Rust 1.85
    ];
    assert_eq!(
        deps, expected,
        "dependency set changed; review before allowing"
    );
}

#[test]
fn no_shell_outs_or_dynamic_exec_in_the_source_tree() {
    let mut sources = Vec::new();
    rust_sources(&manifest_dir().join("src"), &mut sources);
    assert!(!sources.is_empty());
    // Flag these direct process and syscall API spellings for review. As with the
    // socket check, aliases and dependency behavior are outside this check's scope.
    for path in &sources {
        let text = fs::read_to_string(path).expect("read source");
        for forbidden in ["process::Command", "libc::"] {
            assert!(
                !text.contains(forbidden),
                "{} contains {forbidden:?}",
                path.display()
            );
        }
    }
}

/// The engine-side flag is stated in four shipped places and must read identically in
/// all of them. Normalize whitespace because YAML examples can wrap the JSON string.
#[test]
fn the_engine_flag_is_stated_identically_everywhere() {
    let root = manifest_dir();
    let files = [
        "README.md",
        "docs/onboarding.md",
        "packaging/kubernetes/README.md",
        "packaging/kubernetes/example-vllm-deployment.yaml",
    ];
    let mut seen: Vec<(String, String)> = Vec::new();
    for f in files {
        let text = fs::read_to_string(root.join(f)).expect("read the file");
        let start = text
            .find("{\"enable_kv_cache_events\"")
            .unwrap_or_else(|| panic!("{f} does not state the engine flag"));
        let end = text[start..]
            .find('}')
            .map(|i| start + i + 1)
            .expect("the flag's JSON closes");
        let normalized: String = text[start..end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        seen.push((f.to_string(), normalized));
    }
    let (first_file, first) = &seen[0];
    for (f, s) in &seen[1..] {
        assert_eq!(
            s, first,
            "{f} states the engine flag differently from {first_file}"
        );
    }
    assert!(
        first.contains("\"publisher\": \"zmq\"") && first.contains("\"replay_endpoint\""),
        "the stated flag must name the publisher and the replay endpoint: {first}"
    );
}
