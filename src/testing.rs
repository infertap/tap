// SPDX-License-Identifier: Apache-2.0
//! Test support: the in-memory transport, a scripted source, scratch directories and the
//! fixture loaders this crate's own tests and examples share. Behind the `testing`
//! feature, which the crate's dev-dependency on itself enables for every test and example
//! target and which a release build never carries. Nothing here is API.

// Test support opts out of the panic lints the way every test module does: a helper that
// cannot fail loudly is not a test helper.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

use crate::source::wire::{FrameTransport, Message};
use crate::source::{EventSource, SourceError, SourceKey, SourceStats, Sourced};

/// Canned multipart messages, served in order, then idle: the whole vLLM pipeline above
/// the socket runs on real wire bytes with no engine.
#[derive(Debug, Default)]
pub struct QueueTransport(pub VecDeque<Message>);

impl QueueTransport {
    /// A transport that serves `messages` in order.
    #[must_use]
    pub fn new(messages: Vec<Message>) -> Self {
        Self(messages.into())
    }
}

impl FrameTransport for QueueTransport {
    fn poll(&mut self, _timeout: Duration) -> io::Result<Option<Message>> {
        Ok(self.0.pop_front())
    }
}

/// One publisher frame as vLLM sends it: topic, big-endian sequence, msgpack payload.
#[must_use]
pub fn frame(source: &str, seq: u64, payload: Vec<u8>) -> Message {
    Message {
        source: SourceKey::from(source),
        parts: vec![b"kv-events".to_vec(), seq.to_be_bytes().to_vec(), payload],
    }
}

/// One step of a [`ScriptedSource`]: what its next poll yields.
#[derive(Debug)]
pub enum Step {
    /// Events buffered, each with its source and message sequence.
    Events(Vec<Sourced>),
    /// A skippable error: one message the adapter could not read.
    Skippable(String),
    /// A fatal transport error of the given kind.
    Fatal(io::ErrorKind, String),
}

/// An [`EventSource`] driven by a script, for the run loop's tests: each poll takes the
/// next step, and past the end the last step repeats forever or the source reports idle.
#[derive(Debug, Default)]
pub struct ScriptedSource {
    steps: Vec<Step>,
    cursor: usize,
    repeat_last: bool,
    pending: VecDeque<Sourced>,
    /// Polls so far; doubles as `messages_seen`.
    pub polls: u64,
}

impl ScriptedSource {
    /// A source that follows `steps` and then reports idle.
    #[must_use]
    pub fn new(steps: Vec<Step>) -> Self {
        Self {
            steps,
            ..Self::default()
        }
    }

    /// Past the end of the script, repeat its last step forever.
    #[must_use]
    pub fn repeating_last(mut self) -> Self {
        self.repeat_last = true;
        self
    }

    fn next_step(&mut self) -> Option<&Step> {
        if self.cursor < self.steps.len() {
            self.cursor += 1;
            return self.steps.get(self.cursor - 1);
        }
        if self.repeat_last {
            return self.steps.last();
        }
        None
    }
}

impl EventSource for ScriptedSource {
    fn poll(&mut self) -> Result<usize, SourceError> {
        self.polls += 1;
        let Some(step) = self.next_step() else {
            return Ok(0);
        };
        match step {
            Step::Events(events) => {
                let events = events.clone();
                let n = events.len();
                self.pending.extend(events);
                Ok(n)
            }
            Step::Skippable(why) => Err(SourceError::Message {
                stage: crate::source::MessageStage::Decode,
                source: Box::new(io::Error::other(why.clone())),
            }),
            Step::Fatal(kind, why) => {
                Err(SourceError::Transport(io::Error::new(*kind, why.clone())))
            }
        }
    }
    fn next_event(&mut self) -> Option<Sourced> {
        self.pending.pop_front()
    }
    fn topic_for(&self, _source: &SourceKey) -> Option<&str> {
        None
    }
    fn reuse_reporting(&self) -> crate::record::ReuseReporting {
        crate::record::ReuseReporting::Unlabelled
    }
    fn stats(&self) -> SourceStats {
        SourceStats {
            messages_seen: self.polls,
            ..SourceStats::default()
        }
    }
    fn seen(&self, _: &SourceKey) -> u64 {
        0
    }
    fn dropped(&self, _: &SourceKey) -> u64 {
        0
    }
}

/// A fresh, empty directory under the system temp directory, unique to this process and
/// `tag`. The caller removes it.
///
/// # Panics
/// If the directory cannot be created.
#[must_use]
pub fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("infertap-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create the scratch directory");
    dir
}

/// Read JSONL test output with file and line context on failure.
///
/// # Panics
/// Panics if the file cannot be read or a line is invalid JSON.
#[must_use]
pub fn jsonl_records(path: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()))
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("{}:{}: {error}", path.display(), index + 1))
        })
        .collect()
}

/// The repository root, for fixture paths.
#[must_use]
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The sidecar shipper's `ship.sh` out of `packaging/kubernetes/tap-config.yaml`: the
/// block under `ship.sh: |`, de-indented. Read as text rather than parsed as YAML, so no
/// dependency joins the tree for a test; the block is the lines after the key that carry
/// the block's indentation.
///
/// # Panics
/// If the `ConfigMap` or the block is missing.
#[must_use]
pub fn sidecar_ship_script() -> String {
    let yaml = std::fs::read_to_string(repo_root().join("packaging/kubernetes/tap-config.yaml"))
        .expect("read tap-config.yaml");
    let mut lines = yaml.lines();
    lines
        .by_ref()
        .find(|l| l.trim_end() == "  ship.sh: |")
        .expect("tap-config.yaml carries the shipper script under ship.sh");
    let mut out = String::new();
    for l in lines {
        if l.is_empty() {
            out.push('\n');
            continue;
        }
        let Some(body) = l.strip_prefix("    ") else {
            break; // the block ends at the first line that is not indented under the key
        };
        out.push_str(body);
        out.push('\n');
    }
    assert!(!out.trim().is_empty(), "the ship.sh block is empty");
    out
}

/// A wire fixture from `conformance/<corpus>` (`vllm-wire`, `sglang-wire`): its JSON and
/// its decoded payload bytes.
///
/// # Panics
/// If the fixture is missing, is not JSON, or carries no `wire_hex`.
#[must_use]
pub fn wire_fixture(corpus: &str, name: &str) -> (Value, Vec<u8>) {
    let path = repo_root().join("conformance").join(corpus).join(name);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let fx: Value = serde_json::from_str(&text).expect("the fixture is JSON");
    let bytes = hex::decode(fx["wire_hex"].as_str().expect("wire_hex")).expect("hex");
    (fx, bytes)
}

/// A `store` record with every optional field absent: block `block`, instance `i0`, rank
/// and group 0, 16 tokens on the GPU tier, at 1 ms. Tests set the fields they care about.
#[must_use]
pub fn store(block: u64) -> crate::record::Store {
    crate::record::Store {
        instance_id: crate::record::InstanceLabel::from("i0"),
        at_ms: crate::clock::EngineMs(1.0),
        seq: None,
        dp_rank: 0,
        group_idx: 0,
        block_id: crate::record::Id(block),
        parent_id: None,
        parent_unknown: false,
        n_tokens: 16,
        tier: Some("GPU".into()),
        extra_keys: None,
        lora_id: None,
        lora_name: None,
        spec_kind: None,
        spec_sliding_window: None,
        reused: None,
        locality: None,
        backend_id: None,
        content_id: None,
        epoch: None,
    }
}

/// [`store`] as a [`Record`](crate::record::Record).
#[must_use]
pub fn store_record(block: u64) -> crate::record::Record {
    crate::record::Record::Store(store(block))
}
