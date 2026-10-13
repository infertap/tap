// SPDX-License-Identifier: Apache-2.0
//! Kubernetes instance identity and event-port isolation.
//!
//! Inspect manifest text and execute the identity-building shell command with a
//! stub binary to verify that separate pods receive distinct instance labels.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use infertap::testing::repo_root as root;
use std::fs;
use std::process::Command;

fn manifest(name: &str) -> String {
    fs::read_to_string(root().join("packaging/kubernetes").join(name)).expect("read the manifest")
}

/// The tap container's shell line out of the sidecar patch: the block under `args:` /
/// `- |`, de-indented.
fn tap_command() -> String {
    let text = manifest("tap-sidecar.yaml");
    let start = text
        .find("        - name: tap\n")
        .expect("the tap container");
    let mut lines = text[start..].lines();
    lines
        .by_ref()
        .find(|l| l.trim_end() == "            - |")
        .expect("the tap's command is a literal block");
    let mut out = String::new();
    for l in lines {
        let Some(body) = l.strip_prefix("              ") else {
            break;
        };
        out.push_str(body);
        out.push('\n');
    }
    assert!(out.contains("exec /usr/local/bin/infertap"), "{out}");
    out
}

/// Run the tap's shell line for one pod, with a stub in the binary's place that prints
/// its arguments one per line. Returns the arguments the tap would have been started with.
fn tap_args_for(pod: &str, namespace: &str, cluster: Option<&str>) -> Vec<String> {
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let run = RUNS.fetch_add(1, Ordering::Relaxed);
    let dir = infertap::testing::scratch(&format!("k8s-identity-{run}"));
    let stub = dir.join("infertap");
    fs::write(
        &stub,
        "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done\n",
    )
    .unwrap();
    fs::set_permissions(&stub, fs::Permissions::from_mode(0o755)).unwrap();
    let mut script = tap_command();
    script = script.replace("/usr/local/bin/infertap", stub.to_str().unwrap());
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(&script)
        .env("POD_NAME", pod)
        .env("POD_NAMESPACE", namespace)
        .env("POD_UID", format!("uid-of-{pod}"))
        .env_remove("CLUSTER_ID");
    if let Some(c) = cluster {
        cmd.env("CLUSTER_ID", c);
    }
    let out = cmd.output().expect("sh runs");
    let _ = fs::remove_dir_all(&dir);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

fn value_after<'a>(args: &'a [String], flag: &str) -> &'a str {
    let i = args
        .iter()
        .position(|a| a == flag)
        .unwrap_or_else(|| panic!("{flag} is passed"));
    &args[i + 1]
}

/// Two replicas of one Deployment carry two instance identities: the identity is the
/// pod's, not the engine's stable name, so scaling never merges engines. Both the live
/// and the replay endpoint carry it, and the pod UID rides in provenance.
#[test]
fn two_replicas_are_two_instances() {
    let a = tap_args_for("vllm-7c9d-abcde", "serving", None);
    let b = tap_args_for("vllm-7c9d-fghij", "serving", None);
    assert_eq!(
        value_after(&a, "--engine-endpoint"),
        "serving/vllm-7c9d-abcde=tcp://127.0.0.1:5557"
    );
    assert_eq!(
        value_after(&b, "--engine-endpoint"),
        "serving/vllm-7c9d-fghij=tcp://127.0.0.1:5557"
    );
    assert_eq!(
        value_after(&a, "--engine-replay-endpoint"),
        "serving/vllm-7c9d-abcde=tcp://127.0.0.1:5657"
    );
    let provenance: serde_json::Value =
        serde_json::from_str(value_after(&a, "--provenance")).expect("the provenance is JSON");
    assert_eq!(provenance["pod_identity"], "vllm-7c9d-abcde");
    assert_eq!(provenance["pod_uid"], "uid-of-vllm-7c9d-abcde");
    // The namespace is part of the identity: the same pod name in two namespaces is two
    // instances.
    let c = tap_args_for("vllm-7c9d-abcde", "staging", None);
    assert_ne!(
        value_after(&a, "--engine-endpoint"),
        value_after(&c, "--engine-endpoint")
    );
}

/// The cluster prefixes the identity when set and is absent otherwise: one cluster is
/// the whole domain by default, and two clusters sharing a domain are told apart by name.
#[test]
fn the_cluster_prefix_is_optional_and_leads_when_set() {
    let bare = tap_args_for("vllm-0", "serving", None);
    let named = tap_args_for("vllm-0", "serving", Some("eu-west-1"));
    let empty = tap_args_for("vllm-0", "serving", Some(""));
    assert_eq!(
        value_after(&bare, "--engine-endpoint"),
        "serving/vllm-0=tcp://127.0.0.1:5557"
    );
    assert_eq!(
        value_after(&named, "--engine-endpoint"),
        "eu-west-1/serving/vllm-0=tcp://127.0.0.1:5557"
    );
    assert_eq!(
        value_after(&empty, "--engine-endpoint"),
        value_after(&bare, "--engine-endpoint"),
        "an empty cluster key is the same as none"
    );
}

/// A tap container restart is a new incarnation of the same instance: the identity is
/// composed from the pod, which a container restart does not change, and the segment
/// sink mints a fresh incarnation per open. Records from both incarnations carry one
/// `instance_id`; their segments carry two incarnations.
#[test]
fn a_tap_restart_is_a_new_incarnation_of_the_same_instance() {
    use infertap::record::{InstanceLabel, Provenance, Record};
    use infertap::sink::segments::{SegmentConfig, SegmentedSink};
    use infertap::sink::Sink as _;
    let first = tap_args_for("vllm-0", "serving", None);
    let again = tap_args_for("vllm-0", "serving", None);
    assert_eq!(
        value_after(&first, "--engine-endpoint"),
        value_after(&again, "--engine-endpoint")
    );
    let label = value_after(&first, "--engine-endpoint")
        .split_once('=')
        .unwrap()
        .0;
    let instance = InstanceLabel::from(label);

    let dir = infertap::testing::scratch("k8s-restart");
    let cfg = || SegmentConfig {
        dir: dir.clone(),
        max_bytes: 1 << 20,
        max_secs: 3600,
        max_disk_bytes: None,
        workload_class: None,
        heartbeat_secs: 60,
        key_epoch: None,
    };
    let mut incarnations = Vec::new();
    for _ in 0..2 {
        let mut sink =
            SegmentedSink::open_write_ahead_only(cfg(), Provenance::default()).expect("open");
        incarnations.push(sink.incarnation().to_string());
        let mut store = infertap::testing::store(1);
        store.instance_id = instance.clone();
        sink.emit(Record::Store(store));
        sink.close().expect("close");
    }
    assert_ne!(
        incarnations[0], incarnations[1],
        "each open is a new incarnation"
    );
    let mut sealed: Vec<String> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("seg-"))
        .collect();
    sealed.sort();
    assert_eq!(sealed.len(), 2);
    for name in &sealed {
        let text = fs::read_to_string(dir.join(name)).unwrap();
        let store: serde_json::Value = serde_json::from_str(text.lines().nth(1).unwrap()).unwrap();
        assert_eq!(store["instance_id"], label, "{name}");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// The `ConfigMap` carries no engine endpoint: a fixed label there was how every replica
/// became `vllm`, and a list in the file would now be overridden by the flag silently.
#[test]
fn the_config_map_names_no_engine_and_carries_the_cluster_key() {
    let text = manifest("tap-config.yaml");
    let toml = text
        .split("tap.toml: |")
        .nth(1)
        .unwrap()
        .split("\n---")
        .next()
        .unwrap();
    for line in toml.lines().map(str::trim) {
        assert!(
            !line.starts_with("engine_endpoint") && !line.starts_with("engine_replay_endpoint"),
            "tap.toml in the ConfigMap must not fix an engine label: {line}"
        );
    }
    assert!(
        text.contains("\n  cluster: \"\""),
        "the ConfigMap declares the optional cluster key"
    );
}

/// The network policy the kustomization applies selects the example's engine pods and
/// admits ingress to the serving port alone: the publisher's ports are named nowhere in
/// it, so nothing but the pod's own loopback reaches them.
#[test]
fn the_network_policy_keeps_the_publisher_in_the_pod() {
    let kustomization = manifest("kustomization.yaml");
    assert!(
        kustomization.contains("  - network-policy.yaml\n"),
        "the kustomization applies the policy"
    );
    let policy = manifest("network-policy.yaml");
    let example = manifest("example-vllm-deployment.yaml");
    let labels = example
        .lines()
        .find(|l| l.trim().starts_with("labels:"))
        .expect("the example labels its pods")
        .trim()
        .trim_start_matches("labels:")
        .trim()
        .to_string();
    assert!(
        policy.contains(&format!("matchLabels: {labels}")),
        "the policy selects the example's pods ({labels})"
    );
    assert!(policy.contains("policyTypes: [Ingress]"));
    let spec = policy.split("spec:").nth(1).unwrap();
    let serving = example
        .lines()
        .find(|l| l.trim().starts_with("containerPort:"))
        .unwrap()
        .trim()
        .trim_start_matches("containerPort:")
        .trim();
    assert!(
        spec.contains(&format!("port: {serving}")),
        "the serving port ({serving}) is admitted"
    );
    for publisher_port in ["5557", "5657"] {
        assert!(
            !spec.contains(publisher_port),
            "the policy must not admit the publisher's port {publisher_port}"
        );
    }
}

/// The three documents that state the engine flag state the isolation requirement beside
/// it, each naming the artifact for its platform.
#[test]
fn the_isolation_requirement_stands_beside_the_flag() {
    for (file, artifact) in [
        ("README.md", "packaging/systemd/kv-events.nft"),
        ("docs/onboarding.md", "packaging/systemd/kv-events.nft"),
        ("packaging/kubernetes/README.md", "network-policy.yaml"),
    ] {
        let text = fs::read_to_string(root().join(file)).unwrap();
        let flag = text.find("enable_kv_cache_events").unwrap();
        let window = &text[flag.saturating_sub(1200)..(flag + 1200).min(text.len())];
        assert!(
            window.contains(artifact),
            "{file} states the flag without the isolation artifact {artifact} beside it"
        );
    }
    let nft = fs::read_to_string(root().join("packaging/systemd/kv-events.nft")).unwrap();
    assert!(nft.contains("iifname \"lo\" tcp dport { 5557, 5657 } accept"));
    assert!(nft.contains("tcp dport { 5557, 5657 } drop"));
}
