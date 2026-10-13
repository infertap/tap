#!/usr/bin/env bash
# Code checks shared by preflight, CI, and releases. Requires the capture fixtures.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
cargo run --locked --quiet --example replay_corpus >/dev/null
cargo test --locked
cargo test --all-features --locked
cargo build --release --locked
