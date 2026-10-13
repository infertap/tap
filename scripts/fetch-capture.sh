#!/usr/bin/env bash
# Fetch the vLLM 0.26.0 capture the adapter and the corpora were derived from, verify it
# against its pinned digest, and unpack it into capture/ (gitignored). The capture lives in
# the spec repository's tree and as an asset on the corpus tag's release; this repository
# cites it and does not carry it. Judged by exit code.
set -euo pipefail
cd "$(dirname "$0")/.."
TAG=v1-draft.22
ASSET=vllm-0.26.0-capture-2026-07-27.tar.gz
SHA256=62abe893ea8eda1f1c8b855e1a22a20c0c7ce9f1a6c2b1b7545404973cb3ba85
URL="https://github.com/infertap/inference-events/releases/download/$TAG/$ASSET"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
curl -sSfL "$URL" -o "$tmp/$ASSET"
echo "$SHA256  $tmp/$ASSET" | shasum -a 256 -c - >/dev/null
tar -C . -xzf "$tmp/$ASSET"
echo "fetch-capture: capture/ unpacked and verified ($ASSET, sha256 $SHA256)"
