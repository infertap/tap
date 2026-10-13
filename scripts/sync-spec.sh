#!/usr/bin/env bash
# Vendor conformance/ from the commit SPEC_PIN names, verifying everything on the way:
# the tag must resolve to the pinned commit, the upstream blessing must be current, and
# the blessing must equal the pin. Run after editing SPEC_PIN to bump; never edit
# vendored fixtures directly.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
. ./SPEC_PIN

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git clone --quiet --depth 1 --branch "$tag" "$repo" "$tmp"
got=$(git -C "$tmp" rev-parse HEAD)
if [ "$got" != "$commit" ]; then
    echo "tag $tag resolves to $got but SPEC_PIN says $commit" >&2
    exit 1
fi
(cd "$tmp" && python3 tools/contract-hash.py --check)
if [ "$(cat "$tmp/conformance/CONTRACT_HASH")" != "$corpus_sha256" ]; then
    echo "upstream blessing disagrees with SPEC_PIN corpus_sha256" >&2
    exit 1
fi
rsync -a --delete "$tmp/conformance/" conformance/
mkdir -p vendor/inference-events
rsync -a --delete "$tmp/crates/model/" vendor/inference-events/
echo "vendored conformance/ from $tag ($commit)"
