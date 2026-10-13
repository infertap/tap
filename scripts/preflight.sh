#!/usr/bin/env bash
# Local release-readiness checks. A successful run does not establish CI success.
# CI also checks MSRV, DCO, the upstream spec pin, advisories, dependency policy,
# container behavior, and SBOM generation. The local E2E step requires pyzmq.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
step() {
  printf '%-46s' "$1"; shift
  if "$@" >/tmp/preflight.$$ 2>&1; then
    echo "ok"
  else
    echo "FAILED"; fail=1
    sed -n '1,25p' /tmp/preflight.$$ | sed 's/^/    /'
    # The head of a test run is its passing tests; the verdict lines are what the reader
    # needs, wherever they fell.
    grep -nE 'FAILED|panicked at|^error' /tmp/preflight.$$ | head -20 | sed 's/^/    /'
  fi
  rm -f /tmp/preflight.$$
}

step "spdx headers"                           scripts/spdx-check.sh
step "nothing shipped cites the left-behind"   scripts/citation-check.sh
step "capture present (fetched if absent)"    sh -c '[ -d capture ] || scripts/fetch-capture.sh'
step "shared code checks (both feature sets)" scripts/check-code.sh
# The live-transport end to end, where pyzmq is present; absent pyzmq is reported, not hidden.
if python3 -c 'import zmq' 2>/dev/null; then
  step "e2e: stub publisher -> sealed parquet"  sh -c 'cargo build --features zmq --quiet && scripts/e2e-stub.sh target/debug/infertap'
else
  printf '%-46s%s\n' "e2e: stub publisher -> sealed parquet" "skipped (no pyzmq; CI runs it)"
fi

echo
[ "$fail" -eq 0 ] && echo "preflight: local checks passed; verify CI after pushing" || echo "preflight: RED, do not push"
exit "$fail"
