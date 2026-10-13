#!/usr/bin/env bash
# Every non-merge commit in a range carries a Developer Certificate of Origin sign-off:
#     Signed-off-by: Name <email>
# Usage: scripts/dco-check.sh [BASE] [HEAD]
# With no BASE (or the all-zero sha GitHub sends for a new branch) only HEAD is checked.
# Judged by exit code, run by CI's `dco` job. Lists every offender, then fails once.
set -euo pipefail
cd "$(dirname "$0")/.."
base="${1:-}"
head="${2:-HEAD}"
# Resolve each endpoint as a commit before constructing the revision expression.
head="$(git rev-parse --verify --end-of-options "$head^{commit}")" || exit 1
if [ -z "$base" ] || [ "$base" = "0000000000000000000000000000000000000000" ]; then
  range="$head^!"
else
  base="$(git rev-parse --verify --end-of-options "$base^{commit}")" || exit 1
  range="$base..$head"
fi
# Process substitution would hide a failed enumeration from the loop's exit status.
commits="$(git rev-list --no-merges "$range")" || exit 1
missing=0
n=0
while IFS= read -r sha; do
  [ -n "$sha" ] || continue
  n=$((n + 1))
  if ! git log -1 --format=%B "$sha" | grep -qE '^Signed-off-by: .+ <[^>]+@[^>]+>$'; then
    echo "dco-check: $(git log -1 --format='%h %s' "$sha")" >&2
    echo "           lacks a 'Signed-off-by: Name <email>' line (git commit -s)" >&2
    missing=1
  fi
done <<< "$commits"
[ "$missing" -eq 0 ] && echo "dco-check: all $n commits in $range are signed off"
exit "$missing"
