#!/usr/bin/env bash
# Reject references from shipped files to excluded or gitignored working notes.
# The cut manifest is absent in the public snapshot; that case needs no check.
# The manifest, .gitignore, generated notices, and this script are exempt.
set -euo pipefail
cd "$(dirname "$0")/.."
manifest=scripts/public-cut.manifest
if [ ! -f "$manifest" ]; then
  echo "citation-check: no cut manifest; nothing stays behind"
  exit 0
fi
left_behind=()
while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  left_behind+=("$line")
done < "$manifest"
never_cited=("${left_behind[@]}")
while IFS= read -r note; do never_cited+=("$note"); done < <(grep -E '^docs/(design/)?[A-Za-z0-9_-]+\.md$' .gitignore)

shipping_files() {
  git ls-files -z | while IFS= read -r -d '' f; do
    for skip in "${left_behind[@]}"; do
      [ "$f" = "$skip" ] && continue 2
    done
    printf '%s\0' "$f"
  done
}
cited=0
for skip in "${never_cited[@]}"; do
  base="$(basename "$skip")"
  if shipping_files | xargs -0 grep -Il -- "$base" 2>/dev/null | grep -Ev '^(THIRD-PARTY-NOTICES|\.gitignore|scripts/citation-check\.sh)$' | grep -q .; then
    echo "citation-check: $skip is left behind but still cited by:" >&2
    shipping_files | xargs -0 grep -In -- "$base" 2>/dev/null | grep -Ev '^(THIRD-PARTY-NOTICES|\.gitignore|scripts/citation-check\.sh):' | sed 's/^/    /' >&2
    cited=1
  fi
done
[ "$cited" -eq 0 ] && echo "citation-check: nothing that ships cites what stays behind"
exit "$cited"
