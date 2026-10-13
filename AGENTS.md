# Coding agents

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Read it first, then [README.md](README.md)
and [docs/trust.md](docs/trust.md). Contributor references are in `docs/design/`.
`CLAUDE.md` is an alias of this file.

- Use one task per commit with a DCO sign-off (`git commit -s`).
- Before every push, run `scripts/preflight.sh` without arguments and check its exit status.
  It checks both feature sets. Report skipped checks accurately.
- After pushing, inspect CI's verdict with `gh run list --limit 1` and
  `gh run view <id> --json jobs`. Local success does not establish CI success.
- Never edit vendored fixtures under `conformance/`. Fix the implementation or update
  the upstream contract and pin.
- Follow the remediation reported by `tests/static_audit.rs`; do not bypass its checks.
- Write short, active sentences about current behavior. Keep useful technical rationale
  in contributor documentation and private working notes outside the repository.
- Submit contributions through pull requests for maintainer review.
