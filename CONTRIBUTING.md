# Contributing

Build, test, and submit changes to the tap. See the [documentation index](docs/README.md)
for operator guides and contributor references.

## Build

Install Rust through rustup and a C++ compiler. The repository pins its development
toolchain in `rust-toolchain.toml`; `Cargo.toml` declares the minimum supported Rust version.

```sh
cargo build --release --features zmq --locked
```

The live-transport feature compiles and statically links vendored libzmq. No separate
libzmq package is required. A build without the feature supports offline processing and
sample output.

## Test

Run tests as an unprivileged user so file-permission checks can exercise access denial.
Install Git for the DCO integration test and pyzmq for the live E2E test.

```sh
scripts/fetch-capture.sh
scripts/preflight.sh
```

Preflight checks source headers, formatting, clippy, rustdoc, both test feature sets,
and the release build. It runs the standalone live E2E test when pyzmq is installed;
otherwise it reports that step as skipped. Use its exit status as the local result.

CI additionally checks notices, the MSRV, DCO sign-offs, the upstream specification pin,
advisories, dependency policy, container behavior, and SBOM generation. The E2E test
always runs in CI. After pushing, check CI separately:

```sh
gh run list --limit 1
gh run view <id> --json jobs -q '.jobs[]|"\(.conclusion) \(.name)"'
```

Fixtures under `conformance/` are vendored from the revision in `SPEC_PIN`. Fix conformance
failures in the implementation, or change the upstream specification and update the pin.
Never edit the fixtures locally. See [Conformance](docs/conformance-notes.md).

For deployment checks, use [Release acceptance](docs/release-acceptance.md) and the
[packaging smoke procedure](packaging/smoke/runbook.md). [Validation reports](docs/validation.md)
record previous workloads and results.

## Dependencies

The dependency set is an allowlist that `tests/static_audit.rs` pins. Dependency review
weighs correctness and maintenance benefits against added code and supply-chain exposure.
See [Dependencies](docs/design/design-log.md#dependencies-cargotoml-testsstatic_auditrs)
for the current choices, including the argument parser, logger, and error types. A new
dependency needs a one-sentence justification in the allowlist, a `deny.toml` review, and
regenerated `THIRD-PARTY-NOTICES`.

## Code and documentation style

Use the configured formatter and linter. Keep modules focused on one responsibility,
share repeated behavior through small helpers, and use types to distinguish states and
units. Tests should verify behavior with independently derived expectations.

Write short sentences in active voice. Describe current behavior, configuration, and
constraints. Comments should explain ownership, invariants, and non-obvious choices.
Use one term per concept and qualify guarantees with their actual scope.

Keep test histories and results in validation reports. Keep private operational details,
internal task journals, and commercial planning out of the repository. Avoid promotional
claims and rhetorical judgments in technical documentation.

## The Developer Certificate of Origin

This repository is under the Apache-2.0 licence and accepts contributions under the Developer
Certificate of Origin. There is no contributor licence agreement to sign. You keep your
copyright; the sign-off is your certification that you may submit the work under the
project's licence.

> Developer Certificate of Origin
> Version 1.1
>
> Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
>
> Everyone is permitted to copy and distribute verbatim copies of this
> license document, but changing it is not allowed.
>
> Developer's Certificate of Origin 1.1
>
> By making a contribution to this project, I certify that:
>
> (a) The contribution was created in whole or in part by me and I
>     have the right to submit it under the open source license
>     indicated in the file; or
>
> (b) The contribution is based upon previous work that, to the best
>     of my knowledge, is covered under an appropriate open source
>     license and I have the right under that license to submit that
>     work with modifications, whether created in whole or in part
>     by me, under the same open source license (unless I am
>     permitted to submit under a different license), as indicated
>     in the file; or
>
> (c) The contribution was provided directly to me by some other
>     person who certified (a), (b) or (c) and I have not modified
>     it.
>
> (d) I understand and agree that this project and the contribution
>     are public and that a record of the contribution (including all
>     personal information I submit with it, including my sign-off) is
>     maintained indefinitely and may be redistributed consistent with
>     this project or the open source license(s) involved.

To certify, add a line to every commit message:

    Signed-off-by: Your Name <you@example.com>

`git commit -s` adds it from your git identity. Use your real name and a working address.
CI's `dco` job checks every non-merge commit a push or pull request brings and fails on one
without the line; `scripts/dco-check.sh main HEAD` runs the same check locally. A pull
request with an unsigned commit is held until the commit is amended.

## Submit a change

- Use one task per commit and add a DCO sign-off with `git commit -s`.
- Run `scripts/preflight.sh` before every push and report any skipped checks in the PR.
- Describe the problem, resulting behavior, compatibility impact, and relevant validation.
- Update documentation with the change. Preserve the public record contract unless its
  upstream revision changes.
- Keep the SPDX licence header on Rust files.
- Resolve failures reported by the static audit directly; do not bypass the checks.
- Obtain maintainer review before merging.
