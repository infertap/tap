#!/usr/bin/env python3
"""Generate THIRD-PARTY-NOTICES for the shipped infertap binary.

**Hand-rolled.** The alternative is a dependency whose job is to read the dependencies,
which sits badly in a repository with a dependency allowlist. `cargo metadata` already
provides the graph as JSON.

**What it covers.** The closure reachable from the `infertap` binary through normal and
build edges, resolved with the shipped feature set (`zmq`, which the release builds
with and which pulls the zmq crates in). Dev-dependencies are excluded because they are
not redistributed. Build-dependencies are included even where they contribute no linked
code, because over-attributing costs a paragraph and under-attributing is the failure this
file exists to prevent.

**One component no graph walk finds, named explicitly:** libzmq itself. `zeromq-src`
vendors the C sources and `zmq-sys` compiles and statically links them into every shipped
binary. That is redistribution of the C library, whose licence is its own and not the
wrapper crate's. Its notice is written here by hand.

Usage:  scripts/third-party-notices.py [--check] > THIRD-PARTY-NOTICES
        --check compares against the committed file and exits 1 on drift.
"""

import json
import subprocess
import sys
from pathlib import Path

# Files a crate conventionally puts its licence text in. Ordered so the canonical name wins
# when a crate ships several; `NOTICE` last because Apache-2.0 makes it supplementary.
LICENCE_FILES = ("LICENSE", "LICENCE", "COPYING", "UNLICENSE", "NOTICE")

# Compiled from vendored source and statically linked: redistribution, and not something
# the crate-graph walk below attributes, because the licence belongs to the C library, not
# to the Rust wrapper crates that carry it.
LIBZMQ_NOTICE = """\
ZeroMQ (libzmq)
    Version:    4.3.4 (vendored by zeromq-src, compiled and statically linked by zmq-sys)
    Licence:    LGPL-3.0-or-later WITH the ZeroMQ static-linking exception
    Homepage:   https://zeromq.org
    Source:     https://github.com/zeromq/libzmq

    Compiled from the vendored sources and statically linked into the shipped binary. The
    ZeroMQ licence grants an explicit static-linking exception, which is what permits this
    form of distribution; the full text rides in the vendored tree (COPYING and
    COPYING.LESSER inside the zeromq-src crate). Built without libsodium; the bundled
    tweetnacl (public domain) provides the curve implementation.
"""


def metadata() -> dict:
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--features", "zmq"],
        capture_output=True,
        check=True,
        text=True,
    )
    return json.loads(out.stdout)


def shipped_closure(meta: dict) -> list[dict]:
    """Every third-party package the infertap binary's build reaches."""
    by_id = {p["id"]: p for p in meta["packages"]}
    workspace = set(meta["workspace_members"])
    edges = {n["id"]: n for n in meta["resolve"]["nodes"]}

    root = next((i for i in workspace if by_id[i]["name"] == "infertap"), None)
    if root is None:
        sys.exit("no infertap package in this workspace")

    seen: set[str] = set()
    queue = [root]
    while queue:
        current = queue.pop()
        if current in seen:
            continue
        seen.add(current)
        for dep in edges[current]["deps"]:
            # `kind: null` is a normal dependency; "build" ships generated code; "dev" does
            # not ship at all. An empty `dep_kinds` predates the field; treat it as normal.
            kinds = {k.get("kind") for k in dep.get("dep_kinds", [])} or {None}
            if kinds & {None, "build"}:
                queue.append(dep["pkg"])

    third_party = [by_id[i] for i in seen if i not in workspace]
    return sorted(third_party, key=lambda p: (p["name"].lower(), p["version"]))


def licence_text(package: dict) -> str:
    """The crate's own licence text, from the source cargo already has on disk."""
    root = Path(package["manifest_path"]).parent
    found = []
    for entry in sorted(root.iterdir()):
        if not entry.is_file():
            continue
        if entry.name.upper().startswith(LICENCE_FILES):
            try:
                found.append((entry.name, entry.read_text(encoding="utf-8", errors="replace")))
            except OSError:
                continue
    return "\n".join(f"    --- {name} ---\n{text}" for name, text in found)


def render(packages: list[dict]) -> str:
    lines = [
        "THIRD-PARTY NOTICES",
        "",
        "InferTap is Apache-2.0 (see LICENSE and NOTICE). It is built with, and redistributes, the",
        "third-party components below, which remain under their own licences.",
        "",
        "This file covers:",
        "  - every Rust crate reachable from the infertap binary through normal and build",
        "    dependencies under the shipped feature set (dev-dependencies are not",
        "    redistributed and are excluded)",
        "  - libzmq, compiled from vendored source and statically linked",
        "",
        "=" * 78,
        "",
        LIBZMQ_NOTICE,
        "=" * 78,
        "",
        f"Rust crates ({len(packages)}):",
        "",
    ]
    for package in packages:
        lines.append(f"{package['name']} {package['version']}")
        lines.append(f"    Licence:    {package.get('license') or 'see source'}")
        if package.get("repository"):
            lines.append(f"    Repository: {package['repository']}")
        text = licence_text(package)
        if text:
            lines.append(text)
        else:
            # Named rather than silently omitted: a crate whose licence text is not in its
            # published source is a thing somebody should look at once, not a gap that
            # reads as completeness.
            lines.append(
                "    (no licence file in the published crate; the SPDX identifier above is"
            )
            lines.append("     the declaration from its Cargo.toml)")
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    rendered = render(shipped_closure(metadata()))

    if "--check" in sys.argv:
        committed = root / "THIRD-PARTY-NOTICES"
        if not committed.exists():
            sys.exit("THIRD-PARTY-NOTICES is not committed; run this script and commit its output")
        if committed.read_text() != rendered:
            sys.exit(
                "THIRD-PARTY-NOTICES is stale. The dependency tree moved and the notices "
                "did not.\nRegenerate: scripts/third-party-notices.py > THIRD-PARTY-NOTICES"
            )
        return

    sys.stdout.write(rendered)


if __name__ == "__main__":
    main()
