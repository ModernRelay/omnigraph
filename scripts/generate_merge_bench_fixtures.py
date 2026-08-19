#!/usr/bin/env python3
"""Generate the branch-merge micro-benchmark v1 fixtures, nothing else.

This script is the source of truth for the branch-merge micro-benchmark's
fixture data (RFC 0039 micro profile) - three fixtures, one slice of the
benchmark program, not its whole dataset. It builds them synthetically and
deterministically (same builder version and parameters produce the same
bytes), validates and freezes each, and stops. It runs no measurements: measuring is a separate,
deliberate act against these fixtures (see the crate README for the run
commands, or `omnigraph-bench list`).

Everything lands under <target>/bench/fixtures (gitignored; <target> honors
CARGO_TARGET_DIR); nothing outside this repository is read or written. A
cached fixture is kept only when its manifest's builder version matches the
binary's (`omnigraph-bench fixture builder-version`); a stale one is deleted
and rebuilt — the fixtures directory is disposable build output.

Usage: python3 scripts/generate_merge_bench_fixtures.py
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run(args: list[str | Path], cwd: Path | None = None) -> None:
    print(f"$ {' '.join(str(a) for a in args)}", flush=True)
    subprocess.run([str(a) for a in args], check=True, cwd=cwd)


def main() -> int:
    root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
    exe = ".exe" if sys.platform == "win32" else ""
    bench = target / "release" / f"omnigraph-bench{exe}"
    fixtures = target / "bench" / "fixtures"

    print("== building omnigraph-bench (release) ==")
    # cwd, not --manifest-path: cargo finds the repo's .cargo/config.toml by
    # walking up from cwd only, so the build runs from the repo root no
    # matter where this script was invoked.
    run(["cargo", "build", "--release", "-p", "omnigraph-bench"], cwd=root)

    # Capture stdout only; the subprocess's stderr passes through so a
    # failure explains itself.
    builder_version = int(
        subprocess.run(
            [str(bench), "fixture", "builder-version"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )

    fixtures.mkdir(parents=True, exist_ok=True)

    def cached_builder_version(fixture_dir: Path) -> int | None:
        """The builder version stamped in a cached fixture's manifest, or
        None when the manifest is missing/unreadable (a partial build)."""
        try:
            manifest = json.loads(
                (fixture_dir / "fixture-manifest.json").read_text()
            )
            return int(manifest["builder_version"])
        except (OSError, ValueError, KeyError):
            return None

    def build_fixture(name: str, *args: str) -> None:
        fixture_dir = fixtures / name
        if fixture_dir.is_dir():
            cached = cached_builder_version(fixture_dir)
            if cached == builder_version:
                print(f"== fixture {name} exists (builder v{cached}), keeping ==")
                return
            print(f"== fixture {name} is stale (manifest builder "
                  f"{'v' + str(cached) if cached is not None else 'unreadable'}, "
                  f"binary v{builder_version}): deleting and rebuilding — the "
                  f"fixtures directory is disposable build output ==")
            shutil.rmtree(fixture_dir)
        print(f"== building fixture {name} ==")
        run([bench, "fixture", "build", "--fixtures-root", fixtures, *args])
        # The expected name is hand-derived here; the binary derives its own.
        # A silent desync would freeze a fixture this script then reports
        # missing, so verify the build landed where this script expects.
        if not fixture_dir.is_dir():
            sys.exit(f"error: fixture build did not produce {fixture_dir} — "
                     f"this script's fixture name is out of sync with the "
                     f"binary's derived name")

    # The merge-bench v1 fixtures: three, covering the initial-workload section
    # of RFC 0039 (small unindexed, large indexed, many-table).
    build_fixture("fx-t8-n4k-scalars-noindex", "--tables", "8", "--rows", "4000")
    build_fixture("fx-t8-n100k-scalars-btree-fresh",
                  "--tables", "8", "--rows", "100000", "--index")
    build_fixture("fx-t140-n4k-scalars-noindex",
                  "--tables", "140", "--rows", "4000")

    print("\n== merge-bench v1 fixtures ready ==")
    run([bench, "list", "--fixtures-root", fixtures])
    print(f"\nfixtures root: {fixtures}")
    print("run a point (example):")
    print(f"  {bench} run --scenario m3 "
          f"--fixture {fixtures / 'fx-t8-n4k-scalars-noindex'} "
          f"--warmth warm --reps 5 --out <records-dir>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
