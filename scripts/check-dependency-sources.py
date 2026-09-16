#!/usr/bin/env python3
"""Refuse dependency code that reaches the build without a crates.io source.

`cargo deny` classifies a crate by its resolved source and skips a crate that
has none, so a `path` copy (bare or behind `[patch]`) and a `.cargo/config.toml`
source replacement pass its `sources` check. This script holds the three places
such a route shows up: the source-less `Cargo.lock` packages are exactly the
workspace members by name and version, no manifest declares a `[patch]` table,
and neither cargo config file (`.cargo/config.toml` or the deprecated
`.cargo/config`) declares a source replacement or path override. Build scripts
are outside both checks.
"""

from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"

MEMBERS_RE = re.compile(r"^members\s*=\s*\[(.*?)\]", re.MULTILINE | re.DOTALL)
QUOTED_RE = re.compile(r'"([^"]+)"')
PACKAGE_NAME_RE = re.compile(r'^name\s*=\s*"([^"]+)"', re.MULTILINE)
PACKAGE_VERSION_RE = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)
WORKSPACE_VERSION_RE = re.compile(r"^version\s*(?:\.workspace\s*=\s*true|=\s*\{\s*workspace\s*=\s*true\s*\})", re.MULTILINE)
CONFIG_FILES = (".cargo/config.toml", ".cargo/config")
LOCK_FIELD_RE = re.compile(r'^(name|version|source)\s*=\s*"([^"]*)"', re.MULTILINE)
PATCH_TABLE_RE = re.compile(r"^\s*\[patch[.\]]", re.MULTILINE)
CONFIG_OVERRIDE_RE = re.compile(
    r"^\s*(\[source[.\]]|\[registries[.\]]|\[registry\]|\[patch[.\]]|replace-with\s*=|paths\s*=)",
    re.MULTILINE,
)


def member_manifests(root: Path) -> list[Path]:
    match = MEMBERS_RE.search((root / "Cargo.toml").read_text())
    if match is None:
        return []
    manifests: list[Path] = []
    for entry in QUOTED_RE.findall(match.group(1)):
        for member_dir in sorted(root.glob(entry)):
            manifest = member_dir / "Cargo.toml"
            if manifest.is_file():
                manifests.append(manifest)
    return manifests


def workspace_package_version(root: Path) -> str | None:
    text = (root / "Cargo.toml").read_text()
    section = text.find("[workspace.package]")
    if section < 0:
        return None
    match = PACKAGE_VERSION_RE.search(text, section)
    return match.group(1) if match else None


def package_identity(root: Path, manifest: Path) -> tuple[str, str] | None:
    """The member's `(name, version)`; a `version.workspace = true` resolves through the root."""
    text = manifest.read_text()
    package_section = text.find("[package]")
    if package_section < 0:
        return None
    name = PACKAGE_NAME_RE.search(text, package_section)
    if name is None:
        return None
    version = PACKAGE_VERSION_RE.search(text, package_section)
    if version is not None:
        return name.group(1), version.group(1)
    if WORKSPACE_VERSION_RE.search(text, package_section) is not None:
        inherited = workspace_package_version(root)
        if inherited is not None:
            return name.group(1), inherited
    return name.group(1), "?"


def lock_packages(lock: Path) -> list[dict[str, str]]:
    packages: list[dict[str, str]] = []
    for block in lock.read_text().split("[[package]]")[1:]:
        block = block.split("\n[[", 1)[0]
        fields = {key: value for key, value in LOCK_FIELD_RE.findall(block)}
        packages.append(fields)
    return packages


def check(root: Path) -> tuple[list[str], str]:
    failures: list[str] = []
    manifests = member_manifests(root)
    members = {identity for identity in (package_identity(root, m) for m in manifests) if identity}

    registry = 0
    for package in lock_packages(root / "Cargo.lock"):
        name = package.get("name", "?")
        version = package.get("version", "?")
        source = package.get("source")
        if source is None:
            if (name, version) not in members:
                failures.append(
                    f"Cargo.lock: {name} {version} has no source and is not a workspace member "
                    "by name and version (a path copy or a [patch] path entry)"
                )
        elif source == CRATES_IO:
            registry += 1
        else:
            failures.append(f"Cargo.lock: {name} {version} comes from {source}, not crates.io")

    for manifest in [root / "Cargo.toml", *manifests]:
        for match in PATCH_TABLE_RE.finditer(manifest.read_text()):
            line = manifest.read_text().count("\n", 0, match.start()) + 1
            failures.append(f"{manifest.relative_to(root)}:{line}: [patch] table replaces a dependency's code")

    for relative in CONFIG_FILES:
        config = root / relative
        if not config.is_file():
            continue
        text = config.read_text()
        for match in CONFIG_OVERRIDE_RE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            failures.append(f"{relative}:{line}: source replacement or path override: {match.group(1).strip()}")

    summary = (
        f"Dependency sources OK ({registry} crates.io packages, {len(members)} workspace members by "
        "name and version; no [patch] table, no source replacement)."
    )
    return failures, summary


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def self_test() -> int:
    lock_ok = (
        '[[package]]\nname = "a"\nversion = "0.1.0"\n\n'
        f'[[package]]\nname = "b"\nversion = "1.0.0"\nsource = "{CRATES_IO}"\nchecksum = "x"\n'
    )
    cases: list[tuple[str, dict[str, str], int]] = [
        ("clean workspace", {"Cargo.lock": lock_ok}, 0),
        (
            "path copy outside the workspace",
            {"Cargo.lock": lock_ok + '\n[[package]]\nname = "c"\nversion = "2.0.0"\n'},
            1,
        ),
        (
            "git source",
            {"Cargo.lock": lock_ok + '\n[[package]]\nname = "d"\nversion = "3.0.0"\nsource = "git+https://example.invalid/d#abc"\n'},
            1,
        ),
        (
            "[patch] table in the root manifest",
            {"Cargo.lock": lock_ok, "Cargo.toml": 'members = ["crates/a"]\n\n[patch.crates-io]\nb = { path = "vendor/b" }\n'},
            1,
        ),
        (
            "[patch] table in a member manifest",
            {"Cargo.lock": lock_ok, "crates/a/Cargo.toml": '[package]\nname = "a"\nversion = "0.1.0"\n\n[patch.crates-io]\nb = { path = "../b" }\n'},
            1,
        ),
        (
            "source replacement in the cargo config",
            {"Cargo.lock": lock_ok, ".cargo/config.toml": '[source.crates-io]\nreplace-with = "vendored"\n\n[source.vendored]\ndirectory = "vendor"\n'},
            3,
        ),
        (
            "path copy named like a member at another version",
            {"Cargo.lock": lock_ok + '\n[[package]]\nname = "a"\nversion = "9.9.9"\n'},
            1,
        ),
        (
            "path override in the deprecated .cargo/config",
            {"Cargo.lock": lock_ok, ".cargo/config": 'paths = ["vendor/b"]\n'},
            1,
        ),
        (
            "member version inherited from [workspace.package]",
            {
                "Cargo.lock": lock_ok.replace('version = "0.1.0"', 'version = "0.2.0"', 1),
                "Cargo.toml": '[workspace]\nmembers = ["crates/a"]\n\n[workspace.package]\nversion = "0.2.0"\n',
                "crates/a/Cargo.toml": '[package]\nname = "a"\nversion.workspace = true\n',
            },
            0,
        ),
    ]
    for label, files, expected_failures in cases:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            write(root / "Cargo.toml", '[workspace]\nmembers = ["crates/a"]\n')
            write(root / "crates" / "a" / "Cargo.toml", '[package]\nname = "a"\nversion = "0.1.0"\n')
            for relative, text in files.items():
                write(root / relative, text)
            failures, _ = check(root)
            if len(failures) != expected_failures:
                print(f"self-test FAILED: {label}: expected {expected_failures} failure(s), got {failures}", file=sys.stderr)
                return 1
    print(f"check-dependency-sources self-test OK ({len(cases)} cases).")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    failures, summary = check(REPO_ROOT)
    if failures:
        print("Dependency code without a crates.io source:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
