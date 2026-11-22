#!/usr/bin/env python3
"""Provision the predecessor binaries of `.github/predecessors.toml`.

  provision [--store DIR] [--require] (--all | ROW...)
      Install or build each named row, or with --all every row, into DIR/rows/<row>/ (default
      target/predecessors), reusing a row already there when it was made from
      the same registry entry and still reports its version. Then write
      DIR/manifest.json, naming every provisioned row in DIR, and print its
      path, the only line it writes to stdout (child output goes to stderr).
      --all takes every row this platform can provision. A test reads the
      manifest named by OMNIGRAPH_PREDECESSORS, else
      target/predecessors/manifest.json. With --require, a test that asks for
      a row the manifest lacks fails instead of skipping.
  cache-key ROW...
      Print a cache key that changes exactly when the named rows, the binary
      packages, this script or the installer change.
  check
      Every row is asked for by a test, every row a test or a workflow names
      exists, and every row is well formed.
  --self-test
      Run this script's own tests.

docs/dev/testing.md, Predecessor binaries, describes the registry.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / ".github/predecessors.toml"
DEFAULT_STORE = ROOT / "target/predecessors"
INSTALLER = ROOT / "scripts/install.sh"
REPOSITORY = "ModernRelay/omnigraph"
# The platform that every release row pins a digest for: CI's runner.
CI_PLATFORM = "linux-x86_64"
RELEASE_FIELDS = {"about", "release", "sha256", "binaries", "reports", "retire_when"}
SOURCE_FIELDS = {"about", "commit", "toolchain", "binaries", "reports", "retire_when"}
PROSE_FIELDS = {"about", "retire_when"}
# A test asks for a row with `predecessor("<row>")`.
TEST_REQUEST = re.compile(r'\bpredecessor\(\s*"([^"]+)"\s*\)')
# A workflow asks for rows with the local action and a `rows:` input.
WORKFLOW_ROWS = re.compile(
    r"uses: \./\.github/actions/predecessors\n\s+with:\n(?:\s+\w+: .*\n)*?\s+rows: (.+)\n"
)


class RegistryError(Exception):
    """The registry, a row or the store is not what a provision needs."""


def log(message: str) -> None:
    print(f"predecessors: {message}", file=sys.stderr, flush=True)


def load_registry(path: Path = REGISTRY) -> dict:
    try:
        registry = tomllib.loads(path.read_text())
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RegistryError(f"cannot read {path}: {error}") from error
    validate_registry(registry)
    return registry


def validate_registry(registry: dict) -> None:
    packages = registry.get("packages")
    rows = registry.get("predecessors")
    if not isinstance(packages, dict) or not packages:
        raise RegistryError("the registry needs a [packages] table naming each binary's package")
    if not isinstance(rows, dict) or not rows:
        raise RegistryError("the registry names no predecessor")
    for name, row in rows.items():
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", name):
            raise RegistryError(f"row {name!r}: a name is lowercase letters, digits, '.' and '-'")
        fields = set(row)
        expected = RELEASE_FIELDS if "release" in row else SOURCE_FIELDS
        if fields != expected:
            raise RegistryError(
                f"row {name!r} must have exactly {sorted(expected)}; "
                f"missing {sorted(expected - fields)}, unknown {sorted(fields - expected)}"
            )
        binaries = row["binaries"]
        if not isinstance(binaries, list) or not binaries or len(set(binaries)) != len(binaries):
            raise RegistryError(f"row {name!r}: binaries is a nonempty list without repeats")
        unknown = sorted(set(binaries) - set(packages))
        if unknown:
            raise RegistryError(f"row {name!r}: no [packages] entry builds {unknown}")
        if "omnigraph" not in binaries:
            raise RegistryError(f"row {name!r}: every predecessor provides `omnigraph`, which checks `reports`")
        for field in ("about", "reports", "retire_when"):
            if not isinstance(row[field], str) or not row[field].strip():
                raise RegistryError(f"row {name!r}: {field} is a nonempty string")
        if "release" in row:
            digests = row["sha256"]
            if not isinstance(digests, dict) or CI_PLATFORM not in digests:
                raise RegistryError(f"row {name!r}: sha256 pins at least the {CI_PLATFORM} asset")
            for asset, digest in digests.items():
                if not re.fullmatch(r"[0-9a-f]{64}", str(digest)):
                    raise RegistryError(f"row {name!r}: sha256.{asset} is not a SHA256 digest")
        else:
            if not re.fullmatch(r"[0-9a-f]{40}", str(row["commit"])):
                raise RegistryError(f"row {name!r}: commit is a full 40-character SHA")
            if not re.fullmatch(r"\d+\.\d+\.\d+", str(row["toolchain"])):
                raise RegistryError(f"row {name!r}: toolchain is an exact Rust version")


def select(registry: dict, names: list[str]) -> dict:
    if not names:
        raise RegistryError("name at least one row")
    rows = registry["predecessors"]
    unknown = sorted(set(names) - set(rows))
    if unknown:
        raise RegistryError(f"no registry row is named {unknown}; rows: {sorted(rows)}")
    return {name: rows[name] for name in dict.fromkeys(names)}


def available(registry: dict, asset: str) -> list[str]:
    """The rows this platform can provision: every source row, and each release that pins its asset."""
    return [
        name for name, row in registry["predecessors"].items()
        if "release" not in row or asset in row["sha256"]
    ]


def platform_asset() -> str:
    """The `scripts/install.sh` asset stem for this host."""
    system, machine = platform.system(), platform.machine()
    assets = {
        ("Linux", "x86_64"): "linux-x86_64",
        ("Linux", "aarch64"): "linux-arm64",
        ("Darwin", "arm64"): "macos-arm64",
    }
    if (system, machine) not in assets:
        raise RegistryError(f"no predecessor can be provisioned on {system}/{machine}")
    return assets[(system, machine)]


def identity(name: str, row: dict, packages: dict, asset: str) -> dict:
    """What a provisioned row was made from; a stored row is reused only when this matches."""
    # Prose (`about`, `retire_when`) does not change what is provisioned.
    made_from = {field: value for field, value in row.items() if field not in PROSE_FIELDS}
    return {
        "name": name,
        "row": made_from,
        "packages": {binary: packages[binary] for binary in row["binaries"]},
        "platform": asset,
    }


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def cache_key(registry: dict, names: list[str], asset: str) -> str:
    rows = select(registry, names)
    digest = hashlib.sha256()
    digest.update(canonical([identity(name, row, registry["packages"], asset) for name, row in sorted(rows.items())]).encode())
    for path in (Path(__file__).resolve(), INSTALLER):
        digest.update(path.read_bytes())
    return f"predecessors-{asset}-{digest.hexdigest()[:20]}"


def reported(binary: Path) -> str:
    try:
        output = subprocess.run(
            [str(binary), "--version"], capture_output=True, text=True, timeout=60,
            env={**os.environ, "OMNIGRAPH_HOME": "/nonexistent/omnigraph-predecessor-home"},
        )
    except (OSError, subprocess.SubprocessError) as error:
        return f"<cannot run {binary}: {error}>"
    return output.stdout.strip() if output.returncode == 0 else f"<exit {output.returncode}: {output.stderr.strip()}>"


def stored(row_dir: Path, wanted: dict) -> bool:
    """Whether `row_dir` already holds the row `wanted` describes, reporting its version."""
    try:
        recorded = json.loads((row_dir / "row.json").read_text())
    except (OSError, ValueError):
        return False
    if recorded != wanted:
        return False
    binaries = wanted["row"]["binaries"]
    if not all((row_dir / binary).is_file() for binary in binaries):
        return False
    return reported(row_dir / "omnigraph") == wanted["row"]["reports"]


def install_release(row: dict, asset: str, into: Path) -> None:
    digest = row["sha256"].get(asset)
    if digest is None:
        raise RegistryError(f"release {row['release']} pins no digest for the {asset} asset")
    environment = {
        **os.environ,
        "REPO_SLUG": REPOSITORY,
        "VERSION": row["release"],
        "INSTALL_DIR": str(into),
        "ARCHIVE_SHA256": digest,
    }
    # A failed download leaves nothing behind; three attempts, the only retry.
    for attempt in (1, 2, 3):
        # The installer's summary goes to stderr: stdout carries only the manifest path.
        if subprocess.run(["bash", str(INSTALLER)], env=environment, stdout=sys.stderr).returncode == 0:
            return
        log(f"install of {row['release']} failed on attempt {attempt}/3")
        if attempt < 3:
            time.sleep(10)
    raise RegistryError(f"cannot install release {row['release']}")


def git(*args: str, cwd: Path = ROOT) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


def build_commit(name: str, row: dict, packages: dict, work: Path, into: Path) -> None:
    commit, toolchain = row["commit"], row["toolchain"]
    if git("cat-file", "-e", f"{commit}^{{commit}}").returncode != 0:
        fetched = git("fetch", "--no-tags", "--depth=1", "origin", commit)
        if fetched.returncode != 0:
            raise RegistryError(f"cannot fetch {commit} for {name}: {fetched.stderr.strip()}")
    checkout, target = work / "checkout", work / "target"
    shutil.rmtree(work, ignore_errors=True)
    git("worktree", "prune")
    added = git("worktree", "add", "--detach", "--force", str(checkout), commit)
    if added.returncode != 0:
        raise RegistryError(f"cannot check out {commit} for {name}: {added.stderr.strip()}")
    try:
        head = git("rev-parse", "HEAD", cwd=checkout).stdout.strip()
        if head != commit:
            raise RegistryError(f"the checkout of {name} is at {head}, not {commit}")
        if shutil.which("rustup"):
            subprocess.run(
                ["rustup", "toolchain", "install", toolchain, "--profile", "minimal"],
                check=True, stdout=sys.stderr,
            )
        selection = []
        for package in dict.fromkeys(packages[binary] for binary in row["binaries"]):
            selection += ["--package", package]
        for binary in row["binaries"]:
            selection += ["--bin", binary]
        # Inside its own checkout, so the predecessor's Cargo configuration
        # and lockfile apply; its own target directory, so no artifact of a
        # same-named path package of the current tree can be linked into it.
        log(f"building {name} from {commit} with Rust {toolchain}")
        subprocess.run(
            ["cargo", f"+{toolchain}", "build", "--locked", *selection, "--target-dir", str(target)],
            cwd=checkout, check=True, stdout=sys.stderr,
        )
        for binary in row["binaries"]:
            shutil.copy2(target / "debug" / binary, into / binary)
    finally:
        git("worktree", "remove", "--force", str(checkout))
        shutil.rmtree(work, ignore_errors=True)


def provision_row(name: str, row: dict, packages: dict, asset: str, store: Path) -> None:
    wanted = identity(name, row, packages, asset)
    row_dir = store / "rows" / name
    if stored(row_dir, wanted):
        log(f"{name}: reusing {row_dir}")
        return
    shutil.rmtree(row_dir, ignore_errors=True)
    row_dir.mkdir(parents=True)
    if "release" in row:
        with tempfile.TemporaryDirectory(dir=store) as installed:
            install_release(row, asset, Path(installed))
            for binary in row["binaries"]:
                shutil.copy2(Path(installed) / binary, row_dir / binary)
    else:
        build_commit(name, row, packages, store / "work" / name, row_dir)
    for binary in row["binaries"]:
        if not os.access(row_dir / binary, os.X_OK):
            raise RegistryError(f"{name} provided no executable {binary}")
    actual = reported(row_dir / "omnigraph")
    if actual != row["reports"]:
        raise RegistryError(f"{name} reports {actual!r}, the registry says {row['reports']!r}")
    # Written last: a row without it is incomplete and is provisioned again.
    (row_dir / "row.json").write_text(canonical(wanted) + "\n")


def manifest(registry: dict, asset: str, store: Path, required: bool) -> dict:
    """Every row in `store` made from its current registry entry."""
    entries = {}
    rows_dir = store / "rows"
    for name, row in sorted(registry["predecessors"].items()):
        row_dir = rows_dir / name
        try:
            recorded = json.loads((row_dir / "row.json").read_text())
        except (OSError, ValueError):
            continue
        if recorded != identity(name, row, registry["packages"], asset):
            continue
        source = {"release": row["release"]} if "release" in row else {"commit": row["commit"]}
        entries[name] = {
            "reports": row["reports"],
            "source": source,
            "binaries": {binary: str((row_dir / binary).resolve()) for binary in row["binaries"]},
        }
    return {"required": required, "predecessors": entries}


def provision(names: list[str], store: Path, required: bool, registry: dict | None = None) -> Path:
    registry = load_registry() if registry is None else registry
    rows = select(registry, names)
    asset = platform_asset()
    store.mkdir(parents=True, exist_ok=True)
    for name, row in rows.items():
        provision_row(name, row, registry["packages"], asset, store)
    written = manifest(registry, asset, store, required)
    missing = sorted(set(rows) - set(written["predecessors"]))
    if missing:
        raise RegistryError(f"provisioned rows {missing} are not in the manifest")
    path = store / "manifest.json"
    path.write_text(json.dumps(written, indent=2, sort_keys=True) + "\n")
    return path


def requests(root: Path = ROOT) -> dict[str, set[str]]:
    """Each row a test or workflow names, with the files that name it."""
    named: dict[str, set[str]] = {}
    for source in sorted((root / "crates").glob("*/tests/**/*.rs")):
        for name in TEST_REQUEST.findall(source.read_text()):
            named.setdefault(name, set()).add(str(source.relative_to(root)))
    for workflow in sorted((root / ".github/workflows").glob("*.yml")):
        for listed in WORKFLOW_ROWS.findall(workflow.read_text()):
            for name in listed.strip().strip("'\"").split():
                named.setdefault(name, set()).add(str(workflow.relative_to(root)))
    return named


def check(registry: dict, root: Path = ROOT) -> list[str]:
    failures = []
    rows = set(registry["predecessors"])
    named = requests(root)
    for name in sorted(set(named) - rows):
        failures.append(f"{', '.join(sorted(named[name]))} names {name!r}, which is not a registry row")
    asked = {name for name, files in named.items() if any(file.endswith(".rs") for file in files)}
    for name in sorted(rows - asked):
        failures.append(f"no test asks for registry row {name!r}; retire it")
    return failures


class Tests(unittest.TestCase):
    def registry(self) -> dict:
        return {
            "packages": {"omnigraph": "omnigraph-cli", "omnigraph-server": "omnigraph-server"},
            "predecessors": {
                "release-1.0.0": {
                    "about": "a release", "release": "v1.0.0",
                    "sha256": {CI_PLATFORM: "a" * 64}, "binaries": ["omnigraph"],
                    "reports": "omnigraph 1.0.0", "retire_when": "never",
                },
                "stamp-1": {
                    "about": "a commit", "commit": "b" * 40, "toolchain": "1.97.1",
                    "binaries": ["omnigraph", "omnigraph-server"],
                    "reports": "omnigraph 0.1.0", "retire_when": "never",
                },
            },
        }

    def test_the_checked_in_registry_is_valid_and_complete(self):
        registry = load_registry()
        self.assertEqual(check(registry), [])
        for name in registry["predecessors"]:
            cache_key(registry, [name], CI_PLATFORM)

    def test_malformed_rows_are_refused(self):
        def broken(change):
            registry = self.registry()
            change(registry["predecessors"])
            with self.assertRaises(RegistryError):
                validate_registry(registry)
        validate_registry(self.registry())
        broken(lambda rows: rows["release-1.0.0"].pop("sha256"))
        broken(lambda rows: rows["release-1.0.0"]["sha256"].pop(CI_PLATFORM))
        broken(lambda rows: rows["release-1.0.0"]["sha256"].update({CI_PLATFORM: "short"}))
        broken(lambda rows: rows["release-1.0.0"].update(commit="b" * 40))
        broken(lambda rows: rows["stamp-1"].update(commit="main"))
        broken(lambda rows: rows["stamp-1"].update(toolchain="stable"))
        broken(lambda rows: rows["stamp-1"].update(binaries=["omnigraph-server"]))
        broken(lambda rows: rows["stamp-1"].update(binaries=["omnigraph", "omnigraph"]))
        broken(lambda rows: rows["stamp-1"].update(binaries=["omnigraph", "omnigraph-gqt"]))
        broken(lambda rows: rows["stamp-1"].update(reports=""))
        broken(lambda rows: rows["stamp-1"].pop("retire_when"))
        broken(lambda rows: rows.update({"Stamp 2": rows["stamp-1"]}))
        broken(lambda rows: rows.clear())

    def test_all_selects_the_rows_this_platform_can_provision(self):
        registry = self.registry()
        self.assertEqual(available(registry, CI_PLATFORM), ["release-1.0.0", "stamp-1"])
        self.assertEqual(available(registry, "linux-arm64"), ["stamp-1"])
        checked_in = load_registry()
        self.assertEqual(available(checked_in, CI_PLATFORM), list(checked_in["predecessors"]))

    def test_child_output_never_reaches_stdout(self):
        from unittest import mock
        registry = self.registry()
        calls = []

        def run(command, **options):
            calls.append((command, options))
            return subprocess.CompletedProcess(command, 0, "", "")

        with mock.patch.object(subprocess, "run", run), tempfile.TemporaryDirectory() as temp:
            install_release(registry["predecessors"]["release-1.0.0"], CI_PLATFORM, Path(temp))
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][1].get("stdout"), sys.stderr)
        # Every streaming child of a build sends stdout to stderr too.
        source = Path(__file__).read_text()
        build = source[source.index("def build_commit("):source.index("def provision_row(")]
        self.assertEqual(build.count("stdout=sys.stderr"), build.count("subprocess.run("))

    def test_selection_refuses_unknown_and_empty_names(self):
        registry = self.registry()
        self.assertEqual(list(select(registry, ["stamp-1", "stamp-1"])), ["stamp-1"])
        with self.assertRaises(RegistryError):
            select(registry, ["stamp-2"])
        with self.assertRaises(RegistryError):
            select(registry, [])

    def test_cache_key_follows_what_the_rows_provision(self):
        registry = self.registry()
        key = cache_key(registry, ["stamp-1", "release-1.0.0"], CI_PLATFORM)
        self.assertEqual(key, cache_key(registry, ["release-1.0.0", "stamp-1"], CI_PLATFORM))
        self.assertNotEqual(key, cache_key(registry, ["stamp-1"], CI_PLATFORM))
        self.assertNotEqual(key, cache_key(registry, ["stamp-1", "release-1.0.0"], "macos-arm64"))
        registry["predecessors"]["stamp-1"]["commit"] = "c" * 40
        self.assertNotEqual(key, cache_key(registry, ["stamp-1", "release-1.0.0"], CI_PLATFORM))
        registry = self.registry()
        registry["predecessors"]["stamp-1"]["about"] = "reworded"
        self.assertEqual(key, cache_key(registry, ["stamp-1", "release-1.0.0"], CI_PLATFORM))
        registry["packages"]["omnigraph-server"] = "another-package"
        self.assertNotEqual(key, cache_key(registry, ["stamp-1", "release-1.0.0"], CI_PLATFORM))

    def plant(self, store: Path, name: str, row: dict, packages: dict, reports: str) -> None:
        row_dir = store / "rows" / name
        row_dir.mkdir(parents=True)
        for binary in row["binaries"]:
            path = row_dir / binary
            path.write_text(f"#!/bin/sh\necho '{reports}'\n")
            path.chmod(0o755)
        (row_dir / "row.json").write_text(canonical(identity(name, row, packages, CI_PLATFORM)))

    def test_a_stored_row_is_reused_only_when_its_entry_and_version_match(self):
        registry = self.registry()
        row = registry["predecessors"]["stamp-1"]
        with tempfile.TemporaryDirectory() as temp:
            store = Path(temp)
            self.plant(store, "stamp-1", row, registry["packages"], "omnigraph 0.1.0")
            wanted = identity("stamp-1", row, registry["packages"], CI_PLATFORM)
            self.assertTrue(stored(store / "rows/stamp-1", wanted))
            self.assertFalse(stored(store / "rows/stamp-1", {**wanted, "platform": "macos-arm64"}))
            (store / "rows/stamp-1/omnigraph").write_text("#!/bin/sh\necho 'omnigraph 9.9.9'\n")
            self.assertFalse(stored(store / "rows/stamp-1", wanted))
            (store / "rows/stamp-1/omnigraph-server").unlink()
            self.assertFalse(stored(store / "rows/stamp-1", wanted))

    def test_the_manifest_names_every_current_row_in_the_store(self):
        registry = self.registry()
        with tempfile.TemporaryDirectory() as temp:
            store = Path(temp)
            for name, row in registry["predecessors"].items():
                self.plant(store, name, row, registry["packages"], row["reports"])
            (store / "rows/orphan").mkdir()
            written = manifest(registry, CI_PLATFORM, store, required=True)
            self.assertTrue(written["required"])
            self.assertEqual(sorted(written["predecessors"]), ["release-1.0.0", "stamp-1"])
            entry = written["predecessors"]["stamp-1"]
            self.assertEqual(entry["reports"], "omnigraph 0.1.0")
            self.assertEqual(entry["source"], {"commit": "b" * 40})
            self.assertEqual(sorted(entry["binaries"]), ["omnigraph", "omnigraph-server"])
            self.assertTrue(all(Path(path).is_absolute() for path in entry["binaries"].values()))
            registry["predecessors"]["stamp-1"]["commit"] = "c" * 40
            self.assertEqual(sorted(manifest(registry, CI_PLATFORM, store, False)["predecessors"]), ["release-1.0.0"])

    def test_check_names_unknown_requests_and_unrequested_rows(self):
        registry = self.registry()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            tests = root / "crates/omnigraph-cli/tests/integration"
            tests.mkdir(parents=True)
            workflows = root / ".github/workflows"
            workflows.mkdir(parents=True)
            (tests / "journey.rs").write_text('let old = predecessor("release-1.0.0");\n')
            (workflows / "ci.yml").write_text(
                "      - uses: ./.github/actions/predecessors\n"
                "        with:\n          require: true\n          rows: stamp-1 stamp-9\n"
            )
            self.assertEqual(
                check(registry, root),
                [
                    ".github/workflows/ci.yml names 'stamp-9', which is not a registry row",
                    "no test asks for registry row 'stamp-1'; retire it",
                ],
            )
            (tests / "journey.rs").write_text('predecessor("release-1.0.0"); predecessor( "stamp-1" );\n')
            (workflows / "ci.yml").write_text(
                "      - uses: ./.github/actions/predecessors\n        with:\n          rows: stamp-1\n"
            )
            self.assertEqual(check(registry, root), [])

    def test_the_workflows_name_rows_the_registry_has(self):
        named = requests()
        workflow_rows = {name for name, files in named.items() if any(file.endswith(".yml") for file in files)}
        self.assertTrue(workflow_rows, "no workflow provisions a predecessor")
        self.assertLessEqual(workflow_rows, set(load_registry()["predecessors"]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--self-test", action="store_true")
    commands = parser.add_subparsers(dest="command")
    provisioning = commands.add_parser("provision")
    provisioning.add_argument("--store", type=Path, default=DEFAULT_STORE)
    provisioning.add_argument("--require", action="store_true")
    provisioning.add_argument("--all", action="store_true", help="every registry row")
    provisioning.add_argument("rows", nargs="*")
    keying = commands.add_parser("cache-key")
    keying.add_argument("rows", nargs="+")
    commands.add_parser("check")
    args = parser.parse_args()
    if args.self_test:
        result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
        return 0 if result.wasSuccessful() else 1
    try:
        if args.command == "provision":
            if args.all == bool(args.rows):
                parser.error("provision takes either --all or row names")
            registry = load_registry()
            names = args.rows
            if args.all:
                names = available(registry, platform_asset())
                for name in sorted(set(registry["predecessors"]) - set(names)):
                    log(f"{name}: no archive is pinned for {platform_asset()}; not provisioned")
            print(provision(names, args.store.resolve(), args.require, registry))
        elif args.command == "cache-key":
            print(cache_key(load_registry(), args.rows, platform_asset()))
        elif args.command == "check":
            failures = check(load_registry())
            for failure in failures:
                print(f"Predecessors: {failure}", file=sys.stderr)
            if failures:
                return 1
            print("Predecessors OK (every registry row is well formed and asked for, every named row exists).")
        else:
            parser.error("name a command")
    except (RegistryError, subprocess.CalledProcessError) as error:
        print(f"predecessors: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
