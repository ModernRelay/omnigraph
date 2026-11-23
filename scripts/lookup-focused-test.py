#!/usr/bin/env python3
"""Map a source or test file path to its owning package and fastest cargo test command.

Eliminates multi-turn search/grep churn by determining the exact crate, test binary,
and focused filter argument for any given file in the Omnigraph repository.

Usage:
    python3 scripts/lookup-focused-test.py <file-path> [--command-only] [--json]
    python3 scripts/lookup-focused-test.py --self-test
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class TestMapping:
    file_path: str
    package: str
    crate_dir: str
    category: str
    recommended_command: str
    notes: str = ""


# Mapping of directory prefix to cargo package name
CRATE_PACKAGE_MAP: dict[str, str] = {
    "crates/omnigraph": "omnigraph-engine",
    "crates/omnigraph-compiler": "omnigraph-compiler",
    "crates/omnigraph-storage": "omnigraph-storage",
    "crates/omnigraph-seams": "omnigraph-seams",
    "crates/omnigraph-core": "omnigraph-core",
    "crates/omnigraph-catalog": "omnigraph-catalog",
    "crates/omnigraph-planner": "omnigraph-planner",
    "crates/omnigraph-reference-engine": "omnigraph-reference-engine",
    "crates/omnigraph-cli": "omnigraph-cli",
    "crates/omnigraph-api-types": "omnigraph-api-types",
    "crates/omnigraph-cluster": "omnigraph-cluster",
    "crates/omnigraph-azure-admission": "omnigraph-azure-admission",
    "crates/omnigraph-bench": "omnigraph-bench",
    "crates/omnigraph-policy": "omnigraph-policy",
    "crates/omnigraph-mcp": "omnigraph-mcp",
    "crates/omnigraph-server": "omnigraph-server",
    "crates/omnigraph-dst": "omnigraph-dst",
    "crates/omnigraph-gqt": "omnigraph-gqt",
    "crates/omnigraph-gqt-core": "omnigraph-gqt-core",
    "crates/omnigraph-gqt-served": "omnigraph-gqt-served",
    "skills": "omnigraph-skill",
    "tools/omnigraph-vocabulary-guard": "omnigraph-vocabulary-guard",
}

# Test suites and integration modules requiring the failpoints feature gate
FAILPOINT_TEST_BINARIES: set[str] = {
    "failpoints",
    "system_column_upgrade",
    "detached_commit_matrix",
    "identity_recovery",
}
FAILPOINT_INTEGRATION_MODULES: set[str] = {
    "legacy_columns",
}


def normalize_path(path_str: str, repo_root: Path) -> Path:
    """Normalize input path relative to repository root."""
    p = Path(path_str)
    if p.is_absolute():
        try:
            return p.relative_to(repo_root)
        except ValueError:
            return p
    return p


def resolve_mapping(path_str: str, repo_root: Path | None = None) -> TestMapping:
    if repo_root is None:
        repo_root = Path(__file__).resolve().parent.parent

    rel_path = normalize_path(path_str, repo_root)
    posix_path = rel_path.as_posix()

    # 1. Documentation files
    if posix_path.startswith("docs/") or posix_path in ("AGENTS.md", "CLAUDE.md", "README.md"):
        return TestMapping(
            file_path=posix_path,
            package="docs",
            crate_dir="docs",
            category="documentation",
            recommended_command="python3 scripts/check-docs.py && bash scripts/check-agents-md.sh",
            notes="Documentation link integrity and AGENTS.md sync checks",
        )

    # 2. GQT test cases (.gqt files)
    if posix_path.startswith("crates/omnigraph-gqt/cases/") and posix_path.endswith(".gqt"):
        case_name = Path(posix_path).stem
        return TestMapping(
            file_path=posix_path,
            package="omnigraph-gqt",
            crate_dir="crates/omnigraph-gqt",
            category="gqt-logic-test",
            recommended_command=f"cargo test -p omnigraph-gqt --locked --test gq_logic_tests -- {case_name}",
            notes=f"Single GQT test case execution for {case_name}",
        )

    if posix_path.startswith("crates/omnigraph-gqt/cases_slow/") and posix_path.endswith(".gqt"):
        return TestMapping(
            file_path=posix_path,
            package="omnigraph-gqt",
            crate_dir="crates/omnigraph-gqt",
            category="gqt-slow-test",
            recommended_command=f"cargo run -p omnigraph-gqt --bin omnigraph-gqt -- {posix_path}",
            notes="Slow GQT test case execution via omnigraph-gqt binary",
        )

    # 3. Skills directory
    if posix_path.startswith("skills/"):
        return TestMapping(
            file_path=posix_path,
            package="omnigraph-skill",
            crate_dir="skills",
            category="skill-verification",
            recommended_command="cargo test -p omnigraph-cli --test integration cli_schema_config::",
            notes="Verifies embedded skill byte identity and CLI export parity",
        )

    # 4. DST (Deterministic Simulation Testing)
    if posix_path.startswith("crates/omnigraph-dst/"):
        return TestMapping(
            file_path=posix_path,
            package="omnigraph-dst",
            crate_dir="crates/omnigraph-dst",
            category="dst-simulation",
            recommended_command="(cd crates/omnigraph-dst && cargo test --locked)",
            notes="Crate-local Cargo configuration supplies the pool environment; workspace configuration enables tokio_unstable",
        )

    # Find matching crate by longest directory prefix match
    matching_prefix = None
    for prefix in sorted(CRATE_PACKAGE_MAP.keys(), key=len, reverse=True):
        if posix_path.startswith(prefix + "/") or posix_path == prefix:
            matching_prefix = prefix
            break

    if not matching_prefix:
        # Generic fallback
        return TestMapping(
            file_path=posix_path,
            package="workspace",
            crate_dir=".",
            category="generic",
            recommended_command="cargo check --workspace",
            notes="Unmapped workspace path",
        )

    package = CRATE_PACKAGE_MAP[matching_prefix]
    sub_path = posix_path[len(matching_prefix) :].lstrip("/")

    # Integration test module: tests/integration/<module>.rs
    if sub_path.startswith("tests/integration/") and sub_path.endswith(".rs"):
        module_name = Path(sub_path).stem
        features_flag = " --features failpoints" if module_name in FAILPOINT_INTEGRATION_MODULES else ""
        if module_name == "main":
            return TestMapping(
                file_path=posix_path,
                package=package,
                crate_dir=matching_prefix,
                category="integration-suite",
                recommended_command=f"cargo test -p {package} --test integration",
                notes="Runs all modules linked in integration/main.rs",
            )
        return TestMapping(
            file_path=posix_path,
            package=package,
            crate_dir=matching_prefix,
            category="focused-integration-module",
            recommended_command=f"cargo test -p {package}{features_flag} --test integration {module_name}::",
            notes=f"Runs integration module '{module_name}'" + (" (requires --features failpoints)" if features_flag else ""),
        )

    # Standalone integration test: tests/<binary>.rs
    if sub_path.startswith("tests/") and sub_path.endswith(".rs") and "/" not in sub_path[len("tests/") :]:
        binary_name = Path(sub_path).stem
        if binary_name in FAILPOINT_TEST_BINARIES:
            return TestMapping(
                file_path=posix_path,
                package=package,
                crate_dir=matching_prefix,
                category="standalone-integration-test",
                recommended_command=f"cargo test -p {package} --features failpoints --test {binary_name}",
                notes=f"Failpoint integration test '{binary_name}' requires --features failpoints",
            )
        return TestMapping(
            file_path=posix_path,
            package=package,
            crate_dir=matching_prefix,
            category="standalone-integration-test",
            recommended_command=f"cargo test -p {package} --test {binary_name}",
            notes=f"Standalone integration binary '{binary_name}'",
        )

    # Library source: src/**/*.rs
    if sub_path.startswith("src/"):
        stem = Path(sub_path).stem
        # Special case: engine write path changes
        if package == "omnigraph-engine" and ("staging" in sub_path or "storage" in sub_path or "table_store" in sub_path):
            return TestMapping(
                file_path=posix_path,
                package=package,
                crate_dir=matching_prefix,
                category="unit-and-write-integration",
                recommended_command=f"cargo test -p {package} --test writes",
                notes="Write-path source file: fastest coverage via the writes integration binary",
            )
        # Special case: engine traversal / planner
        if package == "omnigraph-engine" and ("traversal" in sub_path or "engine" in sub_path):
            return TestMapping(
                file_path=posix_path,
                package=package,
                crate_dir=matching_prefix,
                category="unit-and-traversal-integration",
                recommended_command=f"cargo test -p {package} --test integration traversal_indexed::",
                notes="Traversal source file: fastest coverage via traversal_indexed:: module",
            )

        file_name = Path(sub_path).name
        crate_src_dir = repo_root / matching_prefix / "src"
        parent_dir = repo_root / matching_prefix / Path(sub_path).parent

        # 1. Check if file is mounted from crate root (lib.rs or main.rs)
        crate_root_mount = None
        for root_file in (crate_src_dir / "lib.rs", crate_src_dir / "main.rs"):
            if root_file.is_file():
                try:
                    root_text = root_file.read_text(encoding="utf-8", errors="ignore")
                    m = re.search(
                        rf'#\[path\s*=\s*"{re.escape(file_name)}"\]\s*(?:#\[[^\]]+\]\s*)*mod\s+([a-zA-Z0-9_]+)',
                        root_text,
                    )
                    if m:
                        crate_root_mount = m.group(1)
                        break
                except OSError:
                    pass

        if crate_root_mount:
            mod_filter = crate_root_mount
        elif stem == "tests":
            parent_name = Path(sub_path).parent.name
            mod_filter = "tests" if parent_name == "src" else parent_name
        else:
            mod_filter = stem
            # Check if this file is mounted under a sibling module via #[path]
            # e.g. parser_tests.rs mounted in parser.rs via #[path = "parser_tests.rs"]
            candidate_base = stem[:-6] if stem.endswith("_tests") else (stem[:-5] if stem.endswith("_test") else None)
            if candidate_base:
                sibling_file = parent_dir / f"{candidate_base}.rs"
                if sibling_file.is_file():
                    try:
                        content = sibling_file.read_text(encoding="utf-8", errors="ignore")
                        if f'#[path = "{file_name}"]' in content or f'#[path="{file_name}"]' in content:
                            mod_filter = candidate_base
                    except OSError:
                        pass

        if mod_filter not in ("lib", "main", "mod", "src", ""):
            return TestMapping(
                file_path=posix_path,
                package=package,
                crate_dir=matching_prefix,
                category="unit-test",
                recommended_command=f"cargo test -p {package} --lib {mod_filter}::",
                notes=f"Focused unit tests for module '{mod_filter}'",
            )
        return TestMapping(
            file_path=posix_path,
            package=package,
            crate_dir=matching_prefix,
            category="unit-test",
            recommended_command=f"cargo test -p {package} --lib",
            notes=f"Crate unit tests for {package}",
        )

    # Fallback for crate
    return TestMapping(
        file_path=posix_path,
        package=package,
        crate_dir=matching_prefix,
        category="crate-test",
        recommended_command=f"cargo test -p {package}",
        notes=f"General crate test for {package}",
    )


def self_test() -> None:
    repo_root = Path(__file__).resolve().parent.parent

    test_cases = [
        # (Input path, expected package, expected substring in recommended_command)
        ("crates/omnigraph/tests/integration/traversal_indexed.rs", "omnigraph-engine", "--test integration traversal_indexed::"),
        ("crates/omnigraph/tests/writes.rs", "omnigraph-engine", "--test writes"),
        ("crates/omnigraph/tests/failpoints.rs", "omnigraph-engine", "--features failpoints --test failpoints"),
        ("crates/omnigraph/tests/system_column_upgrade.rs", "omnigraph-engine", "--features failpoints --test system_column_upgrade"),
        ("crates/omnigraph/tests/detached_commit_matrix.rs", "omnigraph-engine", "--features failpoints --test detached_commit_matrix"),
        ("crates/omnigraph-cluster/tests/identity_recovery.rs", "omnigraph-cluster", "--features failpoints --test identity_recovery"),
        ("crates/omnigraph/tests/integration/legacy_columns.rs", "omnigraph-engine", "--features failpoints --test integration legacy_columns::"),
        ("crates/omnigraph/src/exec/staging.rs", "omnigraph-engine", "--test writes"),
        ("crates/omnigraph-compiler/src/lexer.rs", "omnigraph-compiler", "--lib lexer::"),
        ("crates/omnigraph-compiler/src/query/parser_tests.rs", "omnigraph-compiler", "--lib parser::"),
        ("crates/omnigraph-catalog/src/tests.rs", "omnigraph-catalog", "--lib tests::"),
        ("crates/omnigraph-cli/src/main_tests.rs", "omnigraph-cli", "--lib tests::"),
        ("crates/omnigraph-bench/src/gqt_tests.rs", "omnigraph-bench", "--lib gqt_tests::"),
        ("crates/omnigraph-cli/src/operator_schema/schema_tests.rs", "omnigraph-cli", "--lib schema_tests::"),
        ("crates/omnigraph-gqt/cases/match_simple.gqt", "omnigraph-gqt", "gq_logic_tests -- match_simple"),
        ("crates/omnigraph-gqt/cases_slow/v2/issue_782_multi_hop_limit_stops_early.gqt", "omnigraph-gqt", "cargo run -p omnigraph-gqt --bin omnigraph-gqt --"),
        ("crates/omnigraph-dst/src/harness.rs", "omnigraph-dst", "(cd crates/omnigraph-dst && cargo test --locked)"),
        ("docs/dev/invariants.md", "docs", "scripts/check-docs.py"),
        ("skills/omnigraph/SKILL.md", "omnigraph-skill", "cli_schema_config::"),
    ]

    for path_str, expected_pkg, expected_cmd_substr in test_cases:
        mapping = resolve_mapping(path_str, repo_root)
        assert mapping.package == expected_pkg, f"Expected {expected_pkg}, got {mapping.package} for {path_str}"
        assert expected_cmd_substr in mapping.recommended_command, (
            f"Expected {expected_cmd_substr!r} in {mapping.recommended_command!r} for {path_str}"
        )

    print(f"self-test OK ({len(test_cases)} path mappings verified).")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file_path", nargs="?", help="Path to source or test file")
    parser.add_argument("--command-only", action="store_true", help="Print only the recommended cargo command")
    parser.add_argument("--json", action="store_true", help="Output mapping details as JSON")
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit")

    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0

    if not args.file_path:
        parser.error("file_path is required unless --self-test is specified")

    mapping = resolve_mapping(args.file_path)

    if args.command_only:
        print(mapping.recommended_command)
    elif args.json:
        print(json.dumps(asdict(mapping), indent=2))
    else:
        print(f"File:        {mapping.file_path}")
        print(f"Package:     {mapping.package}")
        print(f"Crate Dir:   {mapping.crate_dir}")
        print(f"Category:    {mapping.category}")
        print(f"Command:     {mapping.recommended_command}")
        if mapping.notes:
            print(f"Notes:       {mapping.notes}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
