#!/usr/bin/env python3
"""Enforce the one-way Azure admission dependency boundary."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ADMISSION = "omnigraph-azure-admission"
STORAGE = "omnigraph-storage"
FORBIDDEN_CONSUMERS = {
    "omnigraph-storage",
    "omnigraph-engine",
    "omnigraph-cluster",
    "omnigraph-server",
    "omnigraph-cli",
}
RUST_ADMISSION_REFERENCE = re.compile(
    r"\bomnigraph_azure_admission::|"
    r"^\s*(?:pub\s+)?use\s+omnigraph_azure_admission\b|"
    r"^\s*extern\s+crate\s+omnigraph_azure_admission\b",
    flags=re.MULTILINE,
)


def dependency_path(
    graph: dict[str, set[str]], start: str, target: str
) -> list[str] | None:
    pending = [(start, [start])]
    visited: set[str] = set()
    while pending:
        package, path = pending.pop(0)
        if package in visited:
            continue
        visited.add(package)
        if package == target:
            return path
        pending.extend(
            (dependency, [*path, dependency])
            for dependency in sorted(graph.get(package, set()))
            if dependency not in visited
        )
    return None


def check_path_finder() -> None:
    fixture = {
        "engine": {"middle"},
        "middle": {"admission"},
        "storage": set(),
    }
    assert dependency_path(fixture, "engine", "admission") == [
        "engine",
        "middle",
        "admission",
    ]
    assert dependency_path(fixture, "storage", "admission") is None
    assert RUST_ADMISSION_REFERENCE.search("use omnigraph_azure_admission::Lease;")
    assert RUST_ADMISSION_REFERENCE.search("omnigraph_azure_admission::run()")
    assert not RUST_ADMISSION_REFERENCE.search(
        'const RESERVED: &str = "__omnigraph_azure_admission/v1";'
    )


def main() -> int:
    check_path_finder()
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=REPO_ROOT,
            text=True,
        )
    )
    packages = {package["name"]: package for package in metadata["packages"]}
    required = FORBIDDEN_CONSUMERS | {ADMISSION}
    missing = sorted(required - packages.keys())
    if missing:
        raise SystemExit(f"workspace metadata is missing required packages: {missing}")

    workspace_names = set(packages)
    graph = {
        name: {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"] in workspace_names
        }
        for name, package in packages.items()
    }

    failures: list[str] = []
    for consumer in sorted(FORBIDDEN_CONSUMERS):
        path = dependency_path(graph, consumer, ADMISSION)
        if path is not None:
            failures.append("dependency path: " + " -> ".join(path))

    admission_dependencies = graph[ADMISSION]
    if STORAGE not in admission_dependencies:
        failures.append(f"{ADMISSION} must depend downward on {STORAGE}")
    forbidden_upward = sorted(admission_dependencies & (FORBIDDEN_CONSUMERS - {STORAGE}))
    if forbidden_upward:
        failures.append(
            f"{ADMISSION} has forbidden upward dependencies: {forbidden_upward}"
        )

    for consumer in sorted(FORBIDDEN_CONSUMERS):
        crate_root = Path(packages[consumer]["manifest_path"]).parent
        candidates = [crate_root / "Cargo.toml", *sorted(crate_root.rglob("*.rs"))]
        for candidate in candidates:
            text = candidate.read_text()
            if (
                candidate.name == "Cargo.toml"
                and "omnigraph-azure-admission" in text
            ) or (
                candidate.suffix == ".rs" and RUST_ADMISSION_REFERENCE.search(text)
            ):
                failures.append(
                    f"forbidden admission reference: {candidate.relative_to(REPO_ROOT)}"
                )

    if failures:
        print("Azure admission dependency boundary violations:")
        for failure in failures:
            print(f"  {failure}")
        return 1

    print(
        "Azure admission boundary OK: admission -> storage; "
        "storage/engine/cluster/server/CLI cannot depend upward"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
