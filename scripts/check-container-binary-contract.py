#!/usr/bin/env python3
"""Keep the CodeBuild archive aligned with the runtime image binary set."""

from __future__ import annotations

import re
import shlex
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = REPO_ROOT / "Dockerfile"
DOCKERIGNORE = REPO_ROOT / ".dockerignore"
PACKAGE_WORKFLOW = REPO_ROOT / ".github/workflows/omnigraph-package.yml"


def one(values: list[str], description: str) -> str:
    if len(values) != 1:
        raise SystemExit(f"expected exactly one {description}, found {len(values)}")
    return values[0]


def main() -> int:
    docker_text = DOCKERFILE.read_text()
    dockerignore_text = DOCKERIGNORE.read_text()
    workflow_text = PACKAGE_WORKFLOW.read_text()

    docker_pairs = re.findall(
        r"^COPY target/release/(\S+) /usr/local/bin/(\S+)$",
        docker_text,
        flags=re.MULTILINE,
    )
    if not docker_pairs or any(source != destination for source, destination in docker_pairs):
        raise SystemExit(f"unexpected Dockerfile release-binary COPY contract: {docker_pairs!r}")
    docker_binaries = {source for source, _ in docker_pairs}

    context_binaries = set(
        re.findall(
            r"^!target/release/(\S+)$",
            dockerignore_text,
            flags=re.MULTILINE,
        )
    )

    install_pairs = re.findall(
        r"^\s+install -m 0755 target/release/(\S+) release/(\S+)$",
        workflow_text,
        flags=re.MULTILINE,
    )
    if any(source != destination for source, destination in install_pairs):
        raise SystemExit(f"package install renames a runtime binary: {install_pairs!r}")
    installed_binaries = {source for source, _ in install_pairs}

    archive_line = one(
        [line.strip() for line in workflow_text.splitlines() if "tar -C release -czf" in line],
        "CodeBuild archive command",
    )
    archive_words = shlex.split(archive_line)
    archive_flag = archive_words.index("-czf")
    archived_binaries = set(archive_words[archive_flag + 2 :])

    if (
        docker_binaries != context_binaries
        or docker_binaries != installed_binaries
        or docker_binaries != archived_binaries
    ):
        raise SystemExit(
            "runtime binary contract drift:\n"
            f"  Dockerfile COPY: {sorted(docker_binaries)}\n"
            f"  Docker context: {sorted(context_binaries)}\n"
            f"  package install: {sorted(installed_binaries)}\n"
            f"  package archive: {sorted(archived_binaries)}"
        )

    print(f"Container/package binary contract OK: {', '.join(sorted(docker_binaries))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
