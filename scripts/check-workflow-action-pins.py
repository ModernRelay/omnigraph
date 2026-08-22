#!/usr/bin/env python3
"""Reject mutable third-party GitHub Action and reusable-workflow refs."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / ".github" / "workflows"
USES_RE = re.compile(r"^\s*(?:-\s*)?uses:\s*([^\s#]+)")
FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")


def main() -> int:
    failures: list[str] = []
    checked = 0

    workflows = (*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml"))
    for workflow in sorted(workflows):
        for line_number, line in enumerate(workflow.read_text().splitlines(), start=1):
            match = USES_RE.match(line)
            if match is None:
                continue

            reference = match.group(1).strip("'\"")
            if reference.startswith(("./", "docker://")):
                continue

            checked += 1
            if "@" not in reference:
                failures.append(
                    f"{workflow.relative_to(REPO_ROOT)}:{line_number}: "
                    f"external use has no ref: {reference}"
                )
                continue

            revision = reference.rsplit("@", 1)[1]
            if FULL_SHA_RE.fullmatch(revision) is None:
                failures.append(
                    f"{workflow.relative_to(REPO_ROOT)}:{line_number}: "
                    f"pin {reference} to a full 40-character commit SHA"
                )

    if failures:
        print("Mutable GitHub Actions references found:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print(f"GitHub Actions refs OK ({checked} external uses, all commit-pinned).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
