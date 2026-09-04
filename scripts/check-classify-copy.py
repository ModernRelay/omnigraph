#!/usr/bin/env python3
"""Asserts that `gq-logic-tests.yml`'s `classify_changes` job is a verbatim
copy of `ci.yml`'s, modulo the job's display `name:` line. A job cannot
depend on another workflow's job, so the documentation-only classification
lives in both files; `ci.yml` is the source of truth, and this check is what
makes the copy enforced rather than promised. Run from the repository root.
Exit 0 exactly when the two job blocks match.
"""

from __future__ import annotations

import difflib
import re
import sys
from pathlib import Path

SOURCE = Path(".github/workflows/ci.yml")
COPY = Path(".github/workflows/gq-logic-tests.yml")
# The job block: from its key to the next two-space-indented key.
JOB_BLOCK = re.compile(r"^  classify_changes:\n(?:(?!^(?:  \S|\S)).*\n?)*", re.MULTILINE)
DISPLAY_NAME = re.compile(r"^    name: .*$", re.MULTILINE)


def job_block(path: Path) -> list[str]:
    match = JOB_BLOCK.search(path.read_text(encoding="utf-8"))
    if not match:
        sys.exit(f"FAIL: no `classify_changes` job in {path}")
    lines = DISPLAY_NAME.sub("    name: <display name>", match.group(0), count=1).splitlines()
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


def main() -> int:
    source = job_block(SOURCE)
    copy = job_block(COPY)
    if source == copy:
        print(f"ok: {COPY} classify_changes matches {SOURCE} ({len(source)} lines)")
        return 0
    diff = difflib.unified_diff(source, copy, str(SOURCE), str(COPY), lineterm="")
    print("\n".join(diff))
    print(f"FAIL: {COPY} classify_changes drifted from {SOURCE}, the source of truth")
    return 1


if __name__ == "__main__":
    sys.exit(main())
