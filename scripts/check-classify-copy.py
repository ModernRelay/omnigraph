#!/usr/bin/env python3
"""Asserts that every workflow under `.github/workflows/` carrying a
`classify_changes` job holds a verbatim copy of `ci.yml`'s, modulo the job's
display `name:` line. Candidates are discovered by a loose scan (`*.yml` and
`*.yaml`, any line whose first token is `classify_changes:`), never listed
here, so a new workflow that gates on the classification is checked the
moment it carries the job; a candidate the strict block extraction cannot
read fails closed instead of dropping out of the check. A job cannot depend
on another workflow's job, so the change classification lives in each
workflow that gates on it; `ci.yml` is the source of truth, and this check is
what makes the copies enforced rather than promised. Run from the repository
root. Exit 0 exactly when every copy matches and at least one copy exists.
`--self-test` proves the negative controls: a drifted `.yaml` copy, a drifted
copy behind a `classify_changes: # comment` key, a source-only tree.
"""

from __future__ import annotations

import difflib
import re
import sys
import tempfile
from pathlib import Path

SOURCE = Path(".github/workflows/ci.yml")
# The job block: from its key to the next two-space-indented key.
JOB_BLOCK = re.compile(r"^  classify_changes:\n(?:(?!^(?:  \S|\S)).*\n?)*", re.MULTILINE)
# Any spelling of the key at any indent, trailing comment or not: the
# discovery predicate, deliberately looser than the extraction above.
CANDIDATE = re.compile(r"^\s*classify_changes\s*:", re.MULTILINE)
DISPLAY_NAME = re.compile(r"^    name: .*$", re.MULTILINE)
SUPPORTED = "supported spelling: `  classify_changes:` at the two-space job indent, nothing after the colon"


def job_block(path: Path) -> list[str] | None:
    match = JOB_BLOCK.search(path.read_text(encoding="utf-8"))
    if not match:
        return None
    lines = DISPLAY_NAME.sub("    name: <display name>", match.group(0), count=1).splitlines()
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


def workflows(directory: Path) -> list[Path]:
    return sorted(list(directory.glob("*.yml")) + list(directory.glob("*.yaml")))


def candidates(directory: Path, source: Path) -> list[Path]:
    """Every workflow other than the source whose text carries the key."""
    return [
        path
        for path in workflows(directory)
        if path != source and CANDIDATE.search(path.read_text(encoding="utf-8"))
    ]


def check(directory: Path, source: Path) -> tuple[bool, list[str]]:
    """(all copies match, report lines)."""
    lines: list[str] = []
    source_block = job_block(source)
    if source_block is None:
        return False, [f"FAIL: no `classify_changes` job in {source} ({SUPPORTED})"]
    found = candidates(directory, source)
    if not found:
        return False, [f"FAIL: no workflow under {directory} carries a classify_changes copy"]
    ok = True
    for copy in found:
        block = job_block(copy)
        if block is None:
            lines.append(f"FAIL: {copy} carries a classify_changes key this check cannot read ({SUPPORTED})")
            ok = False
            continue
        if source_block == block:
            lines.append(f"ok: {copy} classify_changes matches {source} ({len(source_block)} lines)")
            continue
        lines.extend(difflib.unified_diff(source_block, block, str(source), str(copy), lineterm=""))
        lines.append(f"FAIL: {copy} classify_changes drifted from {source}, the source of truth")
        ok = False
    return ok, lines


BLOCK = "  classify_changes:\n    name: Classify Changes\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo classify\n"


def self_test() -> int:
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        source = directory / "ci.yml"
        source.write_text("jobs:\n" + BLOCK + "  other:\n    runs-on: ubuntu-latest\n")
        (directory / "good.yml").write_text("jobs:\n" + BLOCK.replace("Classify Changes", "Classify Changes (Good)"))
        (directory / "drifted.yaml").write_text("jobs:\n" + BLOCK.replace("echo classify", "echo drifted"))
        (directory / "commented.yml").write_text("jobs:\n" + BLOCK.replace("classify_changes:\n", "classify_changes: # note\n").replace("echo classify", "echo drifted"))
        (directory / "unrelated.yml").write_text("jobs:\n  build:\n    runs-on: ubuntu-latest\n")
        ok, lines = check(directory, source)
        report = "\n".join(lines)
        checks = [
            ("drifted .yaml copy is red", "FAIL: " + str(directory / "drifted.yaml") in report),
            ("commented-key copy is red (unreadable spelling)", "cannot read" in report and "commented.yml" in report),
            ("healthy copy is green", "ok: " + str(directory / "good.yml") in report),
            ("unrelated workflow is not a candidate", "unrelated.yml" not in report),
            ("verdict is red", not ok),
        ]
        alone = directory / "alone"
        alone.mkdir()
        (alone / "ci.yml").write_text("jobs:\n" + BLOCK)
        ok_alone, lines_alone = check(alone, alone / "ci.yml")
        checks.append(("source-only tree is red", not ok_alone and "no workflow" in "\n".join(lines_alone)))
        for name, passed in checks:
            failures += not passed
            print(f"{'ok  ' if passed else 'FAIL'} self-test {name}")
    return failures


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return 1 if self_test() else 0
    if argv:
        print(__doc__)
        return 2
    ok, lines = check(SOURCE.parent, SOURCE)
    print("\n".join(lines))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
