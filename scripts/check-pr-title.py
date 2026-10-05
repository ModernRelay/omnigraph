#!/usr/bin/env python3
"""Release Note Gate: the pull request title reads `type(scope)!: summary`,
and a user-visible pull request adds a release note.

`feat`, `fix` and `perf` add at least one `changelog.d/<slug>.<category>.md`
note; a breaking title (`!`) adds a `.breaking.md` note. GitHub's own revert
title, `Revert "<valid title>"`, counts as a revert. The `skip-changelog`
label waives the note requirement, never the title rule. The workflow runs
this script from the base branch and passes the head only as a diff range.
`--self-test` runs the in-memory cases.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys

TYPES = ("feat", "fix", "perf", "refactor", "docs", "test", "ci", "build", "chore", "revert", "rfc", "bench", "release")
TITLE = re.compile(rf"^(?P<type>{'|'.join(TYPES)})(?:\((?P<scope>[a-z0-9][a-z0-9, -]*)\))?(?P<bang>!)?: \S")
REVERT = re.compile(r'^Revert "(?P<inner>.+)"$')
NEEDS_NOTE = frozenset({"feat", "fix", "perf"})
# Must equal release_notes.CATEGORIES; scripts/test_release_notes.py checks it.
CATEGORIES = ("breaking", "added", "changed", "fixed", "performance", "deprecated", "removed")
NOTE_PATH = re.compile(rf"changelog\.d/[a-z0-9]+(?:-[a-z0-9]+)*\.({'|'.join(CATEGORIES)})\.md")
SKIP_LABEL = "skip-changelog"
GUIDE = "docs/dev/documentation.md#release-notes"


def check(title: str, labels: set[str], added: list[str]) -> list[str]:
    revert = REVERT.match(title)
    if revert and TITLE.match(revert["inner"]):
        return []
    match = TITLE.match(title)
    if not match:
        return [f"title {title!r} must read `type(scope)!: summary` with a lowercase type from "
                f"{', '.join(TYPES)}; the scope and `!` are optional (see {GUIDE})"]
    if SKIP_LABEL in labels:
        return []
    notes = [path for path in added if NOTE_PATH.fullmatch(path)]
    errors = []
    if match["type"] in NEEDS_NOTE and not notes:
        errors.append(f"a `{match['type']}` pull request adds a changelog.d note, or carries the "
                      f"`{SKIP_LABEL}` label (see {GUIDE})")
    if match["bang"] and not any(path.endswith(".breaking.md") for path in notes):
        errors.append(f"a breaking (`!`) pull request adds a changelog.d/<slug>.breaking.md note (see {GUIDE})")
    return errors


def added_paths(range_: str) -> list[str]:
    result = subprocess.run(
        ["git", "-c", "core.quotePath=false", "diff", "--name-only", "--no-renames", "--diff-filter=A",
         range_, "--", "changelog.d/"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.splitlines()


CASES = (
    # (title, labels, added paths, expected error substring, or None for a pass)
    ("feat(gq): add list membership", set(), ["changelog.d/gq-in.added.md"], None),
    ("feat(gq): add list membership", set(), [], "adds a changelog.d note"),
    ("feat(gq): add list membership", {SKIP_LABEL}, [], None),
    ("fix: refuse bad dates", set(), ["changelog.d/dates.fixed.md"], None),
    ("perf(merge): skip the walk", set(), [], "adds a changelog.d note"),
    ("feat!: new contract", set(), ["changelog.d/contract.added.md"], "breaking.md note"),
    ("feat!: new contract", set(), ["changelog.d/contract.breaking.md"], None),
    ("refactor(blob,schema)!: remove flag", set(), ["changelog.d/flag.breaking.md"], None),
    ("refactor(blob,schema)!: remove flag", {SKIP_LABEL}, [], None),
    ("refactor: inline the contract", set(), [], None),
    ("docs(rfcs): name RFCs by date", set(), [], None),
    ("rfc: shared schema gate", set(), [], None),
    ("bench: concurrent writes", set(), [], None),
    ("release: v0.13.0", set(), [], None),
    ('Revert "feat(gq): add list membership"', set(), [], None),
    ('Revert "Add a thing"', set(), [], "must read"),
    ("Add a thing", set(), [], "must read"),
    ("Feat: add a thing", set(), [], "must read"),
    ("feat(GQ): add a thing", set(), [], "must read"),
    ("feat : add a thing", set(), [], "must read"),
    ("feat:add a thing", set(), [], "must read"),
    ("feat: ", set(), [], "must read"),
    ("engine: shared schema gate", set(), [], "must read"),
    ("feat: add a thing", set(), ["changelog.d/v0.13.0.md"], "adds a changelog.d note"),
    ("feat: add a thing", set(), ["changelog.d/x.other.md"], "adds a changelog.d note"),
)


def self_test() -> int:
    failures = 0
    for title, labels, added, expected in CASES:
        errors = check(title, labels, added)
        ok = not errors if expected is None else any(expected in error for error in errors)
        if not ok:
            failures += 1
            print(f"FAIL: {title!r} labels={sorted(labels)} added={added}: expected {expected!r}, got {errors}")
    print(f"self-test: {len(CASES) - failures}/{len(CASES)} cases pass")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--title")
    parser.add_argument("--labels", default="", help="comma-separated pull request labels")
    parser.add_argument("--range", dest="range_", help="BASE...HEAD")
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()
    if args.title is None or args.range_ is None:
        parser.error("--title and --range are required without --self-test")
    labels = {label.strip() for label in args.labels.split(",") if label.strip()}
    errors = check(args.title, labels, added_paths(args.range_))
    for error in errors:
        print(f"::error::{error}")
    if not errors:
        print("ok: the title and release notes satisfy the Release Note Gate")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
