#!/usr/bin/env python3
"""Fix Regression Gate: every issue a PR body closes by keyword needs a
matching regression addition in the diff, or the PR carries the `no-repro`
label. Specified in docs/rfcs/0045-gq-logic-tests.md (User and operational
behavior, "Fix-PR gate").

The gate reads only GitHub's own closing-keyword form (`fixes #123`,
`fixes: #123`, `fixes:#123`): case-insensitive, a word boundary before the
keyword, an optional colon, whitespace unless the colon is present, then
`#N`. Closings by URL, `owner/repo#N`, `GH-N`, a bare `fixes#123`, commit
message, or manual close pass unexamined and belong to review.

Label names are read as a comma-joined list, so a label whose own name
contains a comma could smuggle the waiver token; creating labels needs the
same triage rights as applying the real one. Run from the repository root:
AGENTS.md and git are resolved relative to the working directory.

A closed issue N is satisfied by an addition under `crates/*/tests/**`:
an added file whose path contains `issue_N`, or an added line containing
`issue_N` or `issue: N`, where N is followed by a non-digit or the end.

Exit 0 exactly when every keyword-closed issue has its match or the PR
carries `no-repro`, and AGENTS.md still names the logic-test corpus path.

Usage:
  check-fix-regression.py --body-file F --labels "a,b" --range BASE...HEAD
  check-fix-regression.py --self-test
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

CLOSING_KEYWORD = re.compile(
    r"(?<![A-Za-z0-9_])(?:close[sd]?|fix(?:es|ed)?|resolve[sd]?)(?::\s*|\s+)#(\d+)",
    re.IGNORECASE,
)
CORPUS_PATH_SENTENCE = "crates/omnigraph/tests/gq_logic_tests/"
WAIVER_LABEL = "no-repro"
TEST_PATHSPEC = ":(glob)crates/*/tests/**"


def closed_issues(body: str) -> list[str]:
    return sorted({str(int(n)) for n in CLOSING_KEYWORD.findall(body)}, key=int)


def issue_token(n: str) -> re.Pattern[str]:
    return re.compile(rf"issue_{n}(?!\d)")


def issue_line_patterns(n: str) -> list[re.Pattern[str]]:
    return [issue_token(n), re.compile(rf"issue: {n}(?!\d)")]


def added_files(range_: str) -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=A", range_, "--", TEST_PATHSPEC],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in out.stdout.splitlines() if line]


def added_lines(range_: str) -> list[str]:
    out = subprocess.run(
        ["git", "diff", "-U0", range_, "--", TEST_PATHSPEC],
        check=True,
        capture_output=True,
        text=True,
    )
    return [
        line[1:]
        for line in out.stdout.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]


def issue_satisfied(n: str, files: list[str], lines: list[str]) -> bool:
    token = issue_token(n)
    if any(token.search(path) for path in files):
        return True
    patterns = issue_line_patterns(n)
    return any(p.search(line) for line in lines for p in patterns)


def check_agents_md() -> bool:
    agents = Path("AGENTS.md")
    return agents.is_file() and CORPUS_PATH_SENTENCE in agents.read_text(encoding="utf-8")


def run_gate(body: str, labels: list[str], range_: str) -> int:
    ok = True
    if not check_agents_md():
        print(
            f"FAIL: AGENTS.md no longer names the corpus path `{CORPUS_PATH_SENTENCE}`; "
            "the contract sentence and this gate leave together"
        )
        ok = False
    issues = closed_issues(body)
    if not issues:
        print("ok: the PR body closes no issue by keyword")
        return 0 if ok else 1
    if WAIVER_LABEL in labels:
        print(f"ok: `{WAIVER_LABEL}` label waives the regression requirement for this PR")
        return 0 if ok else 1
    try:
        files = added_files(range_)
        lines = added_lines(range_)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        print(f"FAIL: git diff {range_} failed: {stderr or e}")
        return 1
    for n in issues:
        if issue_satisfied(n, files, lines):
            print(f"ok: issue #{n} has a matching regression addition")
        else:
            print(
                f"FAIL: the body closes #{n} but the diff adds no `issue_{n}` "
                f"test or `.gqt` case under crates/*/tests/; add one or apply "
                f"the `{WAIVER_LABEL}` label"
            )
            ok = False
    return 0 if ok else 1


def self_test() -> int:
    cases = [
        ("Closes #563", ["563"]),
        ("closes: #12 and fixes #7", ["7", "12"]),
        ("Resolved #99", ["99"]),
        ("hotfix #563", []),
        ("prefix #5", []),
        ("9fixes #5", []),
        ("_fixes #5", []),
        ("Closing #1", []),
        ("fixes:#123", ["123"]),
        ("fixes#123", []),
        ("fixes #0563", ["563"]),
        ("fixes #7 fixes #07", ["7"]),
        ("Fixes #563\r\nmore text", ["563"]),
        ("see https://github.com/o/r/issues/4", []),
        ("fixes o/r#4", []),
        ("fixes GH-4", []),
        ("FIX #8", ["8"]),
    ]
    for body, expected in cases:
        got = closed_issues(body)
        assert got == expected, f"closed_issues({body!r}) = {got}, expected {expected}"
    assert issue_satisfied("563", ["crates/omnigraph/tests/gq_logic_tests/issue_563_x.gqt"], [])
    assert not issue_satisfied("563", ["crates/omnigraph/tests/issue_5630_x.rs"], [])
    assert issue_satisfied("563", [], ["fn t_issue_563_case() {"])
    assert issue_satisfied("563", [], ["# issue: 563"])
    assert not issue_satisfied("563", [], ["# issue: 5630"])
    assert not issue_satisfied("563", [], ["# issue:563"])
    print("self-test ok")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--body-file")
    parser.add_argument("--labels", default="")
    parser.add_argument("--range")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.body_file or not args.range:
        parser.error("--body-file and --range are required unless --self-test")
    body = Path(args.body_file).read_text(encoding="utf-8")
    labels = [label.strip() for label in args.labels.split(",") if label.strip()]
    return run_gate(body, labels, args.range)


if __name__ == "__main__":
    sys.exit(main())
