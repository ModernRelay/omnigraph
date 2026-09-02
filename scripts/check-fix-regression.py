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

A closed issue N is satisfied by an added `.gqt` case named `issue_N_*` at
the top level of the logic-test corpus, an added `# issue: N` header line
in a corpus `.gqt`, or an added Rust test definition: a function whose
name carries `issue_N`, with an added `#[test]` or `#[<path>::test]`
attribute line (`#[tokio::test(...)]` included) directly above it in the
same hunk, other `#[...]` attribute and `//` comment lines allowed in
between (a blank line, a block comment, or an attribute split across
lines breaks adjacency), in a
top-level test target `crates/*/tests/<name>.rs` or an in-source module
under `crates/*/src/`; helper and fixture modules under `tests/<dir>/`
never match, and a plain function, however named, never matches. What a
match guarantees differs by shape: a corpus match ran green in the
required `GQ Logic Tests` job; a Rust match is a test-attributed
definition, not a run. A pull request runs only the corpus walker and the
`omnigraph-server` aws-feature suite among Rust test targets (`Test
Workspace` runs post-merge), and workspace clippy refuses an unreferenced
private function but not an `#[ignore]`d or cfg-gated one, so whether
that test runs in the suite and asserts the right thing stays with review.
Comments, strings, and fixture lines mentioning the issue do not count.
N is always followed by a non-digit or the end. Named residue: a
definition inside an added block comment or raw string still matches
(line-based parsing cannot see multi-line context); that evasion, like a
test that asserts nothing, is deliberate and stays with review.

Exit 0 exactly when every keyword-closed issue has its match or the PR
carries `no-repro`, and AGENTS.md still names the logic-test corpus path.

Usage:
  check-fix-regression.py --body-file F --labels "a,b" --range BASE...HEAD
  check-fix-regression.py --self-test
"""

from __future__ import annotations

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
PATHSPECS = (":(glob)crates/*/tests/**", ":(glob)crates/*/src/**")


def closed_issues(body: str) -> list[str]:
    return sorted({str(int(n)) for n in CLOSING_KEYWORD.findall(body)}, key=int)


def issue_token(n: str) -> re.Pattern[str]:
    return re.compile(rf"issue_{n}(?!\d)")


CORPUS_DIR_PREFIX = "crates/omnigraph/tests/gq_logic_tests/"
# One path segment after `tests/` (a top-level target) or any `.rs` under `src/`.
RUST_FN_PATH = re.compile(r"^crates/[^/]+/(?:tests/[^/]+\.rs|src/.+\.rs)$")
RUST_FN_DEF = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?"
    r'(?:extern\s+"[^"]*"\s+)?fn\s+([A-Za-z0-9_]+)'
)
# `#[test]`, `#[tokio::test]`, `#[tokio::test(flavor = "...")]`; not
# `#[cfg(test)]`, `#[test_case(...)]`, or `#[serial_test::serial]`.
TEST_ATTR = re.compile(r"^\s*#\[(?:[A-Za-z0-9_]+::)*test\b(?!_)")
# Lines the attribute lookback may step over: other attributes and comments.
ATTR_OR_COMMENT = re.compile(r"^\s*(?:#\[|//)")
# Inserted into the added-line list at every hunk header, so the lookback
# never joins lines that are not adjacent in the new file.
HUNK_BREAK = ("", "")


def added_files(range_: str) -> list[str]:
    out = subprocess.run(
        [
            "git",
            "-c",
            "core.quotePath=false",
            "diff",
            "--name-only",
            "--diff-filter=A",
            range_,
            "--",
            *PATHSPECS,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in out.stdout.splitlines() if line]


def parse_diff(diff_text: str) -> tuple[list[tuple[str, str]], list[str]]:
    """Splits a -U0 diff into path-attributed added lines and bare removed
    lines. A `+++ b/` marker counts as a file header only directly after a
    `--- ` line: an added CONTENT line `++ b/x` also renders as `+++ b/x`,
    and honoring it would let a diff spoof its own file attribution. Every
    `@@` hunk header contributes a `HUNK_BREAK` entry to the added list."""
    current: str | None = None
    previous = ""
    added: list[tuple[str, str]] = []
    removed: list[str] = []
    for line in diff_text.splitlines():
        if line.startswith("+++") and previous.startswith("--- "):
            current = line[len("+++ b/") :] if line.startswith("+++ b/") else None
        elif line.startswith("@@"):
            added.append(HUNK_BREAK)
        elif line.startswith("+") and not line.startswith("+++") and current is not None:
            added.append((current, line[1:]))
        elif line.startswith("-") and not line.startswith("---"):
            removed.append(line[1:])
        previous = line
    return added, removed


def diff_changes(range_: str) -> tuple[list[tuple[str, str]], list[str]]:
    out = subprocess.run(
        ["git", "-c", "core.quotePath=false", "diff", "-U0", range_, "--", *PATHSPECS],
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_diff(out.stdout)


def removed_fn_names(removed: list[str]) -> set[str]:
    names = set()
    for text in removed:
        m = RUST_FN_DEF.match(text)
        if m:
            names.add(m.group(1))
    return names


def issue_satisfied(
    n: str,
    files: list[str],
    lines: list[tuple[str, str]],
    removed_fns: frozenset[str] | set[str] = frozenset(),
) -> bool:
    token = issue_token(n)
    for path in files:
        if corpus_case(path) and Path(path).name.startswith(f"issue_{n}_"):
            return True
    header = re.compile(rf"^\s*#\s*issue:\s*{n}(?!\d)\s*$")
    for i, (path, text) in enumerate(lines):
        if corpus_case(path):
            if header.match(text):
                return True
        elif RUST_FN_PATH.match(path):
            m = RUST_FN_DEF.match(text)
            if (
                m
                and token.search(m.group(1))
                and not m.group(1).startswith("_")
                and m.group(1) not in removed_fns
                and not text.rstrip().endswith(";")
                and test_attributed(lines, i)
            ):
                return True
    return False


def test_attributed(lines: list[tuple[str, str]], i: int) -> bool:
    """True when an added test attribute sits directly above added line `i`
    in the same file and hunk, stepping over other attributes and comments.
    A `HUNK_BREAK` or a different path ends the walk: an attribute that is
    not adjacent in the new file cannot vouch for the definition."""
    path = lines[i][0]
    j = i - 1
    while j >= 0 and lines[j][0] == path and ATTR_OR_COMMENT.match(lines[j][1]):
        if TEST_ATTR.match(lines[j][1]):
            return True
        j -= 1
    return False


def corpus_case(path: str) -> bool:
    rest = path[len(CORPUS_DIR_PREFIX) :] if path.startswith(CORPUS_DIR_PREFIX) else ""
    return bool(rest) and "/" not in rest and rest.endswith(".gqt")


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
        lines, removed = diff_changes(range_)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        print(f"FAIL: git diff {range_} failed: {stderr or e}")
        return 1
    removed_fns = removed_fn_names(removed)
    for n in issues:
        if issue_satisfied(n, files, lines, removed_fns):
            print(f"ok: issue #{n} has a matching regression addition")
        else:
            print(
                f"FAIL: the body closes #{n} but the diff adds no `.gqt` case "
                f"named `issue_{n}_*` or carrying `# issue: {n}` under "
                f"{CORPUS_DIR_PREFIX}, and no `#[test]`-attributed function named "
                f"for `issue_{n}` in crates/*/tests/<name>.rs or crates/*/src/; "
                f"add one or apply the `{WAIVER_LABEL}` label"
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
    corpus = "crates/omnigraph/tests/gq_logic_tests/issue_563_x.gqt"
    rust = "crates/omnigraph/tests/search.rs"
    assert issue_satisfied("563", [corpus], [])
    assert not issue_satisfied("563", ["crates/omnigraph/tests/fixtures/issue_563_x.gqt"], [])
    assert not issue_satisfied("563", ["crates/omnigraph/tests/repro_issue_563.rs"], [])
    assert issue_satisfied("563", [], [(rust, "#[test]"), (rust, "fn t_issue_563_case() {")])
    assert issue_satisfied(
        "563", [], [(rust, "    #[tokio::test]"), (rust, "    async fn repro_issue_563() {")]
    )
    # A plain function, however named, is not a regression test.
    assert not issue_satisfied("563", [], [(rust, "fn t_issue_563_case() {")])
    assert not issue_satisfied("563", [], [(rust, "    async fn repro_issue_563() {")])
    assert not issue_satisfied("563", [], [(rust, "pub fn issue_563_helper(x: u32) -> u32 {")])
    # Attribute variants: flavor args, stacked attributes, doc comments between.
    assert issue_satisfied(
        "563",
        [],
        [
            (rust, '#[tokio::test(flavor = "multi_thread")]'),
            (rust, "#[ignore]"),
            (rust, "/// Regression for the capped-scan underfill."),
            (rust, "async fn issue_563_underfill() {"),
        ],
    )
    assert issue_satisfied("563", [], [(rust, "#[test]"), (rust, "#[serial_test::serial]"), (rust, "fn issue_563() {")])
    # Not test attributes: cfg gates, look-alike names, unrelated attributes.
    assert not issue_satisfied("563", [], [(rust, "#[cfg(test)]"), (rust, "fn issue_563() {")])
    assert not issue_satisfied("563", [], [(rust, "#[test_case(1)]"), (rust, "fn issue_563() {")])
    assert not issue_satisfied("563", [], [(rust, "#[serial_test::serial]"), (rust, "fn issue_563() {")])
    assert not issue_satisfied("563", [], [(rust, "#[allow(dead_code)]"), (rust, "fn issue_563() {")])
    # The attribute must be adjacent: a blank line, a hunk break, or another
    # file between attribute and definition breaks the vouching.
    assert not issue_satisfied("563", [], [(rust, "#[test]"), (rust, ""), (rust, "fn issue_563() {")])
    assert not issue_satisfied("563", [], [(rust, "#[test]"), HUNK_BREAK, (rust, "fn issue_563() {")])
    assert not issue_satisfied(
        "563", [], [("crates/omnigraph/tests/other.rs", "#[test]"), (rust, "fn issue_563() {")]
    )
    assert not issue_satisfied("563", [], [(rust, "fn issue_563() {"), (rust, "#[test]")])
    assert TEST_ATTR.match("#[test]") and TEST_ATTR.match("  #[tokio::test]")
    assert TEST_ATTR.match('#[tokio::test(flavor = "multi_thread", worker_threads = 2)]')
    assert TEST_ATTR.match("#[async_std::test]")
    assert not TEST_ATTR.match("#[cfg(test)]")
    assert not TEST_ATTR.match("#[test_case(1)]")
    assert not TEST_ATTR.match("#[serial_test::serial]")
    assert not TEST_ATTR.match("#[tests]")
    assert not issue_satisfied("563", [], [(rust, "// see issue_563")])
    assert not issue_satisfied("563", [], [(rust, "let s = \"issue_563\";")])
    assert not issue_satisfied("563", [], [(rust, "fn t_issue_5630() {")])
    assert issue_satisfied("563", [], [(corpus, "# issue: 563")])
    assert not issue_satisfied("563", [], [(corpus, "# issue: 0563")])
    assert not issue_satisfied("563", [], [(corpus, "# issue: 5630")])
    assert not issue_satisfied("563", [], [(corpus, "issue: 563 in prose")])
    assert not issue_satisfied(
        "563", ["crates/omnigraph-cli/tests/gq_logic_tests/issue_563_x.gqt"], []
    )
    assert not issue_satisfied(
        "563", ["crates/omnigraph/tests/gq_logic_tests/nested/issue_563_x.gqt"], []
    )
    assert not issue_satisfied(
        "563", ["crates/omnigraph/tests/gq_logic_tests/regression_issue_563.gqt"], []
    )
    assert not issue_satisfied(
        "563", [], [("crates/omnigraph/tests/fixtures/issue_563_gen.rs", "fn t_issue_563() {}")]
    )
    assert not issue_satisfied(
        "563", [], [("crates/omnigraph/tests/helpers/mod.rs", "fn t_issue_563() {}")]
    )
    assert not issue_satisfied("563", [], [(rust, "#[test]"), (rust, "fn _issue_563() {}")])
    assert issue_satisfied("563", [], [(rust, "#[test]"), (rust, "const fn check_issue_563() {}")])
    assert not issue_satisfied("563", [], [(rust, "#[test]"), (rust, "fn t_issue_563(&self);")])
    assert not issue_satisfied(
        "563",
        [],
        [(rust, "#[test]"), (rust, "fn t_issue_563_case() {")],
        removed_fns={"t_issue_563_case"},
    )
    src = "crates/omnigraph/src/exec/query.rs"
    assert issue_satisfied("563", [], [(src, "    #[test]"), (src, "    fn regression_issue_563() {")])
    assert issue_satisfied(
        "563",
        [],
        [("crates/omnigraph-dst/src/lib.rs", "#[tokio::test]"), ("crates/omnigraph-dst/src/lib.rs", "async fn issue_563() {")],
    )
    assert not issue_satisfied("563", [], [(src, "    fn regression_issue_563() {")])
    assert not issue_satisfied("563", [], [(src, "// regression_issue_563 lives below")])
    assert not issue_satisfied(
        "563", [], [("crates/omnigraph/src/query.pest", "#[test]"), ("crates/omnigraph/src/query.pest", "fn t_issue_563() {")]
    )
    assert not issue_satisfied(
        "563", [], [("crates/omnigraph/benches/b.rs", "#[test]"), ("crates/omnigraph/benches/b.rs", "fn t_issue_563() {")]
    )
    assert not issue_satisfied("563", ["crates/omnigraph/src/gq_logic_tests/issue_563_x.gqt"], [])
    assert RUST_FN_PATH.match("crates/omnigraph/tests/search.rs")
    assert RUST_FN_PATH.match("crates/omnigraph/src/a/b/c.rs")
    assert not RUST_FN_PATH.match("crates/omnigraph/tests/helpers/mod.rs")
    assert not RUST_FN_PATH.match("crates/omnigraph/src/lib.md")
    assert RUST_FN_PATH.match("crates/omnigraph/src/bin/omnigraph.rs")
    assert not RUST_FN_PATH.match("crates/omnigraph/tests/search/main.rs")

    spoof_diff = "\n".join(
        [
            "diff --git a/crates/omnigraph/tests/search.rs b/crates/omnigraph/tests/search.rs",
            "--- a/crates/omnigraph/tests/search.rs",
            "+++ b/crates/omnigraph/tests/search.rs",
            "@@ -0,0 +1,3 @@",
            "+/*",
            "+++ b/crates/omnigraph/tests/gq_logic_tests/fake.gqt",
            "+# issue: 999",
            "+*/",
        ]
    )
    added, removed = parse_diff(spoof_diff)
    assert all(
        path == "crates/omnigraph/tests/search.rs" for path, _ in added if (path, _) != HUNK_BREAK
    ), added
    assert not issue_satisfied("999", [], added)

    # A real -U0 shape: attribute and definition in one hunk count; the same
    # definition after a hunk break, with the attribute in the prior hunk, does not.
    adjacent_diff = "\n".join(
        [
            "diff --git a/crates/omnigraph/tests/search.rs b/crates/omnigraph/tests/search.rs",
            "--- a/crates/omnigraph/tests/search.rs",
            "+++ b/crates/omnigraph/tests/search.rs",
            "@@ -40,0 +41,2 @@",
            "+#[tokio::test]",
            "+async fn issue_563_adjacent() {",
        ]
    )
    added, _ = parse_diff(adjacent_diff)
    assert issue_satisfied("563", [], added)
    split_diff = "\n".join(
        [
            "diff --git a/crates/omnigraph/tests/search.rs b/crates/omnigraph/tests/search.rs",
            "--- a/crates/omnigraph/tests/search.rs",
            "+++ b/crates/omnigraph/tests/search.rs",
            "@@ -40,0 +41 @@",
            "+#[tokio::test]",
            "@@ -60,0 +62 @@",
            "+async fn issue_563_split() {",
        ]
    )
    added, _ = parse_diff(split_diff)
    assert not issue_satisfied("563", [], added)

    multi_diff = "\n".join(
        [
            "diff --git a/a.gqt b/a.gqt",
            "--- a/crates/omnigraph/tests/gq_logic_tests/a.gqt",
            "+++ b/crates/omnigraph/tests/gq_logic_tests/a.gqt",
            "@@ -0,0 +1 @@",
            "+# issue: 7",
            "diff --git a/old.rs b/old.rs",
            "--- a/crates/omnigraph/tests/old.rs",
            "+++ /dev/null",
            "@@ -1,2 +0,0 @@",
            "-fn t_issue_8_gone() {}",
            "-fn keep() {}",
        ]
    )
    added, removed = parse_diff(multi_diff)
    assert [a for a in added if a != HUNK_BREAK] == [
        ("crates/omnigraph/tests/gq_logic_tests/a.gqt", "# issue: 7")
    ], added
    assert removed_fn_names(removed) == {"t_issue_8_gone", "keep"}, removed

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
