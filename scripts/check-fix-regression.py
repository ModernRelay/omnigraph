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

A closed issue N is satisfied by a regression that is added or
strengthened. Corpus shape: a `.gqt` case named `issue_N_*` at the top
level of the logic-test corpus, new, or modified with at least one added
body line (a line that is neither a `#` header line nor a `//` comment).
Rust shapes, in a top-level test target `<crates|tools>/*/tests/<name>.rs`
or an in-source module under `<crates|tools>/*/src/`: an added test
definition, a function whose name carries `issue_N`, with an added
`#[test]` or `#[<path>::test]` attribute line (`#[tokio::test(...)]`
included) directly above it in the same hunk, other `#[...]` attribute
and `//` comment lines allowed in between (a blank line, a block comment,
or an attribute split across lines breaks adjacency); or a strengthened
one, an added line carrying at least one alphanumeric character, not a
comment or an attribute, inside the body of an existing test-attributed
function whose name carries `issue_N`, located by the hunk's new-file line
number in the file at the head commit. An owner test not named for the
issue is extended by renaming it to carry `issue_N` in the same change
(the rename alone never counts; the rename plus the assertion does).
Helper and fixture modules under `tests/<dir>/` never match, and a plain
function, however named, never matches. Owners the gate does not
recognize, Python and shell scripts among them, satisfy it only through
the `no-repro` label, which a maintainer applies. What a match guarantees
differs by shape: a corpus match ran green in the required `GQ Logic
Tests` job; a Rust match is a test-attributed definition or an edit inside
one, not a run. A pull request runs only the corpus walker and the
`omnigraph-server` aws-feature suite among Rust test targets (`Test
Workspace` runs post-merge), and workspace clippy refuses an unreferenced
private function but not an `#[ignore]`d or cfg-gated one, so whether
that test runs in the suite and asserts the right thing stays with review.
Comments, strings, and fixture lines mentioning the issue do not count.
N is always followed by a non-digit or the end. Named residue: a
definition inside an added block comment or raw string still matches
(line-based parsing cannot see multi-line context); the enclosing item of
a strengthened line is found by brace counting with string literals, char
literals, and `//` comments blanked, so a brace inside a raw string or a
multi-line block comment can mislead it; those evasions, like a test that
asserts nothing, are deliberate and stay with review.

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
PATHSPECS = (
    ":(glob)crates/*/tests/**",
    ":(glob)crates/*/src/**",
    ":(glob)tools/*/tests/**",
    ":(glob)tools/*/src/**",
)


def closed_issues(body: str) -> list[str]:
    return sorted({str(int(n)) for n in CLOSING_KEYWORD.findall(body)}, key=int)


def issue_token(n: str) -> re.Pattern[str]:
    return re.compile(rf"issue_{n}(?!\d)")


CORPUS_DIR_PREFIX = "crates/omnigraph/tests/gq_logic_tests/"
# One path segment after `tests/` (a top-level target) or any `.rs` under
# `src/`, in a `crates/` or `tools/` workspace member.
RUST_FN_PATH = re.compile(r"^(?:crates|tools)/[^/]+/(?:tests/[^/]+\.rs|src/.+\.rs)$")
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


HUNK_HEADER = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def parse_diff(
    diff_text: str,
) -> tuple[list[tuple[str, str]], list[str], list[tuple[str, int, str]]]:
    """Splits a -U0 diff into path-attributed added lines, bare removed
    lines, and added lines positioned by their new-file line number. A
    `+++ b/` marker counts as a file header only in a file's preamble (after
    its `diff --git` line and before its first `@@`) and directly after a
    `--- ` line: inside a hunk, an added CONTENT line `++ b/x` renders as
    `+++ b/x` and a removed content line `-- x` renders as `--- x`, and
    honoring that pair would let a diff spoof its own file attribution.
    Every `@@` hunk header contributes a `HUNK_BREAK` entry to the added
    list."""
    current: str | None = None
    previous = ""
    preamble = False
    new_line = 0
    added: list[tuple[str, str]] = []
    removed: list[str] = []
    positioned: list[tuple[str, int, str]] = []
    for line in diff_text.splitlines():
        if line.startswith("diff --git "):
            preamble = True
            current = None
        elif preamble and line.startswith("+++") and previous.startswith("--- "):
            current = line[len("+++ b/") :] if line.startswith("+++ b/") else None
        elif line.startswith("@@"):
            preamble = False
            added.append(HUNK_BREAK)
            m = HUNK_HEADER.match(line)
            new_line = int(m.group(1)) if m else 0
        elif line.startswith("+") and current is not None:
            # Includes an added CONTENT line that itself starts with `++`
            # (rendered `+++...`): it is a new-file line and advances the
            # position like any other.
            added.append((current, line[1:]))
            positioned.append((current, new_line, line[1:]))
            new_line += 1
        elif line.startswith("-") and not line.startswith("---"):
            removed.append(line[1:])
        previous = line
    return added, removed, positioned


def diff_changes(
    range_: str,
) -> tuple[list[tuple[str, str]], list[str], list[tuple[str, int, str]]]:
    out = subprocess.run(
        ["git", "-c", "core.quotePath=false", "diff", "-U0", range_, "--", *PATHSPECS],
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_diff(out.stdout)


def head_file_reader(range_: str):
    """Returns a reader of new-file contents at the range's head commit
    (`BASE...HEAD` or `BASE..HEAD`), memoized per path; `None` when the
    path does not exist there."""
    head = range_.split("...")[-1].split("..")[-1]
    cache: dict[str, list[str] | None] = {}

    def read(path: str) -> list[str] | None:
        # An empty head (`A...`) would make `git show :path` read the index.
        if not head:
            return None
        if path not in cache:
            out = subprocess.run(
                ["git", "-c", "core.quotePath=false", "show", f"{head}:{path}"],
                capture_output=True,
                text=True,
            )
            cache[path] = out.stdout.splitlines() if out.returncode == 0 else None
        return cache[path]

    return read


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
    positioned: list[tuple[str, int, str]] = (),
    read_file=None,
) -> bool:
    token = issue_token(n)
    for path in files:
        if corpus_case(path) and Path(path).name.startswith(f"issue_{n}_"):
            return True
    for i, (path, text) in enumerate(lines):
        if corpus_case(path):
            # A strengthened case: a body line added to a case named for the
            # issue, new or modified. Header (`#`) and GQ comment (`//`)
            # lines carry no assertion. (An added `# issue: N` header line
            # is not a shape of its own: the walker requires the file name
            # to match and refuses a second `# issue:`, so that line only
            # ever appears in a new case named for the issue.)
            stripped = text.strip()
            if (
                stripped
                and not stripped.startswith(("#", "//"))
                and Path(path).name.startswith(f"issue_{n}_")
            ):
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
    if read_file is not None:
        for path, lineno, text in positioned:
            if RUST_FN_PATH.match(path) and strengthens_test(token, path, lineno, text, read_file):
                return True
    return False


# Brace-opening items that are not functions: an added line inside one of
# these sits in that item, never in a function above it. Items, `use`
# groups, `extern` blocks, and macro invocations (`lazy_static! {`,
# `proptest! {`); a match counts only on a line that opens a brace, so a
# function-local `const N: usize = 3;` is not an item boundary.
RUST_ITEM_DEF = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:unsafe\s+)?"
    r"(?:(?:mod|impl|struct|enum|union|trait|const|static|type|use|extern)\b"
    r"|[A-Za-z_][A-Za-z0-9_:]*!\s*[{(\[])"
)
# Brace-bearing text that is not code: string literals, char literals, and
# `//` comments. Blanked before counting braces.
BRACE_NOISE = re.compile(r'"(?:\\.|[^"\\])*"' + r"|'(?:\\.|[^'\\])'" + r"|//.*$")


def brace_delta(line: str) -> int:
    """`}` minus `{` on one line, ignoring braces inside literals and
    comments. Raw strings and block comments spanning lines are not
    understood (a named residue)."""
    code = BRACE_NOISE.sub("", line)
    return code.count("}") - code.count("{")


def strengthens_test(token: re.Pattern[str], path: str, lineno: int, text: str, read_file) -> bool:
    """True when added line `lineno` (1-based, new file) is a body line
    carrying at least one alphanumeric character, not a comment or an
    attribute, inside an existing test-attributed function named for the
    issue. The enclosing item is the nearest definition above whose brace
    is still open at the added line (counted upward, literals and comments
    blanked); a non-function item (`mod`, `impl`, `struct`, ...) found open
    first means the line is in that item, not in a function. The function's
    attribute stack is read from the file itself."""
    stripped = text.strip()
    if (
        not any(c.isalnum() for c in stripped)
        or stripped.startswith("//")
        or stripped.startswith("#[")
        or stripped.startswith("#!")
        or RUST_FN_DEF.match(text)
    ):
        return False
    file_lines = read_file(path)
    if file_lines is None or not 1 <= lineno <= len(file_lines):
        return False
    depth = 0
    for j in range(lineno - 2, -1, -1):
        line = file_lines[j]
        delta = brace_delta(line)
        depth += delta
        m = RUST_FN_DEF.match(line)
        if m and depth < 0:
            name = m.group(1)
            return (
                bool(token.search(name))
                and not name.startswith("_")
                and file_test_attributed(file_lines, j)
            )
        opens_item = delta < 0 and RUST_ITEM_DEF.match(line)
        if opens_item and depth < 0:
            return False
        if m or opens_item:
            depth = 0
    return False


def file_test_attributed(file_lines: list[str], fn_index: int) -> bool:
    """The same adjacency rule as `test_attributed`, read from a whole file:
    attribute and comment lines directly above the definition, one of them
    a test attribute."""
    j = fn_index - 1
    while j >= 0 and ATTR_OR_COMMENT.match(file_lines[j]):
        if TEST_ATTR.match(file_lines[j]):
            return True
        j -= 1
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
    """A case is a top-level corpus file whose name ends in `.gqt` and does
    not start with `.`: the same rule the walker's `list_cases` applies
    (`crates/omnigraph/tests/gq_logic_tests.rs`), so nothing the gate
    credits can be a file the walker never runs. Both self-tests walk one
    name battery."""
    rest = path[len(CORPUS_DIR_PREFIX) :] if path.startswith(CORPUS_DIR_PREFIX) else ""
    return bool(rest) and "/" not in rest and rest.endswith(".gqt") and not rest.startswith(".")


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
        lines, removed, positioned = diff_changes(range_)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        print(f"FAIL: git diff {range_} failed: {stderr or e}")
        return 1
    removed_fns = removed_fn_names(removed)
    read_file = head_file_reader(range_)
    for path in sorted({p for p, _, _ in positioned if RUST_FN_PATH.match(p)}):
        if read_file(path) is None:
            print(
                f"warn: {path} could not be read at the head commit; an added line "
                "inside an existing test there cannot count as a strengthened regression"
            )
    for n in issues:
        if issue_satisfied(n, files, lines, removed_fns, positioned, read_file):
            print(f"ok: issue #{n} has a matching regression addition")
        else:
            print(
                f"FAIL: the body closes #{n} but the diff neither adds nor extends a "
                f"`.gqt` case named `issue_{n}_*` under {CORPUS_DIR_PREFIX}, and "
                f"neither adds nor extends a `#[test]`-attributed function named for "
                f"`issue_{n}` in crates/*/tests/<name>.rs, tools/*/tests/<name>.rs, or "
                f"their src/. Add one; to extend an existing owner test, rename it to "
                f"carry `issue_{n}` and add the assertion; or ask a maintainer for the "
                f"`{WAIVER_LABEL}` label"
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
    # An added `# issue: N` line is not a shape: the walker requires the file
    # name to match and refuses a second `# issue:`, so a case not named for
    # the issue never counts, whatever header line it gains.
    other = "crates/omnigraph/tests/gq_logic_tests/ranked_join.gqt"
    assert not issue_satisfied("563", [], [(other, "# issue: 563")])
    assert not issue_satisfied("563", [], [(other, "# issue: 0563")])
    assert not issue_satisfied("563", [], [(other, "{\"c.slug\": \"chunk-12\"}")])
    assert not issue_satisfied("563", [], [(other, "issue: 563 in prose")])
    assert not issue_satisfied(
        "563", ["crates/omnigraph-cli/tests/gq_logic_tests/issue_563_x.gqt"], []
    )
    assert not issue_satisfied(
        "563", ["crates/omnigraph/tests/gq_logic_tests/nested/issue_563_x.gqt"], []
    )
    assert not issue_satisfied(
        "563", ["crates/omnigraph/tests/gq_logic_tests/regression_issue_563.gqt"], []
    )
    # Name battery shared with the walker's `walker_flags_foreign_corpus_entries`:
    # a dot-prefixed `.gqt` is never a case, by name or by header line.
    hidden = "crates/omnigraph/tests/gq_logic_tests/.hidden.gqt"
    assert not issue_satisfied("563", [hidden], [])
    assert not issue_satisfied("563", [], [(hidden, "# issue: 563")])
    assert not issue_satisfied("563", ["crates/omnigraph/tests/gq_logic_tests/.issue_563_x.gqt"], [])
    for name, expected in [
        ("a.gqt", True),
        ("b.txt", False),
        (".hidden.gqt", False),
        (".DS_Store", False),
        ("c.GQT", False),
        ("nested/d.gqt", False),
    ]:
        got = corpus_case(CORPUS_DIR_PREFIX + name)
        assert got == expected, f"corpus_case({name!r}) = {got}, expected {expected}"
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
    added, removed, _ = parse_diff(spoof_diff)
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
    added, _, positioned = parse_diff(adjacent_diff)
    assert issue_satisfied("563", [], added)
    assert positioned == [
        ("crates/omnigraph/tests/search.rs", 41, "#[tokio::test]"),
        ("crates/omnigraph/tests/search.rs", 42, "async fn issue_563_adjacent() {"),
    ], positioned
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
    added, _, _ = parse_diff(split_diff)
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
    added, removed, _ = parse_diff(multi_diff)
    assert [a for a in added if a != HUNK_BREAK] == [
        ("crates/omnigraph/tests/gq_logic_tests/a.gqt", "# issue: 7")
    ], added
    assert removed_fn_names(removed) == {"t_issue_8_gone", "keep"}, removed

    # Workspace members under tools/ are owners too.
    tool = "tools/omnigraph-vocabulary-guard/tests/guard.rs"
    assert RUST_FN_PATH.match(tool)
    assert RUST_FN_PATH.match("tools/omnigraph-vocabulary-guard/src/main.rs")
    assert not RUST_FN_PATH.match("tools/omnigraph-vocabulary-guard/tests/helpers/mod.rs")
    assert not RUST_FN_PATH.match("scripts/check-docs.py")
    assert issue_satisfied("563", [], [(tool, "#[test]"), (tool, "fn issue_563_guard() {")])

    # A strengthened corpus case: a modified `issue_N_*.gqt` with an added
    # body line counts; blank, header, and GQ-comment additions do not.
    assert issue_satisfied("563", [], [(corpus, "{\"c.slug\": \"chunk-12\"}")])
    assert issue_satisfied("563", [], [(corpus, "    $c chunkOfArtifact $a")])
    assert not issue_satisfied("563", [], [(corpus, "   ")])
    assert not issue_satisfied("563", [], [(corpus, "# notes: reworded prose")])
    assert not issue_satisfied("563", [], [(corpus, "# issue: 563")])
    assert not issue_satisfied("563", [], [(corpus, "    // a GQ comment")])
    assert not issue_satisfied(
        "563", [], [("crates/omnigraph/tests/gq_logic_tests/other_case.gqt", "{\"x\": 1}")]
    )

    # A strengthened Rust test: an added body line inside an existing
    # test-attributed `issue_N` function, located in the head-commit file.
    head_file = [
        "use x;",
        "",
        "#[tokio::test]",
        "async fn bm25_underfill_issue_563() {",
        "    let db = setup().await;",
        "    assert_eq!(rows(&db).len(), 2);",
        "    assert_eq!(rows(&db)[0].slug, \"chunk-12\");",
        "}",
        "",
        "fn helper_issue_563() {",
        "    let y = 1;",
        "}",
        "",
        "#[test]",
        "fn unrelated() {",
        "    let z = 2;",
        "}",
    ]
    reader = lambda path: head_file if path == rust else None  # noqa: E731
    inside = [(rust, 7, "    assert_eq!(rows(&db)[0].slug, \"chunk-12\");")]
    assert issue_satisfied("563", [], [], positioned=inside, read_file=reader)
    assert not issue_satisfied("563", [], [], positioned=inside)  # no reader, no lookup
    assert not issue_satisfied("564", [], [], positioned=inside, read_file=reader)
    in_plain = [(rust, 11, "    let y = 1;")]
    assert not issue_satisfied("563", [], [], positioned=in_plain, read_file=reader)
    in_other = [(rust, 16, "    let z = 2;")]
    assert not issue_satisfied("563", [], [], positioned=in_other, read_file=reader)
    between = [(rust, 9, "")]
    assert not issue_satisfied("563", [], [], positioned=between, read_file=reader)
    comment_only = [(rust, 5, "    // touched issue_563")]
    assert not issue_satisfied("563", [], [], positioned=comment_only, read_file=reader)
    missing = [("crates/omnigraph/tests/gone.rs", 3, "    let q = 1;")]
    assert not issue_satisfied("563", [], [], positioned=missing, read_file=reader)
    fixture_path = [("crates/omnigraph/tests/helpers/mod.rs", 7, "    let q = 1;")]
    assert not issue_satisfied("563", [], [], positioned=fixture_path, read_file=reader)
    # Punctuation-only lines carry no assertion; a line after the closing
    # brace is outside the function (brace counting).
    brace_only = [(rust, 8, "}")]
    assert not issue_satisfied("563", [], [], positioned=brace_only, read_file=reader)
    after_close = [(rust, 9, "let outside = 1;")]
    assert not issue_satisfied("563", [], [], positioned=after_close, read_file=reader)
    # Braces inside literals and comments do not count; a non-function item
    # after the test owns the lines inside it.
    noisy = [
        "#[test]",
        "fn issue_563_noisy() {",
        "    assert_eq!(s, \"}\");",
        "    let c = '{';",
        "    // }",
        "    let added = 1;",
        "}",
        "",
        "struct Fixture {",
        "    rows: usize,",
        "}",
        "",
        "mod issue_563_later {",
        "    fn helper() {}",
        "}",
        "",
        "const K: Foo = Foo {",
        "    a: 1,",
        "};",
    ]
    noisy_reader = lambda path: noisy if path == rust else None  # noqa: E731
    assert issue_satisfied("563", [], [], positioned=[(rust, 6, "    let added = 1;")], read_file=noisy_reader)
    assert not issue_satisfied("563", [], [], positioned=[(rust, 10, "    rows: usize,")], read_file=noisy_reader)
    assert not issue_satisfied("563", [], [], positioned=[(rust, 14, "    fn helper() {}")], read_file=noisy_reader)
    assert not issue_satisfied("563", [], [], positioned=[(rust, 18, "    a: 1,")], read_file=noisy_reader)
    assert brace_delta('let s = "{{{"; // }') == 0
    assert brace_delta("if x { y } else {") == -1
    # The honest route for an owner test not named for the issue: rename it
    # to carry the token and add the assertion in the same change.
    renamed = [
        "#[tokio::test]",
        "async fn deferred_reads_issue_563() {",
        "    let db = setup().await;",
        "    assert!(rows(&db).len() <= 10);",
        "}",
    ]
    renamed_reader = lambda path: renamed if path == rust else None  # noqa: E731
    rename_lines = [(rust, "async fn deferred_reads_issue_563() {"), (rust, "    assert!(rows(&db).len() <= 10);")]
    assert issue_satisfied(
        "563",
        [],
        rename_lines,
        removed_fns={"deferred_reads"},
        positioned=[(rust, 2, rename_lines[0][1]), (rust, 4, rename_lines[1][1])],
        read_file=renamed_reader,
    )
    assert not issue_satisfied(
        "563", [], rename_lines[:1], removed_fns={"deferred_reads"}, positioned=[(rust, 2, rename_lines[0][1])], read_file=renamed_reader
    )
    # An added content line that starts with `++` still advances the position.
    plus_diff = "\n".join(
        [
            "diff --git a/crates/omnigraph/tests/search.rs b/crates/omnigraph/tests/search.rs",
            "--- a/crates/omnigraph/tests/search.rs",
            "+++ b/crates/omnigraph/tests/search.rs",
            "@@ -10,0 +11,3 @@",
            "+++x",
            "+let a = 1;",
            "+let b = 2;",
        ]
    )
    _, _, plus_positioned = parse_diff(plus_diff)
    assert [n for _, n, _ in plus_positioned] == [11, 12, 13], plus_positioned
    # Inside a hunk, a removed `-- x` line then an added `++ b/...` line is
    # content, never a file header: attribution stays with the real file.
    sql_diff = "\n".join(
        [
            "diff --git a/crates/omnigraph/tests/fixtures/q.sql b/crates/omnigraph/tests/fixtures/q.sql",
            "--- a/crates/omnigraph/tests/fixtures/q.sql",
            "+++ b/crates/omnigraph/tests/fixtures/q.sql",
            "@@ -1 +1,2 @@",
            "--- old sql comment",
            "+++ b/crates/omnigraph/tests/gq_logic_tests/issue_563_x.gqt",
            "+{\"c.slug\": \"chunk-12\"}",
        ]
    )
    sql_added, _, sql_positioned = parse_diff(sql_diff)
    assert all(p == "crates/omnigraph/tests/fixtures/q.sql" for p, _ in sql_added if (p, _) != HUNK_BREAK), sql_added
    assert [n for _, n, _ in sql_positioned] == [1, 2], sql_positioned
    assert not issue_satisfied("563", [], sql_added)
    # Macro-invocation blocks and `use` groups after a test are not the test;
    # a function-local `const` without a brace is not an item boundary.
    macro_file = [
        "#[tokio::test]",
        "async fn bm25_underfill_issue_563() {",
        "    const N: usize = 3;",
        "    for i in 0..N {",
        "        assert!(i < N);",
        "    }",
        "}",
        "",
        "lazy_static! {",
        "    static ref DB: usize = 1;",
        "}",
        "",
        "proptest! {",
        "    #![proptest_config(Config::default())]",
        "    fn prop_issue_563(x in 0..3usize) { assert!(x < 3); }",
        "}",
        "",
        "use crate::{",
        "    a,",
        "    b,",
        "};",
    ]
    macro_reader = lambda path: macro_file if path == rust else None  # noqa: E731
    assert issue_satisfied("563", [], [], positioned=[(rust, 5, "        assert!(i < N);")], read_file=macro_reader)
    assert not issue_satisfied("563", [], [], positioned=[(rust, 10, "    static ref DB: usize = 1;")], read_file=macro_reader)
    assert not issue_satisfied("563", [], [], positioned=[(rust, 14, "    #![proptest_config(Config::default())]")], read_file=macro_reader)
    assert not issue_satisfied("563", [], [], positioned=[(rust, 20, "    b,")], read_file=macro_reader)

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
