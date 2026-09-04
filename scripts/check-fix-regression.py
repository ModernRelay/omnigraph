#!/usr/bin/env python3
"""Fix Regression Gate: every issue a PR body closes by keyword needs a
matching regression addition in the diff, or the PR carries the `no-repro`
label. Specified in docs/rfcs/0045-gq-logic-tests.md (User and operational
behavior, "Fix-PR gate").

The gate reads the three closing forms GitHub's own parser closes on,
`fixes #123`, `fixes ModernRelay/omnigraph#123`, and
`fixes https://github.com/ModernRelay/omnigraph/issues/123` (the repository
from `--repo`, the workflow passes `GITHUB_REPOSITORY`; with neither, only
`#N`): case-insensitive, a word boundary before the keyword, an optional
colon, whitespace unless the colon is present, then the target. A reference
to another repository closes nothing here and is not read. Closings by
`GH-N`, a bare `fixes#123`, an autolink `<url>` or Markdown link `[#N](url)`,
`http://` or `www.` URLs, commit message, or manual close pass unexamined and
belong to review; a keyword inside a code span, a fence, or an HTML comment
is read, and a PR against a non-default base is examined although GitHub
closes nothing there.

A fix outside the code paths, `crates/` and `tools/` (Markdown files under
them aside) and the root `Cargo.toml` and `Cargo.lock`, where every
workspace member lives, has no logic or Rust test that could witness it (a
workflow, a script, a document, a deployment file), so a PR whose diff
changes no code path passes with its closed issues unexamined, as a log
line and a `::notice` annotation; the diff is listed with renames disabled
so a file moved out of a crate still shows its source-side deletion.

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
recognize inside the code paths (a helper or fixture module, a script
under a crate, a rustdoc-only change) satisfy it only through the
`no-repro` label, which a maintainer applies. What a match guarantees
differs by shape: a corpus match ran green in the required `GQ Logic
Tests` job; a Rust match is a test-attributed definition or an edit inside
one, not a run. A pull request runs every workspace test target in `Test
Workspace`, a reporting context the gate does not consult, and workspace
clippy refuses an unreferenced private function but not an `#[ignore]`d or
cfg-gated one, so whether that test runs in the suite and asserts the right
thing stays with review.
Comments, strings, and fixture lines mentioning the issue do not count.
N is always followed by a non-digit or the end. Named residue: a
definition inside an added block comment or raw string still matches
(line-based parsing cannot see multi-line context); the enclosing item of
a strengthened line is found by brace counting with string literals, char
literals, and `//` comments blanked, so a brace inside a raw string or a
multi-line block comment can mislead it; those evasions, like a test that
asserts nothing, are deliberate and stay with review.

A failure names the code paths that made the gate look, the ways through,
any near miss the diff holds (a case whose header says `# issue: N` under
another name or a subdirectory; a test named with the bare number, moved
rather than added, under a leading `_`, or in a helper module; a function
named for the issue with no added test attribute directly above it), and a
case skeleton, as a log line and as a GitHub `::error` annotation. The
skeleton spells the corpus format by hand (`crates/omnigraph-gqt/README.md`);
a change to the header keys or section names updates it here too.

Exit 0 exactly when the diff changes no code path, or every keyword-closed
issue has its match, or the PR carries `no-repro`; and in every case
AGENTS.md still names the logic-test corpus path.

Usage:
  check-fix-regression.py --body-file F --labels "a,b" --range BASE...HEAD [--repo OWNER/NAME]
  check-fix-regression.py --self-test
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

CLOSING_PREFIX = r"(?<![A-Za-z0-9_])(?:close[sd]?|fix(?:es|ed)?|resolve[sd]?)(?::\s*|\s+)"


def closing_keyword(repo: str | None) -> re.Pattern[str]:
    """`#N` always; `OWNER/NAME#N` and the issue URL only for `repo`, the
    repository the gate runs in, since a reference to any other repository
    closes nothing here."""
    targets = [r"#(\d+)"]
    if repo:
        slug = "/".join(re.escape(part) for part in repo.split("/", 1))
        targets.append(rf"{slug}#(\d+)")
        targets.append(rf"https://github\.com/{slug}/issues/(\d+)")
    return re.compile(CLOSING_PREFIX + "(?:" + "|".join(targets) + ")", re.IGNORECASE)


# The workspace members' homes plus the root manifests, Markdown excluded;
# the self-test pins it to `[workspace] members`.
CODE_PATH = re.compile(r"^(?:(?:crates|tools)/(?!.*\.md$)|Cargo\.toml$|Cargo\.lock$)")
WORKSPACE_MEMBERS = re.compile(r"^members\s*=\s*\[(.*?)\]", re.MULTILINE | re.DOTALL)
# A corpus case's `# issue: N` header line, spelled exactly as the harness
# accepts it (`crates/omnigraph-gqt/src/lib.rs`, `parse_header`).
CASE_ISSUE_HEADER = re.compile(r"^# issue: ([1-9]\d*)$")
CASE_ISSUE_STEM = re.compile(r"^issue[_-]?0*(\d+)[_-]?", re.IGNORECASE)
# A Rust file under a crate's `tests/<dir>/`: a helper or fixture module,
# never a top-level target.
RUST_NESTED_TEST_PATH = re.compile(r"^(?:crates|tools)/[^/]+/tests/[^/]+/.+\.rs$")
CORPUS_PATH_SENTENCE = "crates/omnigraph-gqt/cases/"
WAIVER_LABEL = "no-repro"
PATHSPECS = (
    ":(glob)crates/omnigraph-gqt/cases/**",
    ":(glob)crates/*/tests/**",
    ":(glob)crates/*/src/**",
    ":(glob)tools/*/tests/**",
    ":(glob)tools/*/src/**",
)


def closed_issues(body: str, repo: str | None = None) -> list[str]:
    found = {m.group(m.lastindex) for m in closing_keyword(repo).finditer(body)}
    return sorted({str(int(n)) for n in found}, key=int)


def issue_token(n: str) -> re.Pattern[str]:
    return re.compile(rf"issue_{n}(?!\d)")


CORPUS_DIR_PREFIX = "crates/omnigraph-gqt/cases/"
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


def git_diff(*args: str) -> str:
    out = subprocess.run(
        ["git", "-c", "core.quotePath=false", "diff", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout


def added_files(range_: str) -> list[str]:
    out = git_diff("--name-only", "--diff-filter=A", range_, "--", *PATHSPECS)
    return [line for line in out.splitlines() if line]


def changed_paths(range_: str) -> list[str]:
    """Every path the range changes, renames disabled: a file moved out of
    a crate still shows its source-side deletion."""
    out = git_diff("--name-only", "--no-renames", range_, "--")
    return [line for line in out.splitlines() if line]


def parse_repo(value: str | None) -> str | None:
    """`OWNER/NAME` or nothing; any other shape is refused rather than
    matched against nothing."""
    value = (value or "").strip()
    if not value:
        return None
    owner, sep, name = value.partition("/")
    if not sep or not owner or not name or "/" in name:
        raise ValueError(f"--repo must be OWNER/NAME, got {value!r}")
    return value


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
    return parse_diff(git_diff("-U0", range_, "--", *PATHSPECS))


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
            # is not a shape of its own: the runner requires the file name
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


def near_misses(
    n: str, lines: list[tuple[str, str]], removed_fns: frozenset[str] | set[str] = frozenset()
) -> list[str]:
    """What the diff holds that almost satisfies issue `n`, named in the
    failure and never credited: a corpus case whose header says
    `# issue: n` under another name or under a subdirectory; a test named
    with the bare number, moved rather than added, under a leading `_`, or
    in a helper module; a function named for the issue with no added test
    attribute directly above it."""
    token = issue_token(n)
    bare = re.compile(rf"(?<!\d){n}(?!\d)")
    other_issue = re.compile(r"issue_\d+")
    hints: list[str] = []
    for i, (path, text) in enumerate(lines):
        name = Path(path).name
        if path.startswith(CORPUS_DIR_PREFIX) and name.endswith(".gqt"):
            m = CASE_ISSUE_HEADER.match(text)
            if not m or m.group(1) != n:
                continue
            stem = CASE_ISSUE_STEM.sub("", name[: -len(".gqt")]).lower()
            short = re.sub(r"[^a-z0-9_]", "_", stem)
            target = f"{CORPUS_DIR_PREFIX}issue_{n}_{short or '<short_name>'}.gqt"
            if not corpus_case(path):
                hints.append(
                    f"`{path}` carries `# issue: {n}` but the corpus runs top-level "
                    f"`.gqt` files only; move it to `{target}`"
                )
            elif not name.startswith(f"issue_{n}_") or name == f"issue_{n}_.gqt":
                hints.append(
                    f"`{path}` carries `# issue: {n}` in its header; rename it to "
                    f"`{target}` (`issue_{n}_` then a short name over `[a-z0-9_]`)"
                )
        elif RUST_FN_PATH.match(path) or RUST_NESTED_TEST_PATH.match(path):
            m = RUST_FN_DEF.match(text)
            if not m or text.rstrip().endswith(";"):
                continue
            fn = m.group(1)
            attributed = test_attributed(lines, i)
            if RUST_NESTED_TEST_PATH.match(path):
                if token.search(fn) and attributed:
                    hints.append(
                        f"`fn {fn}` in `{path}` sits in a helper or fixture module; only a "
                        f"top-level target `tests/<name>.rs` or a `src/` module counts"
                    )
            elif token.search(fn) and fn.startswith("_") and attributed:
                hints.append(f"`fn {fn}` in `{path}` starts with `_`; drop the underscore")
            elif token.search(fn) and fn in removed_fns and attributed:
                hints.append(
                    f"`fn {fn}` in `{path}` is moved, not added (the same name is removed "
                    f"elsewhere in the diff); add an assertion inside its body"
                )
            elif token.search(fn) and not attributed:
                hints.append(
                    f"`fn {fn}` in `{path}` is named for the issue but the diff adds no "
                    f"`#[test]` line directly above it (other attributes and `//` comments "
                    f"may sit between, a blank line or a block comment may not); only a "
                    f"test-attributed function counts, a helper by that name is not credited, "
                    f"and a renamed existing test needs an added assertion in its body"
                )
            elif bare.search(fn) and not other_issue.search(fn) and attributed:
                hints.append(
                    f"`fn {fn}` in `{path}` is a test named with the bare number; "
                    f"rename it to carry `issue_{n}`"
                )
    return list(dict.fromkeys(hints))


def failure_message(n: str, code_paths: list[str], hints: list[str]) -> str:
    shown = ", ".join(code_paths[:5])
    if len(code_paths) > 5:
        shown += f", +{len(code_paths) - 5} more"
    parts = [
        f"FAIL: the body closes #{n} and the diff changes code under test ({shown}), "
        f"but adds or extends no `.gqt` case named `issue_{n}_*` under {CORPUS_DIR_PREFIX} "
        f"and no `#[test]`-attributed function named for `issue_{n}`.",
        *(f"  near miss: {hint}" for hint in hints),
        "  Ways through, any one:",
        f"    1. add {CORPUS_DIR_PREFIX}issue_{n}_<short_name>.gqt: the query that went wrong "
        "before the fix, with the rows it returns after (the corpus runs it on every PR)",
        f"    2. add a #[test] function named for issue_{n} in crates/*/tests/<name>.rs, "
        "tools/*/tests/<name>.rs, or their src/",
        f"    3. extend an existing test: rename it to carry issue_{n} and add the assertion",
        f"    4. no test can exist (perf-only, a race, a removal, a docs-only change inside "
        f"a crate such as rustdoc): ask a maintainer for the `{WAIVER_LABEL}` label",
        "  Rule: docs/dev/ci.md, Fix Regression Gate. Skeleton for 1:",
        f"    # issue: {n}",
        "    # red_on: <date>, pre-fix build: <what the old build returned>",
        "    # notes: <one line on what the case pins>",
        "",
        "    --- schema",
        "    node Person {",
        "        name: String @key",
        "    }",
        "",
        "    --- seed",
        '    {"type":"Person","data":{"name":"alice"}}',
        "",
        "    --- query",
        "    query q() {",
        "        match { $p: Person }",
        "        return { $p.name }",
        "    }",
        "",
        "    --- expect unordered",
        '    {"p.name": "alice"}',
    ]
    return "\n".join(parts)


def annotate(level: str, message: str) -> None:
    """The same text as a GitHub annotation (`error` or `notice`), shown on
    the checks summary without opening the log; `%`, CR, and LF escaped
    per the workflow-command encoding."""
    encoded = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::{level} title=Fix Regression Gate::{encoded}")


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
    not start with `.`: the name half of the rule the corpus target's
    `datatest_stable::harness!` pattern applies
    (`crates/omnigraph-gqt/tests/gq_logic_tests.rs`, mirrored by `list_cases`
    in `src/lib.rs`), so nothing the gate credits can be a file the target
    never runs. Both self-tests walk one name battery."""
    rest = path[len(CORPUS_DIR_PREFIX) :] if path.startswith(CORPUS_DIR_PREFIX) else ""
    return bool(rest) and "/" not in rest and rest.endswith(".gqt") and not rest.startswith(".")


def check_agents_md() -> bool:
    agents = Path("AGENTS.md")
    return agents.is_file() and CORPUS_PATH_SENTENCE in agents.read_text(encoding="utf-8")


def run_gate(body: str, labels: list[str], range_: str, repo: str | None) -> int:
    ok = True
    if not check_agents_md():
        print(
            f"FAIL: AGENTS.md no longer names the corpus path `{CORPUS_PATH_SENTENCE}`; "
            "the contract sentence and this gate leave together"
        )
        ok = False
    if repo is None:
        print("warn: no --repo and no GITHUB_REPOSITORY; only the `#N` closing form is read")
    issues = closed_issues(body, repo)
    if not issues:
        print("ok: the PR body closes no issue by keyword")
        return 0 if ok else 1
    if WAIVER_LABEL in labels:
        print(f"ok: `{WAIVER_LABEL}` label waives the regression requirement for this PR")
        return 0 if ok else 1
    try:
        code_paths = [path for path in changed_paths(range_) if CODE_PATH.match(path)]
        if not code_paths:
            closed = ", ".join(f"#{n}" for n in issues)
            message = (
                f"ok: the body closes {closed} but the diff changes no path under crates/ "
                "or tools/ (Markdown aside) and neither Cargo.toml nor Cargo.lock, so no "
                "logic or Rust test can witness the fix; the closed issues pass UNEXAMINED"
            )
            print(message)
            annotate("notice", message)
            return 0 if ok else 1
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
            message = failure_message(n, code_paths, near_misses(n, lines, removed_fns))
            print(message)
            annotate("error", message)
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
        ("fixes ModernRelay/omnigraph#4", ["4"]),
        ("fixes: modernrelay/OMNIGRAPH#4", ["4"]),
        ("fixes ModernRelay/omnigraph-foo#4", []),
        ("fixes ModernRelay/omnigraph#4 closes #4", ["4"]),
        ("Closes https://github.com/ModernRelay/omnigraph/issues/4", ["4"]),
        ("Closes https://github.com/ModernRelay/omnigraph/issues/4.", ["4"]),
        ("Closes https://github.com/ModernRelay/omnigraph/pull/4", []),
        ("Closes https://github.com/o/r/issues/4", []),
        ("see https://github.com/ModernRelay/omnigraph/issues/4", []),
    ]
    repo = "ModernRelay/omnigraph"
    for body, expected in cases:
        got = closed_issues(body, repo)
        assert got == expected, f"closed_issues({body!r}) = {got}, expected {expected}"
    # Without a repository only `#N` is read: another form cannot be told
    # from a reference to some other repository.
    assert closed_issues("fixes ModernRelay/omnigraph#4", None) == []
    assert closed_issues("Closes https://github.com/ModernRelay/omnigraph/issues/4") == []
    assert closed_issues("fixes #4", None) == ["4"]
    for path in ("crates/omnigraph/src/lib.rs", "tools/x/tests/a.rs", "Cargo.toml", "Cargo.lock", "crates/omnigraph-bench/Cargo.toml", "crates/x/tests/fixtures/a.md.json"):
        assert CODE_PATH.match(path), path
    for path in (".github/workflows/ci.yml", "docs/dev/ci.md", "scripts/check-docs.py", "deploy/x.yaml", "benchmarks/Cargo.toml", "rust-toolchain.toml", "crates2/x.rs", "Cargo.toml.bak", "crates/omnigraph-dst/README.md", "crates/omnigraph/tests/fixtures/lance10-fts.md", "tools/x/README.md"):
        assert not CODE_PATH.match(path), path
    # The code paths are pinned to the workspace: every member must sit
    # under one, or a fix there would pass unexamined.
    cargo_toml = Path(__file__).resolve().parents[1] / "Cargo.toml"
    assert cargo_toml.is_file(), f"{cargo_toml} is missing; the members pin needs the workspace manifest"
    m = WORKSPACE_MEMBERS.search(cargo_toml.read_text(encoding="utf-8"))
    members = re.findall(r'"([^"]+)"', m.group(1)) if m else []
    assert members, "no [workspace] members found in Cargo.toml"
    for member in members:
        assert CODE_PATH.match(member + "/"), f"workspace member {member} is outside the code paths"
    assert parse_repo("ModernRelay/omnigraph") == "ModernRelay/omnigraph"
    assert parse_repo("  ModernRelay/omnigraph\n") == "ModernRelay/omnigraph"
    assert parse_repo("") is None and parse_repo(None) is None and parse_repo("  ") is None
    for bad in ("ModernRelay", "/omnigraph", "ModernRelay/", "a/b/c"):
        try:
            parse_repo(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"parse_repo({bad!r}) accepted")
    corpus = "crates/omnigraph-gqt/cases/issue_563_x.gqt"
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
    # An added `# issue: N` line is not a shape: the runner requires the file
    # name to match and refuses a second `# issue:`, so a case not named for
    # the issue never counts, whatever header line it gains.
    other = "crates/omnigraph-gqt/cases/ranked_join.gqt"
    assert not issue_satisfied("563", [], [(other, "# issue: 563")])
    assert not issue_satisfied("563", [], [(other, "# issue: 0563")])
    assert not issue_satisfied("563", [], [(other, "{\"c.slug\": \"chunk-12\"}")])
    assert not issue_satisfied("563", [], [(other, "issue: 563 in prose")])
    assert not issue_satisfied(
        "563", ["crates/omnigraph-cli/tests/gq_logic_tests/issue_563_x.gqt"], []
    )
    assert not issue_satisfied(
        "563", ["crates/omnigraph-gqt/cases/nested/issue_563_x.gqt"], []
    )
    assert not issue_satisfied(
        "563", ["crates/omnigraph-gqt/cases/regression_issue_563.gqt"], []
    )
    # Name battery shared with the runner's `corpus_flags_foreign_entries`:
    # a dot-prefixed `.gqt` is never a case, by name or by header line.
    hidden = "crates/omnigraph-gqt/cases/.hidden.gqt"
    assert not issue_satisfied("563", [hidden], [])
    assert not issue_satisfied("563", [], [(hidden, "# issue: 563")])
    assert not issue_satisfied("563", ["crates/omnigraph-gqt/cases/.issue_563_x.gqt"], [])
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
            "+++ b/crates/omnigraph-gqt/cases/fake.gqt",
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
            "--- a/crates/omnigraph-gqt/cases/a.gqt",
            "+++ b/crates/omnigraph-gqt/cases/a.gqt",
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
        ("crates/omnigraph-gqt/cases/a.gqt", "# issue: 7")
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
        "563", [], [("crates/omnigraph-gqt/cases/other_case.gqt", "{\"x\": 1}")]
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
            "+++ b/crates/omnigraph-gqt/cases/issue_563_x.gqt",
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

    # Near misses: named in the failure, never credited.
    misnamed = "crates/omnigraph-gqt/cases/bm25_underfill.gqt"
    hints = near_misses("563", [(misnamed, "# issue: 563"), (misnamed, "--- schema")])
    assert len(hints) == 1 and "`crates/omnigraph-gqt/cases/issue_563_bm25_underfill.gqt`" in hints[0], hints
    for header in ("# issue: none", "# issue: 5630", "# issue: 0563", "#issue:563", "# issue: #563", "# issue: 563 ", "  # issue: 563"):
        assert near_misses("563", [(misnamed, header)]) == [], header
    assert near_misses("563", [(corpus, "# issue: 563")]) == []
    assert near_misses("563", [("crates/omnigraph/tests/fixtures/x.gqt", "# issue: 563")]) == []
    # A stem already carrying the token is not prefixed a second time.
    for stem in ("issue_563", "issue-563", "issue_0563_x", "issue_563_"):
        hints = near_misses("563", [(f"crates/omnigraph-gqt/cases/{stem}.gqt", "# issue: 563")])
        assert len(hints) == 1 and "issue_563_issue" not in hints[0], (stem, hints)
    assert "issue_563_x.gqt" in near_misses("563", [("crates/omnigraph-gqt/cases/issue_0563_x.gqt", "# issue: 563")])[0]
    assert "`crates/omnigraph-gqt/cases/issue_563_x_y.gqt`" in near_misses("563", [("crates/omnigraph-gqt/cases/ISSUE-563-X-y.gqt", "# issue: 563")])[0]
    assert "`crates/omnigraph-gqt/cases/issue_563_bm25_v2.gqt`" in near_misses("563", [("crates/omnigraph-gqt/cases/bm25.v2.gqt", "# issue: 563")])[0]
    assert near_misses("563", [("crates/omnigraph-gqt/cases/README.md", "# issue: 563")]) == []
    assert "issue_563_<short_name>.gqt" in near_misses("563", [("crates/omnigraph-gqt/cases/issue_563.gqt", "# issue: 563")])[0]
    hints = near_misses("563", [("crates/omnigraph-gqt/cases/traversal/issue_563_x.gqt", "# issue: 563")])
    assert len(hints) == 1 and "top-level" in hints[0] and "cases/issue_563_x.gqt" in hints[0], hints
    hints = near_misses("563", [(rust, "#[test]"), (rust, "fn bm25_underfill_563() {")])
    assert len(hints) == 1 and "bare number" in hints[0], hints
    assert near_misses("563", [(rust, "fn bm25_underfill_563() {")]) == []
    assert near_misses("563", [(rust, "#[test]"), (rust, "fn underfill_5630() {")]) == []
    assert near_misses("563", [(rust, "#[test]"), (rust, "fn issue_564_probe_563() {")]) == []
    hints = near_misses("563", [(rust, "fn issue_563_underfill() {")])
    assert len(hints) == 1 and "adds no `#[test]`" in hints[0] and "helper" in hints[0], hints
    assert near_misses("563", [(rust, "#[test]"), (rust, "fn issue_563_underfill() {")]) == []
    assert near_misses("563", [(rust, "fn issue_563_underfill();")]) == []
    hints = near_misses("563", [(rust, "#[test]"), (rust, "fn _issue_563_underfill() {")])
    assert len(hints) == 1 and "underscore" in hints[0], hints
    hints = near_misses("563", [(rust, "#[test]"), (rust, "fn issue_563_underfill() {")], {"issue_563_underfill"})
    assert len(hints) == 1 and "moved, not added" in hints[0], hints
    nested = "crates/omnigraph/tests/fixtures/mod.rs"
    hints = near_misses("563", [(nested, "#[test]"), (nested, "fn issue_563_underfill() {")])
    assert len(hints) == 1 and "helper or fixture module" in hints[0], hints
    assert near_misses("563", [(nested, "fn issue_563_underfill() {")]) == []
    assert near_misses("563", [(nested, "#[test]"), (nested, "fn underfill_563() {")]) == []
    message = failure_message("563", ["crates/omnigraph/src/lib.rs"], ["hint one"])
    assert message.startswith("FAIL: the body closes #563") and "near miss: hint one" in message
    assert "issue_563_<short_name>.gqt" in message and "# issue: 563" in message

    print("self-test ok")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--body-file")
    parser.add_argument("--labels", default="")
    parser.add_argument("--range")
    parser.add_argument(
        "--repo",
        default=os.environ.get("GITHUB_REPOSITORY"),
        help="OWNER/NAME whose `OWNER/NAME#N` and issue-URL closings count; default $GITHUB_REPOSITORY",
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.body_file or not args.range:
        parser.error("--body-file and --range are required unless --self-test")
    try:
        repo = parse_repo(args.repo)
    except ValueError as e:
        parser.error(str(e))
    body = Path(args.body_file).read_text(encoding="utf-8")
    labels = [label.strip() for label in args.labels.split(",") if label.strip()]
    return run_gate(body, labels, args.range, repo)


if __name__ == "__main__":
    sys.exit(main())
