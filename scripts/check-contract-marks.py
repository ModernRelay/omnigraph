#!/usr/bin/env python3
"""Hold the `✓` marks of docs/dev/contract.md to the tests that name its
statements (docs/dev/contract.md, Citing).

A test names a statement in a comment on a line of its own:

    // contract: merge:2            a Rust test, any file of test code under crates/
    // contract: merge:2, E2        several statements
    # notes: contract: merge:2      a `.gqt` case, whose comments are header lines

The page defines the addresses, and this script reads them from the page:
the kinds from the table whose first header cell is `Kind`, the columns from
the table of rules per kind (the first word of each header cell), the pairs
from the `Pair` table, the gaps and decisions from theirs, the rules from
their bold `**E1. …**` openings and the details from list items that open
with `kind:N`. No kind or column name is spelled here.

A mark has one place for each address: straight after the address where the
page defines it (`**E1. It sees one commit.** ✓`, `` `merge:2` ✓ ``,
`` `read+reclaim` ✓ ``, `` `G1` ✓ ``), and at the end of the cell for a
`kind.column` address, which the page never spells. A pair the page gives two
rows has two places, and both carry the mark.

The check fails when
  - a comment names an address the page does not define,
  - a statement carries `✓` and no test names it,
  - a test names a statement that carries no `✓`,
  - a `✓` stands anywhere else on the page (outside code), where no test
    could ever account for it,
  - a comment opens with `contract:` and a list of words in another
    spelling, or sits outside test code, so a near-miss cannot go unread.
    A sentence that happens to wrap at `contract:` is prose and is not read.

`--update` rewrites the marks from the comments, as `OMNIGRAPH_UPDATE_OPENAPI=1`
regenerates `openapi.json`: the default run only reports, and CI never passes
the flag. It writes nothing while a comment or the page itself is in error.
`--self-test` feeds the check fixture trees it must refuse and ones it must
accept; the fixture page uses kinds the real page does not have.

Run from anywhere; exit 0 exactly when the check passes.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONTRACT = Path("docs/dev/contract.md")
TESTS = Path("crates")
MARK = "✓"
UPDATE = "python3 scripts/check-contract-marks.py --update"

# A table is known by its first header cell: this is the page's structure,
# not its vocabulary. A table that is missing fails the check, so a renamed
# header cannot leave an address form unread.
KIND_TABLE = "Kind"
CELL_TABLE = ""
PAIR_TABLE = "Pair"
NUMBERED_TABLES = {"Gap": "G", "Decision": "D"}
RULE = re.compile(r"^\*\*(E\d+)\. [^*]+\*\*")
DETAIL = re.compile(r"^\s*\d+\. `([^`\s:+.]+):(\d+)`")
# One address may have several places only in this form: the page gives a
# pair one row for each way its two kinds refuse each other.
MULTI_SITE = {"pair"}

FENCE = re.compile(r"^\s*(```+|~~~+)")
CODE_SPAN = re.compile(r"`[^`]*`")
ADDRESS_LIST = re.compile(r"[^\s,]+(?:, [^\s,]+)*")
# Prose also opens a line with `contract:` when a sentence wraps there. A
# comment is read as naming statements when what follows is a list: single
# words joined by commas, or words that each have the shape of an address.
WORD_LIST = re.compile(r"[^\s,]+(?:\s*,\s*[^\s,]+)*,?")
ADDRESS_SHAPE = re.compile(r"[A-Za-z_]+(?:\d+|[.:+][A-Za-z0-9_]+)")
# Suffix -> (a comment that opens with `contract:` and what follows it, the
# one accepted spelling, that spelling as shown to the author).
COMMENTS = {
    ".rs": (
        re.compile(r"//[/!]?\s*contract:\s*(.*?)\s*$"),
        re.compile(r"^\s*// contract: (\S.*)$"),
        "// contract: <address>[, <address>]",
    ),
    ".gqt": (
        re.compile(r"^\s*#\s*(?:notes:\s*)?contract:\s*(.*?)\s*$"),
        re.compile(r"^# notes: contract: (\S.*)$"),
        "# notes: contract: <address>[, <address>]",
    ),
}


def names_statements(rest: str) -> bool:
    words = [word for word in re.split(r"[,\s]+", rest) if word]
    return bool(words) and (WORD_LIST.fullmatch(rest) is not None or all(ADDRESS_SHAPE.fullmatch(word) for word in words))


@dataclass(frozen=True)
class Site:
    """One place a mark can stand: ` ✓` is present at, or inserted at, `col`."""

    address: str
    form: str
    line: int
    col: int
    marked: bool


def site(lines: list[str], address: str, form: str, line: int, col: int) -> Site:
    return Site(address, form, line, col, lines[line][col : col + 2] == " " + MARK)


def split_row(line: str) -> list[tuple[int, int]]:
    """The trimmed span of each cell of a table row. A `|` in a code span or
    after a backslash is text."""
    bars: list[int] = []
    in_code = False
    index = 0
    while index < len(line):
        char = line[index]
        if char == "\\":
            index += 2
            continue
        if char == "`":
            in_code = not in_code
        elif char == "|" and not in_code:
            bars.append(index)
        index += 1
    cells: list[tuple[int, int]] = []
    for left, right in zip(bars, bars[1:]):
        start, end = left + 1, right
        while start < end and line[start] == " ":
            start += 1
        while end > start and line[end - 1] == " ":
            end -= 1
        cells.append((start, end))
    return cells


def parse_page(lines: list[str]) -> tuple[list[Site], list[str]]:
    """Every mark site of the page, and what is wrong with the page itself."""
    errors: list[str] = []

    def error(line: int, message: str) -> None:
        errors.append(f"{CONTRACT}:{line + 1}: {message}")

    # Prose lines and table blocks, outside fenced code.
    prose: list[int] = []
    tables: list[list[int]] = []
    fence: str | None = None
    in_table = False
    for number, line in enumerate(lines):
        fenced = FENCE.match(line)
        if fenced:
            marker = fenced.group(1)[0]
            fence = marker if fence is None else (None if marker == fence else fence)
            in_table = False
        elif fence is not None:
            continue
        elif line.startswith("|"):
            if not in_table:
                tables.append([])
            tables[-1].append(number)
            in_table = True
        else:
            prose.append(number)
            in_table = False

    by_role: dict[str, list[int]] = {}
    for rows in tables:
        cells = split_row(lines[rows[0]])
        role = lines[rows[0]][cells[0][0] : cells[0][1]] if cells else None
        if role in (KIND_TABLE, CELL_TABLE, PAIR_TABLE, *NUMBERED_TABLES):
            if role in by_role:
                error(rows[0], f"a second table whose first header cell is `{role}`; its addresses would be defined twice")
            else:
                by_role[role] = rows
    for role in (KIND_TABLE, CELL_TABLE, PAIR_TABLE, *NUMBERED_TABLES):
        if role not in by_role:
            errors.append(f"{CONTRACT}: no table whose first header cell is `{role}`; the check would read none of its addresses")

    def first_cell(number: int) -> tuple[int, str]:
        start, end = split_row(lines[number])[0]
        return start, lines[number][start:end]

    def data_rows(role: str) -> list[int]:
        return by_role.get(role, [])[2:]

    kinds: list[str] = []
    for number in data_rows(KIND_TABLE):
        _, text = first_cell(number)
        name = re.fullmatch(r"`([^`]+)`", text)
        if name is None:
            error(number, f"a row of the `{KIND_TABLE}` table must open with the kind in backticks, got `{text}`")
        else:
            kinds.append(name.group(1))

    sites: list[Site] = []

    # Rules and details.
    for number in prose:
        rule = RULE.match(lines[number])
        if rule:
            sites.append(site(lines, rule.group(1), "rule", number, rule.end()))
        detail = DETAIL.match(lines[number])
        if detail:
            if detail.group(1) not in kinds:
                error(number, f"`{detail.group(1)}:{detail.group(2)}` names no kind of the `{KIND_TABLE}` table")
            else:
                sites.append(site(lines, f"{detail.group(1)}:{detail.group(2)}", "detail", number, detail.end()))

    # Cells: `kind.column`, the column being the first word of its header.
    if CELL_TABLE in by_role:
        header = by_role[CELL_TABLE][0]
        columns = [lines[header][start:end].split(" ")[0] for start, end in split_row(lines[header])[1:]]
        for column in sorted({column for column in columns if columns.count(column) > 1 or not column}):
            error(header, f"two columns of the table of rules per kind open with `{column}`; a cell address needs one")
        for number in data_rows(CELL_TABLE):
            cells = split_row(lines[number])
            name = re.fullmatch(r"\*\*([^*]+)\*\*", lines[number][cells[0][0] : cells[0][1]])
            if name is None or name.group(1) not in kinds:
                error(number, f"a row of the table of rules per kind must open with a kind of the `{KIND_TABLE}` table in bold")
                continue
            for column, (start, end) in zip(columns, cells[1:]):
                marked = lines[number][start:end].endswith(" " + MARK)
                sites.append(site(lines, f"{name.group(1)}.{column}", "cell", number, end - 2 if marked else end))

    # Pairs: every `kind+kind` of the first cell, in the order of the kinds.
    for number in data_rows(PAIR_TABLE):
        start, text = first_cell(number)
        for span in CODE_SPAN.finditer(text):
            pair = span.group()[1:-1].split("+")
            if len(pair) != 2 or any(kind not in kinds for kind in pair):
                error(number, f"{span.group()} is not two kinds of the `{KIND_TABLE}` table joined by `+`")
            elif kinds.index(pair[0]) > kinds.index(pair[1]):
                error(number, f"{span.group()} is not in the order of the `{KIND_TABLE}` table; write `{pair[1]}+{pair[0]}`")
            else:
                sites.append(site(lines, "+".join(pair), "pair", number, start + span.end()))

    # Gaps and decisions.
    for role, prefix in NUMBERED_TABLES.items():
        for number in data_rows(role):
            start, text = first_cell(number)
            name = re.match(rf"`({prefix}\d+)`", text)
            if name is None:
                error(number, f"a row of the `{role}` table must open with `{prefix}` and a number in backticks, got `{text}`")
            else:
                sites.append(site(lines, name.group(1), role.lower(), number, start + name.end()))

    seen: dict[str, Site] = {}
    for found in sites:
        first = seen.setdefault(found.address, found)
        if first is not found and found.form not in MULTI_SITE:
            error(found.line, f"`{found.address}` is defined twice (also line {first.line + 1}); an address is never reused")

    # A mark anywhere but a site is one no test can account for.
    marks = {(found.line, found.col + 1) for found in sites if found.marked}
    for number in (*prose, *(row for rows in tables for row in rows)):
        text = CODE_SPAN.sub(lambda span: " " * len(span.group()), lines[number])
        for stray in re.finditer(MARK, text):
            if (number, stray.start()) not in marks:
                error(number, f"a `{MARK}` outside a mark site; a mark stands straight after its statement's address, or ends its cell, and `{UPDATE}` writes it")
    return sites, errors


def test_files(root: Path) -> list[Path]:
    found: list[Path] = []
    for directory, names, files in os.walk(root / TESTS):
        names[:] = sorted(name for name in names if name != "target" and not name.startswith("."))
        found.extend(Path(directory) / name for name in sorted(files) if Path(name).suffix in COMMENTS)
    return found


def is_test_code(relative: Path, text: str) -> bool:
    """A `.gqt` case is a test. A Rust file is test code when it sits under a
    `tests` directory, is a `tests.rs` module, or holds a `#[cfg(test)]`."""
    return relative.suffix != ".rs" or "tests" in relative.parts or relative.stem == "tests" or "#[cfg(test)]" in text


def comments(root: Path, errors: list[str]) -> list[tuple[str, list[str]]]:
    """Every `contract:` comment as (`path:line`, addresses)."""
    found: list[tuple[str, list[str]]] = []
    for path in test_files(root):
        text = path.read_text(encoding="utf-8", errors="replace")
        if "contract:" not in text:
            continue
        relative = path.relative_to(root)
        near, form, spelling = COMMENTS[path.suffix]
        for number, line in enumerate(text.splitlines(), start=1):
            opened = near.search(line)
            if opened is None or not names_statements(opened.group(1)):
                continue
            where = f"{relative}:{number}"
            exact = form.match(line)
            if exact is None or not ADDRESS_LIST.fullmatch(exact.group(1)):
                errors.append(f"{where}: a comment that opens with `contract:` and a list names statements of {CONTRACT}; write `{spelling}` on a line of its own")
            elif not is_test_code(relative, text):
                errors.append(f"{where}: a `contract:` comment belongs to a test; this file is not test code")
            else:
                found.append((where, exact.group(1).split(", ")))
    return found


def check(root: Path, update: bool = False) -> tuple[list[str], str]:
    """Errors, and a summary line for a clean run."""
    page = root / CONTRACT
    lines = page.read_text(encoding="utf-8").split("\n")
    sites, errors = parse_page(lines)
    defined = {found.address for found in sites}
    named: dict[str, str] = {}
    named_by = 0
    for where, addresses in comments(root, errors):
        named_by += 1
        for address in addresses:
            if address in defined:
                named.setdefault(address, where)
            else:
                errors.append(f"{where}: names `{address}`, which {CONTRACT} does not define")

    drift = [found for found in sites if found.marked != (found.address in named)]
    if update and not errors:
        # Right to left, so an edit never moves a column still to be edited.
        for found in sorted(drift, key=lambda found: (found.line, found.col), reverse=True):
            line = lines[found.line]
            lines[found.line] = line[: found.col] + ("" if found.marked else " " + MARK) + line[found.col + (2 if found.marked else 0) :]
        if drift:
            page.write_text("\n".join(lines), encoding="utf-8")
        changed = ", ".join(sorted({("-" if found.marked else "+") + found.address for found in drift}))
        return [], f"Contract marks updated ({changed})." if drift else "Contract marks already match the tests."
    for found in drift:
        if found.marked:
            errors.append(f"{CONTRACT}:{found.line + 1}: `{found.address}` carries `{MARK}` and no test names it; a mark is never set by hand, `{UPDATE}` removes it")
        else:
            errors.append(f"{named[found.address]}: names `{found.address}`, which carries no `{MARK}` at {CONTRACT}:{found.line + 1}; `{UPDATE}` writes it")
    return errors, f"Contract marks OK ({len(defined)} addresses, {len(named)} named by {named_by} comments)."


# The self-test page: every address form, a legend table whose rows look like
# gap rows, a mark inside code, and kinds the real page does not have.
FIXTURE_PAGE = """# Fixture

| Mark | Meaning |
|---|---|
| `✓` | A named test holds it |
| `G1` | The code differs |

| Kind | Operations |
|---|---|
| `look` | `peek` |
| `alter` | `poke`, `a|b` |

**E1. It looks.** It reads one thing `G1`.
*Why:* a reason.

**E2. It alters.** It changes one thing.

| | sees | refuses when |
|---|---|---|
| **look** | a head | never `D1` |
| **alter** | a head | a broken rule |

| Pair | One side is refused when |
|---|---|
| `look+alter`, `alter+alter` | Something |
| `look+alter` | Something else `G1` |

### alter

1. `alter:1` A detail.
2. `alter:2` Another detail
   on two lines.

| Gap | Where | Today | See |
|---|---|---|---|
| `G1` | `E1` | Differs | None |

| Decision | Where | Today | Open question |
|---|---|---|---|
| `D1` † | `look.refuses` | Unchosen | Which? |

```text
Contract: keeps E1 ✓
```
"""
TEST = "crates/a/tests/t.rs"
CASE = "crates/a/cases/c.gqt"
NAMING = {
    TEST: "// HTTP contract: 0.1\n// Contract: prose about a caller, whose\n/// contract: any failure but an absent object\n"
    "    // contract: a scoped uncertainty leaves the process available.\n// contract: alter:1, look.sees\n#[test]\nfn t() {}\n",
    CASE: "# issue: none\n# notes: The case keeps its\n# notes: contract: both sides, loosely held.\n# notes: contract: look+alter, D1\n",
}
MARKED = (
    ("1. `alter:1`", "1. `alter:1` ✓"),
    ("| **look** | a head |", "| **look** | a head ✓ |"),
    ("| `look+alter`,", "| `look+alter` ✓,"),
    ("| `look+alter` |", "| `look+alter` ✓ |"),
    ("| `D1` †", "| `D1` ✓ †"),
)


def fixture(root: Path, name: str, edits: tuple[tuple[str, str], ...], files: dict[str, str]) -> Path:
    tree = root / re.sub(r"\W+", "-", name)
    text = FIXTURE_PAGE
    for old, new in edits:
        assert old in text, f"self-test fixture `{name}`: `{old}` is not on the fixture page"
        text = text.replace(old, new)
    for relative, body in {str(CONTRACT): text, **files}.items():
        target = tree / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
    return tree


def self_test() -> int:
    rule = (("**E1. It looks.**", "**E1. It looks.** ✓"),)
    # (name, page edits, test files, a phrase the refusal must carry; None accepts)
    cases: list[tuple[str, tuple[tuple[str, str], ...], dict[str, str], str | None]] = [
        ("nothing named, nothing marked", (), {}, None),
        ("every form named and marked", MARKED, NAMING, None),
        ("in-source test module", rule, {"crates/a/src/lib.rs": "#[cfg(test)]\nmod tests {\n    // contract: E1\n}\n"}, None),
        ("tests.rs module", rule, {"crates/a/src/db/tests.rs": "// contract: E1\n"}, None),
        ("unknown address", (), {TEST: "// contract: alter:9\n"}, "does not define"),
        ("unknown address beside a known one", MARKED, {**NAMING, TEST: NAMING[TEST] + "// contract: alter:1, look.cost\n"}, "`look.cost`, which"),
        ("hand-set mark on a rule", rule, {}, "no test names it"),
        ("hand-set mark on a cell", (("| a broken rule |", "| a broken rule ✓ |"),), {}, "`alter.refuses` carries"),
        ("hand-set mark on a gap", (("| `G1` | `E1`", "| `G1` ✓ | `E1`"),), {}, "no test names it"),
        ("named statement without its mark", (), {TEST: "// contract: E2\n"}, "carries no `✓`"),
        ("pair marked in one of its two rows", MARKED[:3] + MARKED[4:], NAMING, "names `look+alter`, which carries no"),
        ("mark at the end of a sentence", (("It changes one thing.", "It changes one thing ✓."),), {}, "outside a mark site"),
        ("mark in the legend table", (("| `G1` | The code differs |", "| `G1` ✓ | The code differs |"),), {}, "outside a mark site"),
        ("doc comment", rule, {TEST: "/// contract: E1\n"}, "on a line of its own"),
        ("no space after the slashes", rule, {TEST: "//contract: E1\n"}, "on a line of its own"),
        ("no space after the comma", rule, {TEST: "// contract: E1,E2\n"}, "on a line of its own"),
        ("no comma between addresses", rule, {TEST: "// contract: E1 E2\n"}, "on a line of its own"),
        ("a word that is no address", (), {TEST: "// contract: alter\n"}, "`alter`, which"),
        ("comment after code", rule, {TEST: "let x = 1; // contract: E1\n"}, "on a line of its own"),
        ("case header key", rule, {CASE: "# contract: E1\n"}, "# notes: contract:"),
        ("comment outside test code", rule, {"crates/a/src/lib.rs": "// contract: E1\n"}, "not test code"),
        ("pair out of order", (("`alter+alter`", "`alter+look`"),), {}, "order"),
        ("pair of an unknown kind", (("`alter+alter`", "`alter+poke`"),), {}, "not two kinds"),
        ("detail of an unknown kind", (("`alter:2`", "`poke:2`"),), {}, "names no kind"),
        ("address defined twice", (("`alter:2`", "`alter:1`"),), {}, "defined twice"),
        ("gap table missing", (("| Gap | Where", "| Hole | Where"),), {}, "first header cell is `Gap`"),
        ("two columns with one first word", (("| refuses when |", "| sees also |"),), {}, "two columns"),
    ]
    failed = 0
    with tempfile.TemporaryDirectory(prefix="check-contract-marks.") as scratch:
        root = Path(scratch)
        for name, edits, files, phrase in cases:
            errors, _ = check(fixture(root, name, edits, files))
            ok = not errors if phrase is None else any(phrase in error for error in errors)
            if not ok:
                failed += 1
                wanted = "accepted" if phrase is None else f"refused with `{phrase}`"
                print(f"error: self-test: `{name}` must be {wanted}, got {errors or 'no error'}", file=sys.stderr)

        # `--update` derives the marks: it adds the earned ones, removes the
        # hand-set one, reaches a page the check accepts, and is then a no-op.
        tree = fixture(root, "update", rule, NAMING)
        expected = (root / "every-form-named-and-marked" / CONTRACT).read_text(encoding="utf-8")
        steps = [
            ("the check refuses the page before the update", bool(check(tree)[0])),
            ("the update reports no error", not check(tree, update=True)[0]),
            ("the update writes the page the check accepts", (tree / CONTRACT).read_text(encoding="utf-8") == expected),
            ("the check accepts the updated page", not check(tree)[0]),
            ("a second update changes nothing", not check(tree, update=True)[0] and (tree / CONTRACT).read_text(encoding="utf-8") == expected),
        ]
        # An unknown address is an error `--update` cannot repair: it writes nothing.
        tree = fixture(root, "update refused", rule, {TEST: "// contract: alter:9\n"})
        before = (tree / CONTRACT).read_text(encoding="utf-8")
        steps.append(("the update refuses an unknown address and writes nothing", bool(check(tree, update=True)[0]) and (tree / CONTRACT).read_text(encoding="utf-8") == before))
        for name, ok in steps:
            if not ok:
                failed += 1
                print(f"error: self-test: {name}", file=sys.stderr)

    if failed:
        print(f"\ncontract marks self-test failed ({failed} of {len(cases) + len(steps)})", file=sys.stderr)
        return 1
    refused = sum(phrase is not None for *_, phrase in cases)
    print(f"self-test OK ({refused} fixtures refused, {len(cases) - refused} accepted, {len(steps)} update steps).")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if argv not in ([], ["--update"]):
        print(f"usage: {Path(__file__).name} [--update | --self-test]", file=sys.stderr)
        return 2
    errors, summary = check(ROOT, update=bool(argv))
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        print(f"\ncontract marks check failed ({len(errors)} errors)" + ("; nothing was rewritten" if argv else ""), file=sys.stderr)
        return 1
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
