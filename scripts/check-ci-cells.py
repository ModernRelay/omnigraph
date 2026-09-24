#!/usr/bin/env python3
"""Every test a workflow requires by name is defined in the sources.

The RustFS and Azurite jobs run object-store owners by exact name and then
require each name's `... ok` line in the log. A cargo filter that matches no
test passes with zero tests, so a deleted or renamed owner only surfaces when
that job runs. This check reports without building anything: it reads every
name a workflow requires and refuses one that no Rust function defines.

Workflow lines are joined across `\\` continuations and split into shell words
(quotes and `#` comments handled as bash does), and a name is: the first word
after `run_cell`; every word after `for test_name in` up to `;` or `do`; the
word before `--` when `--exact` follows. A `tests::` module prefix is
stripped. A line that mentions one of those forms but does not tokenize is
refused rather than skipped. Rust definitions are collected after comments,
string and char literals are removed, so a commented-out test or a signature
quoted in a doc example does not count. A workflow set with no required name
at all is refused too. Run from the repository root; `--self-test` runs the
in-memory cases first.
"""

from __future__ import annotations

import re
import shlex
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / ".github" / "workflows"
SOURCE_ROOT = REPO_ROOT / "crates"
FN_NAME = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")
CANDIDATE = re.compile(r"run_cell|test_name|--exact")
NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_:]*$")


def logical_lines(text: str) -> list[tuple[int, str]]:
    """(first line number, text) with `\\` continuations joined."""
    out: list[tuple[int, str]] = []
    start, parts = 0, []
    for number, raw in enumerate(text.replace("\r\n", "\n").split("\n"), 1):
        if not parts:
            start = number
        stripped = raw.rstrip()
        if stripped.endswith("\\"):
            parts.append(stripped[:-1])
            continue
        parts.append(stripped)
        out.append((start, " ".join(parts)))
        parts = []
    if parts:
        out.append((start, " ".join(parts)))
    return out


def shell_words(line: str) -> list[str]:
    lexer = shlex.shlex(line, posix=True, punctuation_chars=";|&")
    lexer.whitespace_split = True
    lexer.commenters = "#"
    return list(lexer)


def required_names(text: str) -> tuple[list[tuple[int, str]], list[str]]:
    """(line number, name) for every required test, and the lines refused."""
    found: list[tuple[int, str]] = []
    refused: list[str] = []
    for number, line in logical_lines(text):
        if not CANDIDATE.search(line):
            continue
        try:
            words = shell_words(line)
        except ValueError as error:
            refused.append(f"line {number}: cannot tokenize a line naming a required test: {error}")
            continue
        for i, word in enumerate(words):
            if word == "run_cell" and i + 1 < len(words):
                found.append((number, words[i + 1]))
            elif word == "--exact" and i >= 2 and words[i - 1] == "--":
                found.append((number, words[i - 2]))
        if words[:3] == ["for", "test_name", "in"]:
            for word in words[3:]:
                if word in (";", "do"):
                    break
                found.append((number, word))
    kept: list[tuple[int, str]] = []
    for number, name in found:
        if name.startswith("$"):
            continue
        if not NAME.match(name):
            refused.append(f"line {number}: {name!r} is not a test name")
            continue
        kept.append((number, name))
    return kept, refused


def code_only(text: str) -> str:
    """Rust source with comments, string literals and char literals blanked."""
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        two = text[i : i + 2]
        if two == "//":
            end = text.find("\n", i)
            i = n if end == -1 else end
        elif two == "/*":
            depth, i = 1, i + 2
            while i < n and depth:
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif ch == "b" and text[i + 1 : i + 2] == '"':
            i = skip_string(text, i + 1)
        elif ch == '"':
            i = skip_string(text, i)
        elif (ch == "r" or two == "br") and re.match(r"b?r#*\"", text[i:]):
            hashes = len(re.match(r"b?r(#*)\"", text[i:]).group(1))
            close = '"' + "#" * hashes
            end = text.find(close, i + hashes + (3 if two == "br" else 2))
            i = n if end == -1 else end + len(close)
        elif ch == "'" and (
            re.match(r"'\\(?:x[0-9A-Fa-f]{2}|u\{[0-9A-Fa-f]+\}|.)'", text[i:])
            or re.match(r"'[^'\\]'", text[i:])
        ):
            i = text.find("'", i + 2) + 1
        else:
            out.append(ch)
            i += 1
            continue
        out.append(" ")
    return "".join(out)


def skip_string(text: str, i: int) -> int:
    """Index just past the string literal opening at text[i] == '\"'."""
    i += 1
    while i < len(text):
        if text[i] == "\\":
            i += 2
        elif text[i] == '"':
            return i + 1
        else:
            i += 1
    return i


def defined_functions(sources: list[str]) -> set[str]:
    names: set[str] = set()
    for text in sources:
        names.update(FN_NAME.findall(code_only(text)))
    return names


def validate(workflows: dict[str, str], defined: set[str]) -> list[str]:
    failures: list[str] = []
    total = 0
    for path, text in workflows.items():
        names, refused = required_names(text)
        failures.extend(f"{path}: {reason}" for reason in refused)
        for line, name in names:
            total += 1
            if name.rsplit("::", 1)[-1] not in defined:
                failures.append(f"{path}:{line}: required test {name!r} is defined by no `fn` under crates/")
    if total == 0:
        failures.append("no required test name found in any workflow; the extractor is blind")
    return failures


def load() -> tuple[dict[str, str], set[str]]:
    workflows = {
        str(path.relative_to(REPO_ROOT)): path.read_text(encoding="utf-8")
        for path in sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))
    }
    sources = [path.read_text(encoding="utf-8") for path in sorted(SOURCE_ROOT.rglob("*.rs"))]
    return workflows, defined_functions(sources)


def self_test() -> None:
    workflow = (
        "      run: |\n"
        "          run_cell() {\n"
        '            cargo test --locked "$@" "$name" -- --exact --nocapture 2>&1 | tee -a "$log"\n'
        "          }\n"
        "          run_cell tests::alpha_owner -p a\n"
        '          run_cell "quoted_owner" -p a  # trailing comment\n'
        "          cargo test --locked -p b --test t \\\n"
        "            beta_owner \\\n"
        "            -- --exact --nocapture 2>&1 | tee -a \"$log\"\n"
        "          for test_name in \\\n"
        "            gamma_owner \\\n"
        "            'delta_owner'  # comment after an item\n"
        "          do\n"
        "            require_cell \"$test_name\"\n"
        "          done\n"
        "          for test_name in inline_owner; do require_cell \"$test_name\"; done\n"
    )
    defined = defined_functions([
        "async fn alpha_owner() {}\nfn beta_owner() {}\npub fn quoted_owner<T>() {}\n",
        "#[test]\nfn gamma_owner() {}\nfn delta_owner() {}\nfn inline_owner() {}\n",
    ])
    names, refused = required_names(workflow)
    assert refused == [], refused
    assert sorted(name for _, name in names) == [
        "beta_owner", "delta_owner", "gamma_owner", "inline_owner", "quoted_owner", "tests::alpha_owner",
    ], names
    assert validate({"ci.yml": workflow}, defined) == []
    for missing in ("quoted_owner", "delta_owner", "inline_owner", "beta_owner"):
        failures = validate({"ci.yml": workflow}, defined - {missing})
        assert failures and missing in failures[0], (missing, failures)
    prefixed = validate({"ci.yml": workflow}, defined - {"alpha_owner"})
    assert prefixed and "tests::alpha_owner" in prefixed[0], prefixed
    unclosed = validate({"ci.yml": "run_cell 'unterminated -p a\n"}, defined)
    assert any("cannot tokenize" in f for f in unclosed), unclosed
    assert validate({"ci.yml": "      run: cargo test\n"}, defined) == [
        "no required test name found in any workflow; the extractor is blind"
    ]
    for hidden in (
        "// fn removed_owner() {}\n",
        "/* fn removed_owner() {} */\n",
        "/* outer /* fn removed_owner() {} */ still comment */\n",
        'const DOC: &str = "fn removed_owner() {}";\n',
        'const RAW: &str = r#"fn removed_owner() {}"#;\n',
        "const C: char = '\"'; // fn removed_owner() {}\n",
    ):
        assert "removed_owner" not in defined_functions([hidden]), hidden
    assert defined_functions(["fn kept<'a>(x: &'a str) -> char { '\\'' }\nfn after_char() {}\n"]) == {
        "kept", "after_char",
    }
    print("self-test ok")


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        self_test()
    workflows, defined = load()
    failures = validate(workflows, defined)
    for failure in failures:
        print(f"::error::{failure}")
    if failures:
        return 1
    count = sum(len(required_names(text)[0]) for text in workflows.values())
    print(f"ok: {count} required test names are defined under crates/")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
