#!/usr/bin/env python3
"""Validate PR titles and merge-queue squash subjects.

Text diagnostics identify each independently detectable error and its repair.
JSON is a versioned report with exact source ranges; --schema describes it.
--self-test runs the rule, serialization, color and CLI regression suite.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from enum import Enum
import json
import os
from pathlib import Path
import re
import sys
from typing import Literal, TypedDict, assert_never
import unittest

MAX_LEN = 100
SHAPE = "type(scope)!: description"


class CommitType(str, Enum):
    FEAT = "feat"
    FIX = "fix"
    PERF = "perf"
    REFACTOR = "refactor"
    DOCS = "docs"
    TEST = "test"
    CI = "ci"
    BUILD = "build"
    REVERT = "revert"
    RFC = "rfc"
    RELEASE = "release"


TYPES = tuple(item.value for item in CommitType)
EXAMPLES = (
    "feat(server): return exact merge publication receipts",
    "fix(merge): decide fast_forward by the target's state",
)
PR_SUFFIX = re.compile(r" \(#\d+\)$")
GITHUB_REVERT = re.compile(r'Revert\s+"')


class Field(str, Enum):
    TITLE = "title"
    PREFIX = "prefix"
    TYPE = "type"
    SCOPE = "scope"
    BREAKING = "breaking"
    DESCRIPTION = "description"


class Code(str, Enum):
    EMPTY_TITLE = "empty-title"
    LEADING_WHITESPACE = "leading-whitespace"
    TRAILING_WHITESPACE = "trailing-whitespace"
    NON_SPACE_WHITESPACE = "non-space-whitespace"
    TITLE_TOO_LONG = "title-too-long"
    REVERT_FORMAT = "revert-format"
    MISSING_TYPE = "missing-type"
    TYPE_CASE = "type-case"
    UNKNOWN_TYPE = "unknown-type"
    MISSING_COLON = "missing-colon"
    UNEXPECTED_PREFIX = "unexpected-prefix"
    PREFIX_WHITESPACE = "prefix-whitespace"
    UNCLOSED_SCOPE = "unclosed-scope"
    NESTED_SCOPE = "nested-scope"
    EMPTY_SCOPE_NAME = "empty-scope-name"
    SCOPE_CHARACTER = "scope-character"
    REPEATED_BANG = "repeated-bang"
    MISPLACED_BANG = "misplaced-bang"
    RFC_TYPE = "rfc-type"
    EMPTY_DESCRIPTION = "empty-description"
    COLON_SPACING = "colon-spacing"
    DESCRIPTION_CASE = "description-case"
    TRAILING_PERIOD = "trailing-period"

    @property
    def field(self) -> Field:
        match self:
            case Code.EMPTY_TITLE | Code.LEADING_WHITESPACE | Code.TRAILING_WHITESPACE | Code.NON_SPACE_WHITESPACE | Code.TITLE_TOO_LONG:
                return Field.TITLE
            case Code.REVERT_FORMAT | Code.MISSING_COLON | Code.UNEXPECTED_PREFIX | Code.PREFIX_WHITESPACE:
                return Field.PREFIX
            case Code.MISSING_TYPE | Code.TYPE_CASE | Code.UNKNOWN_TYPE | Code.RFC_TYPE:
                return Field.TYPE
            case Code.UNCLOSED_SCOPE | Code.NESTED_SCOPE | Code.EMPTY_SCOPE_NAME | Code.SCOPE_CHARACTER:
                return Field.SCOPE
            case Code.REPEATED_BANG | Code.MISPLACED_BANG:
                return Field.BREAKING
            case Code.EMPTY_DESCRIPTION | Code.COLON_SPACING | Code.DESCRIPTION_CASE | Code.TRAILING_PERIOD:
                return Field.DESCRIPTION
        assert_never(self)


class InputKind(str, Enum):
    TITLE = "title"
    SQUASH_SUBJECT = "squash-subject"


class OutputFormat(str, Enum):
    TEXT = "text"
    JSON = "json"


class Color(str, Enum):
    AUTO = "auto"
    ALWAYS = "always"
    NEVER = "never"


class SpanJSON(TypedDict):
    start: int
    end: int


class DiagnosticJSON(TypedDict):
    code: Code
    field: Field
    span: SpanJSON
    found: str
    expected: str
    message: str
    fix: str


class ReportJSON(TypedDict):
    schema_version: Literal[1]
    offset_unit: Literal["unicode-code-point"]
    input_kind: InputKind
    valid: bool
    title: str
    length: int
    max_length: int
    excess: int
    errors: list[DiagnosticJSON]


@dataclass(frozen=True)
class Span:
    """Zero-based, end-exclusive code-point offsets in the checked title."""
    start: int
    end: int

    def __post_init__(self) -> None:
        if type(self.start) is not int or type(self.end) is not int:
            raise TypeError("span offsets must be integers")
        if not 0 <= self.start <= self.end:
            raise ValueError("span requires 0 <= start <= end")

    def to_json(self) -> SpanJSON:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True)
class Diagnostic:
    code: Code
    span: Span
    found: str
    expected: str
    message: str
    fix: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, Code) or not isinstance(self.span, Span):
            raise TypeError("diagnostic requires a Code and Span")
        if not all(isinstance(value, str) for value in (self.found, self.expected, self.message, self.fix)):
            raise TypeError("diagnostic text must be strings")
        if not self.expected or not self.message or not self.fix:
            raise ValueError("diagnostic requires expected, message and fix")
        if len(self.found) != self.span.end - self.span.start:
            raise ValueError("found text must have the span's code-point length")

    def to_json(self) -> DiagnosticJSON:
        return {
            "code": self.code, "field": self.code.field, "span": self.span.to_json(),
            "found": self.found, "expected": self.expected,
            "message": self.message, "fix": self.fix,
        }


@dataclass(frozen=True)
class Report:
    title: str
    input_kind: InputKind
    errors: tuple[Diagnostic, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.title, str) or not isinstance(self.input_kind, InputKind):
            raise TypeError("report requires a string title and InputKind")
        if not isinstance(self.errors, tuple) or not all(isinstance(e, Diagnostic) for e in self.errors):
            raise TypeError("report errors must be an immutable tuple of Diagnostic")
        for error in self.errors:
            if error.span.end > len(self.title) or self.title[error.span.start:error.span.end] != error.found:
                raise ValueError("diagnostic found text must equal the original title slice")

    @property
    def valid(self) -> bool:
        return not self.errors

    def to_json(self) -> ReportJSON:
        return {
            "schema_version": 1, "offset_unit": "unicode-code-point",
            "input_kind": self.input_kind, "valid": self.valid, "title": self.title,
            "length": len(self.title), "max_length": MAX_LEN,
            "excess": max(0, len(self.title) - MAX_LEN),
            "errors": [error.to_json() for error in self.errors],
        }


def diagnose(title: str, input_kind: InputKind = InputKind.TITLE) -> Report:
    if not isinstance(title, str) or not isinstance(input_kind, InputKind):
        raise TypeError("diagnose requires a string title and InputKind")
    errors: list[Diagnostic] = []

    def add(code: Code, start: int, end: int, expected: str, message: str, fix: str) -> None:
        errors.append(Diagnostic(code, Span(start, end), title[start:end], expected, message, fix))

    def result() -> Report:
        return Report(title, input_kind, tuple(errors))

    left = len(title) - len(title.lstrip())
    right = max(left, len(title.rstrip()))
    if left:
        add(Code.LEADING_WHITESPACE, 0, left, "no leading whitespace",
            f"leading whitespace {title[:left]!r}", "Remove the leading whitespace.")
    if right < len(title):
        add(Code.TRAILING_WHITESPACE, right, len(title), "no trailing whitespace",
            f"trailing whitespace {title[right:]!r}", "Remove the trailing whitespace.")
    for index, char in enumerate(title):
        if char.isspace() and char != " ":
            add(Code.NON_SPACE_WHITESPACE, index, index + 1, "plain space U+0020",
                f"non-space whitespace {char!r} (U+{ord(char):04X}) at character {index + 1}",
                "Replace this whitespace with a plain space, or remove it at a title boundary.")
    if len(title) > MAX_LEN:
        excess = len(title) - MAX_LEN
        unit = "character" if excess == 1 else "characters"
        add(Code.TITLE_TOO_LONG, MAX_LEN, len(title), f"at most {MAX_LEN} characters",
            f"{len(title)} characters; at most {MAX_LEN} ({excess} over the limit)",
            f"Shorten the title by at least {excess} {unit}.")
    if left == right:
        add(Code.EMPTY_TITLE, left, right, SHAPE, "empty title", f"Write a title using {SHAPE}.")
        return result()
    revert = GITHUB_REVERT.match(title, left, right)
    if revert:
        add(Code.REVERT_FORMAT, left, revert.end(), "revert:",
            "GitHub's default Revert title is not accepted", "Use revert: <what is undone>.")
        return result()
    type_match = re.compile(r"[A-Za-z]+").match(title, left, right)
    if type_match is None:
        add(Code.MISSING_TYPE, left, left + 1, "a permitted lowercase type",
            f"expected a type at the start; found {title[left]!r}", f"Start with one of: {', '.join(TYPES)}.")
        return result()
    raw_type = type_match.group()
    pos = type_match.end()
    try:
        type_ = CommitType(raw_type.lower())
    except ValueError:
        type_ = None
        add(Code.UNKNOWN_TYPE, left, pos, ", ".join(TYPES),
            f"unknown type {raw_type!r}", f"Choose one of: {', '.join(TYPES)}.")
    if type_ is not None and raw_type != type_.value:
        add(Code.TYPE_CASE, left, pos, type_.value,
            f"type {raw_type!r} is spelled in capitals", f"Use {type_.value!r}.")

    def prefix_gap(index: int) -> int:
        end = index
        while end < right and title[end].isspace():
            end += 1
        if end > index and end < right and title[end] in "(!:":
            add(Code.PREFIX_WHITESPACE, index, end, "no whitespace inside the prefix",
                f"unexpected prefix whitespace {title[index:end]!r}", "Remove this whitespace before the prefix punctuation.")
            return end
        return index

    pos = prefix_gap(pos)
    scope_seen = False
    if pos < right and title[pos] == "(":
        scope_seen = True
        opening = pos
        depth = 1
        pos += 1
        nested = False
        while pos < right and depth:
            if title[pos] == "(":
                nested = True
                depth += 1
                add(Code.NESTED_SCOPE, pos, pos + 1, "one unnested scope list",
                    "nested '(' inside the scope", "Use one parenthesized scope list, such as (server,api).")
            elif title[pos] == ")":
                depth -= 1
            pos += 1
        if depth:
            add(Code.UNCLOSED_SCOPE, opening, opening + 1, "a closing ')' for this '('",
                f"scope opened at character {opening + 1} has no closing ')'",
                "Close the scope with ')' before the optional '!' and ':'.")
            return result()
        scope_start, scope_end = opening + 1, pos - 1
        scope = title[scope_start:scope_end]
        if not nested:
            if type_ is CommitType.DOCS and scope in ("rfc", "rfcs"):
                add(Code.RFC_TYPE, left, pos, "rfc",
                    f"docs({scope}) is not the RFC type", "Replace the type and scope with rfc; retain '!' if present.")
            name_start = scope_start
            for name in scope.split(","):
                if not name:
                    add(Code.EMPTY_SCOPE_NAME, name_start, name_start, "a nonempty scope name",
                        "empty scope name", "Supply a lowercase scope name, remove an extra comma, or omit the scope.")
                name_start += len(name) + 1
            for bad in re.finditer(r"[^a-z0-9_,-]+", scope):
                start, end = scope_start + bad.start(), scope_start + bad.end()
                add(Code.SCOPE_CHARACTER, start, end, "lowercase a-z, digits, '_' or '-' in each scope name",
                    f"invalid scope characters {title[start:end]!r}",
                    "Use lowercase names separated by commas, without spaces.")
        pos = prefix_gap(pos)
    if pos < right and title[pos] == "!":
        bang = pos
        while pos < right and title[pos] == "!":
            pos += 1
        if pos - bang > 1:
            add(Code.REPEATED_BANG, bang + 1, pos, "at most one '!'",
                f"found {pos - bang} breaking markers", "Keep one '!' immediately before ':'.")
        pos = prefix_gap(pos)
        if not scope_seen and pos < right and title[pos] == "(":
            add(Code.MISPLACED_BANG, bang, bang + 1, "'!' after the scope",
                "breaking marker precedes the scope", "Move '!' after the closing ')' and immediately before ':'.")
            return result()
    if pos >= right or title[pos] != ":":
        colon = title.find(":", pos, right)
        if colon < 0:
            add(Code.MISSING_COLON, pos, pos, ":",
                "missing ':' after the type, optional scope and optional '!'",
                "Insert ':' immediately after the prefix, followed by one space and a description.")
        else:
            add(Code.UNEXPECTED_PREFIX, pos, colon, "':' after the type, optional scope and optional '!'",
                f"unexpected prefix text {title[pos:colon]!r}", f"Use {SHAPE}; remove or correct this prefix text.")
        return result()
    pos += 1
    desc_start = pos
    while desc_start < right and title[desc_start].isspace():
        desc_start += 1
    if desc_start == right:
        add(Code.EMPTY_DESCRIPTION, desc_start, desc_start, "a nonempty description",
            "empty description", "Add one space after ':' and a description that does not start uppercase.")
        return result()
    if title[pos:desc_start] != " ":
        add(Code.COLON_SPACING, pos, desc_start, "exactly one plain space",
            f"expected exactly one space after the colon; found {title[pos:desc_start]!r}",
            "Put exactly one plain space between ':' and the description.")
    if title[desc_start].isupper():
        add(Code.DESCRIPTION_CASE, desc_start, desc_start + 1, "a first character that is not uppercase",
            f"description starts with a capital {title[desc_start]!r}",
            "Start lowercase; digits, punctuation and uncased characters also pass.")
    if title[right - 1] == ".":
        add(Code.TRAILING_PERIOD, right - 1, right, "no final '.'",
            "description ends with a period", "Remove the final period.")
    return result()


def squash_title(message: str) -> str:
    subject = message.splitlines()[0] if message.strip() else ""
    return PR_SUFFIX.sub("", subject.strip())


def use_color(mode: Color) -> bool:
    if not isinstance(mode, Color):
        raise TypeError("color must be a Color")
    match mode:
        case Color.ALWAYS:
            return True
        case Color.NEVER:
            return False
        case Color.AUTO:
            return not os.environ.get("NO_COLOR") and (
                os.environ.get("GITHUB_ACTIONS") == "true" or sys.stdout.isatty()
            )
    assert_never(mode)


def location(span: Span) -> str:
    if span.start == span.end:
        return f"insertion at offset {span.start}"
    if span.end == span.start + 1:
        return f"character {span.start + 1}"
    return f"characters {span.start + 1}-{span.end}"


def text_report(report: Report, color: bool = False) -> str:
    def paint(text: str, style: str) -> str:
        return f"\x1b[{style}m{text}\x1b[0m" if color else text

    if report.valid:
        return f"{paint('PASS', '1;32')} PR title accepted ({len(report.title)}/{MAX_LEN} characters): {report.title!r}"
    count = len(report.errors)
    lines = [
        paint(f"FAIL PR title: {count} problem{'s' if count != 1 else ''}", "1;31"),
        f"  Title: {report.title!r}", f"  Length: {len(report.title)}/{MAX_LEN} characters", "",
    ]
    for error in report.errors:
        lines.extend([
            paint(f"  [{error.code.value}] {error.message}", "1;31"),
            f"  At: {error.code.field.value}, {location(error.span)}",
            paint(f"  Fix: {error.fix}", "33"), "",
        ])
    if any(error.code in (Code.EMPTY_TITLE, Code.MISSING_TYPE, Code.UNKNOWN_TYPE, Code.MISSING_COLON, Code.UNEXPECTED_PREFIX) for error in report.errors):
        lines.extend([f"  Shape: {SHAPE}", f"  Types: {', '.join(TYPES)}",
                      *(f"  Example: {example}" for example in EXAMPLES), ""])
    lines.append(paint("  Rule: CONTRIBUTING.md, Pull Requests", "36"))
    return "\n".join(lines)


def annotation(error: Diagnostic) -> str:
    data = f"[{error.code.value}] {error.message}. At: {location(error.span)}. Fix: {error.fix}"
    data = data.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return f"::error title=PR Title::{data}"


def emit(report: Report, format_: OutputFormat, color: Color) -> int:
    if not isinstance(report, Report) or not isinstance(format_, OutputFormat) or not isinstance(color, Color):
        raise TypeError("emit requires a Report, OutputFormat and Color")
    match format_:
        case OutputFormat.JSON:
            print(json.dumps(report.to_json()))
        case OutputFormat.TEXT:
            print(text_report(report, use_color(color)))
            if os.environ.get("GITHUB_ACTIONS") == "true":
                for error in report.errors:
                    print(annotation(error))
        case _:
            assert_never(format_)
    return int(not report.valid)


def report_schema() -> dict[str, object]:
    def record(properties: dict[str, object]) -> dict[str, object]:
        return {"type": "object", "additionalProperties": False,
                "required": list(properties), "properties": properties}

    natural: dict[str, object] = {"type": "integer", "minimum": 0}
    text: dict[str, object] = {"type": "string", "minLength": 1}
    error = record({
        "code": {"enum": [code.value for code in Code]},
        "field": {"enum": [field.value for field in Field]},
        "span": record({"start": natural, "end": natural}),
        "found": {"type": "string"}, "expected": text, "message": text, "fix": text,
    })
    error["allOf"] = [
        {"if": {"properties": {"code": {"const": code.value}}},
         "then": {"properties": {"field": {"const": code.field.value}}}}
        for code in Code
    ]
    schema = record({
        "schema_version": {"type": "integer", "const": 1},
        "offset_unit": {"const": "unicode-code-point"},
        "input_kind": {"enum": [kind.value for kind in InputKind]},
        "valid": {"type": "boolean"}, "title": {"type": "string"},
        "length": natural, "max_length": {"type": "integer", "const": MAX_LEN},
        "excess": natural, "errors": {"type": "array", "items": error},
    })
    schema.update({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "PR title diagnostic report",
        "description": "Ranges refer to title, in zero-based, end-exclusive Unicode code points. Missing tokens have zero-width spans.",
        "allOf": [
            {"if": {"properties": {"valid": {"const": True}}},
             "then": {"properties": {"errors": {"maxItems": 0}, "excess": {"const": 0}}},
             "else": {"properties": {"errors": {"minItems": 1}}}},
        ],
    })
    return schema


def self_test() -> int:
    suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent), pattern="test_check_pr_title.py")
    if not suite.countTestCases():
        raise RuntimeError("PR title regression tests were not discovered")
    return int(not unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--title", help="a pull request title")
    mode.add_argument("--squash-subject", help="a merge-queue message; check the first line minus its final (#N)")
    mode.add_argument("--self-test", action="store_true", help="run the regression suite")
    mode.add_argument("--schema", action="store_true", help="print the report's JSON Schema")
    parser.add_argument("--format", choices=[item.value for item in OutputFormat], default=OutputFormat.TEXT.value)
    parser.add_argument("--color", choices=[item.value for item in Color], default=Color.AUTO.value,
                        help="text color: auto on a terminal or Actions unless NO_COLOR is nonempty")
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()
    if args.schema:
        print(json.dumps(report_schema(), indent=2))
        return 0
    kind = InputKind.TITLE if args.title is not None else InputKind.SQUASH_SUBJECT
    title = args.title if kind is InputKind.TITLE else squash_title(args.squash_subject)
    return emit(diagnose(title, kind), OutputFormat(args.format), Color(args.color))


if __name__ == "__main__":
    sys.exit(main())
