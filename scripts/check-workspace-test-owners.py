#!/usr/bin/env python3
"""Every owner `Test Workspace` requires by name passed, per nextest's JUnit report.

`Test Workspace` runs the workspace graph under `cargo nextest run --profile ci`
(.config/nextest.toml writes `target/nextest/ci/junit.xml` with captured
output). A cargo filter that matches no test passes with zero tests, and a
journey that skips its real work prints a notice and passes, so the job names
every required owner and checks this report instead of grepping a log:

  --passed NAME             at least one test case named NAME passed (any binary)
  --output-has NAME=TEXT    a passed NAME captured TEXT on stdout or stderr
  --output-lacks NAME=TEXT  no passed NAME captured TEXT

Every NAME, from whichever option, must have passed: a NAME that is absent,
skipped, failed or errored is refused, so an output assertion alone never
accepts a journey that did not run. `--self-test` runs the in-memory cases
first. Run from the repository root.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


class Case:
    def __init__(self, classname: str, name: str, status: str, output: str) -> None:
        self.classname = classname
        self.name = name
        self.status = status
        self.output = output


def parse_report(text: str) -> list[Case]:
    cases: list[Case] = []
    for element in ET.fromstring(text).iter("testcase"):
        if element.find("failure") is not None or element.find("error") is not None:
            status = "failed"
        elif element.find("skipped") is not None:
            status = "skipped"
        else:
            status = "passed"
        output = "".join(
            (child.text or "") for tag in ("system-out", "system-err") for child in element.findall(tag)
        )
        cases.append(Case(element.get("classname", ""), element.get("name", ""), status, output))
    return cases


def split_assertion(spec: str) -> tuple[str, str]:
    name, separator, text = spec.partition("=")
    if not separator or not name or not text:
        raise SystemExit(f"error: expected NAME=TEXT, got {spec!r}")
    return name, text


def check(cases: list[Case], passed: list[str], has: list[str], lacks: list[str]) -> list[str]:
    """Every refusal, in `::error::` form; empty when the report satisfies the job."""
    by_name: dict[str, list[Case]] = {}
    for case in cases:
        by_name.setdefault(case.name, []).append(case)
    errors: list[str] = []
    required = list(dict.fromkeys(passed + [split_assertion(spec)[0] for spec in has + lacks]))
    for name in required:
        matches = by_name.get(name, [])
        if not matches:
            errors.append(f"::error::required owner did not run: {name}")
        elif not any(case.status == "passed" for case in matches):
            verdicts = ", ".join(f"{case.classname} {case.status}" for case in matches)
            errors.append(f"::error::required owner did not pass: {name} ({verdicts})")
    for spec in has:
        name, text = split_assertion(spec)
        if not any(case.status == "passed" and text in case.output for case in by_name.get(name, [])):
            errors.append(f"::error::{name} did not report {text!r}")
    for spec in lacks:
        name, text = split_assertion(spec)
        if any(case.status == "passed" and text in case.output for case in by_name.get(name, [])):
            errors.append(f"::error::{name} skipped its real work: {text!r}")
    return errors


def self_test() -> None:
    report = """<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="test-workspace" tests="4" failures="1" errors="0">
  <testsuite name="omnigraph-cli::crossversion_upgrade" tests="3" failures="1" errors="0">
    <testcase name="journey_ok" classname="omnigraph-cli::crossversion_upgrade">
      <system-out>stdout line</system-out>
      <system-err>v0.9 refusal and export/import rebuild completed
</system-err>
    </testcase>
    <testcase name="journey_skipped" classname="omnigraph-cli::crossversion_upgrade">
      <system-err>skipping v0.9 upgrade e2e: OMNIGRAPH_V09_BIN is unset
</system-err>
    </testcase>
    <testcase name="journey_red" classname="omnigraph-cli::crossversion_upgrade">
      <failure message="assertion failed" type="test failure">panicked</failure>
    </testcase>
  </testsuite>
  <testsuite name="omnigraph-server::remote" tests="1" failures="0" errors="0">
    <testcase name="ignored_owner" classname="omnigraph-server::remote">
      <skipped/>
    </testcase>
  </testsuite>
</testsuites>
"""
    cases = parse_report(report)
    assert [case.status for case in cases] == ["passed", "passed", "failed", "skipped"], cases
    assert check(cases, ["journey_ok"], [], []) == []
    assert check(
        cases,
        ["journey_ok"],
        ["journey_ok=v0.9 refusal and export/import rebuild completed", "journey_ok=stdout line"],
        ["journey_ok=skipping v0.9 upgrade e2e:"],
    ) == []
    assert check(cases, ["absent_owner"], [], []) == ["::error::required owner did not run: absent_owner"]
    assert check(cases, ["journey_red"], [], []) == [
        "::error::required owner did not pass: journey_red (omnigraph-cli::crossversion_upgrade failed)"
    ]
    assert check(cases, ["ignored_owner"], [], []) == [
        "::error::required owner did not pass: ignored_owner (omnigraph-server::remote skipped)"
    ]
    assert check(cases, [], ["journey_ok=never printed"], []) == [
        "::error::journey_ok did not report 'never printed'"
    ]
    assert check(cases, [], [], ["journey_skipped=skipping v0.9 upgrade e2e:"]) == [
        "::error::journey_skipped skipped its real work: 'skipping v0.9 upgrade e2e:'"
    ]
    assert check(cases, [], ["journey_red=panicked"], []) == [
        "::error::required owner did not pass: journey_red (omnigraph-cli::crossversion_upgrade failed)",
        "::error::journey_red did not report 'panicked'",
    ]
    assert check(cases, [], [], ["absent_owner=skipping v0.9 upgrade e2e:"]) == [
        "::error::required owner did not run: absent_owner"
    ]
    assert check(cases, [], [], ["ignored_owner=skipping v0.9 upgrade e2e:"]) == [
        "::error::required owner did not pass: ignored_owner (omnigraph-server::remote skipped)"
    ]
    try:
        split_assertion("no_separator")
    except SystemExit:
        pass
    else:
        raise AssertionError("NAME without =TEXT was accepted")
    print("self-test OK (11 report shapes)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("report", nargs="?", type=Path, help="nextest JUnit report")
    parser.add_argument("--passed", action="append", default=[], metavar="NAME")
    parser.add_argument("--output-has", action="append", default=[], metavar="NAME=TEXT")
    parser.add_argument("--output-lacks", action="append", default=[], metavar="NAME=TEXT")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        if args.report is None:
            return 0
    if args.report is None:
        parser.error("a JUnit report path is required unless --self-test runs alone")
    if not (args.passed or args.output_has or args.output_lacks):
        parser.error("no owner or output assertion given; the check would pass vacuously")
    if not args.report.is_file():
        print(f"::error::nextest wrote no JUnit report at {args.report}", file=sys.stderr)
        return 1
    errors = check(parse_report(args.report.read_text()), args.passed, args.output_has, args.output_lacks)
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print(f"workspace owners OK ({len(args.passed)} passed, {len(args.output_has)} output present, {len(args.output_lacks)} skips absent)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
