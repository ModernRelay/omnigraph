#!/usr/bin/env python3
"""Seam coverage over the GQ logic test corpus.

Reads every decision seam declared in the engine's sources
(`crates/omnigraph/src/**/*.rs`, each static beside the site it guards and
indexed by `crates/omnigraph/src/seams/catalog.rs`), every `at:` a corpus
case names (`crates/omnigraph-gqt/cases/*.gqt`), and prints one row per
seam: name, where it is declared, operation, declared effects, and the cases
that prove it. Seams whose
operation no GQ step starts are the reachability debt; reachable seams with
no case are the coverage debt. `--check` exits non-zero when a case names a
seam the catalog does not have, or an action that none of its effects admits.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
ENGINE_SRC = ROOT / "crates" / "omnigraph" / "src"
CASES = ROOT / "crates" / "omnigraph-gqt" / "cases"

STATIC = re.compile(
    r'pub static \w+ = \(\s*"(?P<name>[^"]+)"\s*,\s*(?P<op>\w+)\s*,\s*\[(?P<effects>[^\]]*)\]',
    re.S,
)
EFFECT = re.compile(r"\w+")
DIRECTIVE = re.compile(r"^--- seam\s*\n(?P<body>(?:(?!^---).*\n)*)", re.M)
FIELD = re.compile(r"^\s*(\w+):\s*(.+?)\s*$", re.M)

ADMITS = {"fail": {"Fail", "Contention"}, "skip": {"Skip"}}


def catalog() -> dict[str, tuple[str, str, tuple[str, ...]]]:
    """name -> (declared at `file:line`, op, effects), from every static under the engine's src."""
    seams = {}
    for path in sorted(ENGINE_SRC.rglob("*.rs")):
        text = path.read_text()
        for m in STATIC.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            where = f"{path.relative_to(ROOT)}:{line}"
            seams[m["name"]] = (where, m["op"], tuple(EFFECT.findall(m["effects"])))
    return seams


def scalar(value: str) -> str:
    """A YAML plain or quoted scalar as the runner reads it: comment stripped, quotes removed."""
    value = value.split(" #", 1)[0].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return value


def fields_of(body: str) -> dict[str, str]:
    """The directive's fields as the runner's YAML reader sees them; PyYAML when
    installed (quoted keys, escapes, flow mappings, folded scalars all agree),
    the plain-scalar fallback otherwise."""
    try:
        import yaml  # type: ignore

        loaded = yaml.safe_load(body)
        if isinstance(loaded, dict):
            return {str(k): str(v) for k, v in loaded.items()}
    except ImportError:
        pass
    except Exception as error:  # noqa: BLE001 - the runner reports the real error
        print(f"NOTE: fallback field reader on an unparseable body: {error}", file=sys.stderr)
    return {k: scalar(v) for k, v in FIELD.findall(body)}


def corpus() -> list[tuple[str, str, str]]:
    rows = []
    for path in sorted(CASES.glob("*.gqt")):
        text = path.read_text()
        if not text.endswith("\n"):
            text += "\n"
        for m in DIRECTIVE.finditer(text):
            fields = fields_of(m["body"])
            rows.append((path.name, fields.get("at", ""), fields.get("action", "")))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on an unknown seam or a mismatched action")
    args = parser.parse_args()

    seams = catalog()
    uses = corpus()
    problems = []
    by_seam: dict[str, list[str]] = {name: [] for name in seams}
    for case, at, action in uses:
        if at not in seams:
            problems.append(f"{case}: names unknown seam {at!r}")
            continue
        effects = seams[at][2]
        if not ADMITS.get(action, set()) & set(effects):
            problems.append(f"{case}: action {action!r} is not admitted by seam {at} (effects {', '.join(effects)})")
            continue
        by_seam[at].append(case)

    print("| seam | declared at | op | effects | cases |")
    print("|---|---|---|---|---|")
    for name, (where, op, effects) in sorted(seams.items()):
        cases = ", ".join(sorted(set(by_seam[name]))) or ("unreachable" if op == "Unreachable" else "none")
        print(f"| {name} | {where} | {op} | {', '.join(effects)} | {cases} |")
    reachable = [n for n, (_, op, _) in seams.items() if op != "Unreachable"]
    covered = [n for n in reachable if by_seam[n]]
    print()
    print(f"seams: {len(seams)}, reachable: {len(reachable)}, with a case: {len(covered)}, "
          f"reachable without a case: {len(reachable) - len(covered)}, unreachable: {len(seams) - len(reachable)}")
    for problem in problems:
        print(f"PROBLEM: {problem}", file=sys.stderr)
    return 1 if (args.check and problems) else 0


if __name__ == "__main__":
    sys.exit(main())
