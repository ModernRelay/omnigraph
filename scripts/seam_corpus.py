#!/usr/bin/env python3
"""Seam coverage over the GQ logic test corpus.

Reads every decision seam declared in the engine's sources
(`crates/omnigraph/src/**/*.rs`, each static beside the site it guards and
indexed by `crates/omnigraph/src/seams/catalog.rs`), every store place in
`STORE_PLACES` (`crates/omnigraph-dst/src/store_places.rs`), every `at:` a
corpus case names (`crates/omnigraph-gqt/cases/*.gqt`), and prints one row
per seam and per store place: name, where it is declared, operation,
declared effects (store effects and admitted store actions prefixed
`store:`), and the cases that prove it. Seams whose
operation no GQ step starts are the reachability debt; reachable seams with
no case are the coverage debt. `--check` exits non-zero when a case names a
seam neither registry has, or an action that none of its sets admits.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
ENGINE_SRC = ROOT / "crates" / "omnigraph" / "src"
STORE_PLACES = ROOT / "crates" / "omnigraph-dst" / "src" / "store_places.rs"
CASES = ROOT / "crates" / "omnigraph-gqt" / "cases"

STATIC = re.compile(
    r'(?P<invocation>(?:\b\w+\s*::\s*)*\bdecide_seam)\s*!\s*\{'
    r'(?:\s|//[^\n]*|/\*.*?\*/)*'
    r'pub\s+static\s+\w+\s*=\s*\(\s*"(?P<name>[^"]+)"\s*,\s*(?P<op>\w+)\s*,\s*\[(?P<effects>[^\]]*)\]'
    r'(?:\s*,\s*store\s*\[(?P<store>[^\]]*)\])?',
    re.S,
)
ROW = re.compile(
    r'(?P<invocation>\brow)\s*!\s*\(\s*\w+\s*,\s*"(?P<name>[^"]+)"\s*,\s*\[[^\]]*\]\s*,'
    r'\s*honors\s*\[[^\]]*\]\s*,\s*admitted\s*\[(?P<admitted>[^\]]*)\]\s*\)',
    re.S,
)
TABLE = re.compile(r"pub static STORE_PLACES(?P<rows>.*?)\];", re.S)
EFFECT = re.compile(r"\w+")
SECTION = re.compile(r"^--- (?P<kind>\w+)[^\n]*\n(?P<body>(?:(?!^---).*\n)*)", re.M)
FIELD = re.compile(r"^\s*(\w+):\s*(.+?)\s*$", re.M)

ADMITS = {
    "fail": {"Fail", "Contention"},
    "skip": {"Skip"},
    "contention": {"Contention"},
    "misdirect": {"store:Misdirect"},
}

STORE_ACTIONS = {"misdirect", "lose", "error", "corrupt", "delay"}


def catalog() -> tuple[dict[str, tuple[str, str, tuple[str, ...]]], set[str], list[str]]:
    """name -> (declared at `file:line`, op, effects), from every static under the
    engine's src and every row of `STORE_PLACES`; a store effect or an admitted
    store action is spelled `store:<Name>`. Also the store-place names and the
    problems the two registries raise between them."""
    seams = {}
    places: set[str] = set()
    problems: list[str] = []
    for path in sorted(ENGINE_SRC.rglob("*.rs")):
        text = path.read_text()
        for m in STATIC.finditer(text):
            line = text.count("\n", 0, m.start("invocation")) + 1
            where = f"{path.relative_to(ROOT)}:{line}"
            effects = tuple(EFFECT.findall(m["effects"]))
            effects += tuple(f"store:{e}" for e in EFFECT.findall(m["store"] or ""))
            seams[m["name"]] = (where, m["op"], effects)
    text = STORE_PLACES.read_text()
    table = TABLE.search(text)
    for m in ROW.finditer(table["rows"] if table else ""):
        line = text.count("\n", 0, table.start("rows") + m.start("invocation")) + 1
        where = f"{STORE_PLACES.relative_to(ROOT)}:{line}"
        places.add(m["name"])
        if m["name"] in seams:
            problems.append(f"{m['name']} is both an engine seam and a store place")
            continue
        seams[m["name"]] = (where, "AnyWrite", tuple(f"store:{a}" for a in EFFECT.findall(m["admitted"])))
    return seams, places, problems


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


def corpus() -> list[tuple[str, str, str, str, str]]:
    """One row per `--- seam` directive: case, `at`, `action`, `subject`, and the
    group of consecutive directives that precede one step."""
    rows = []
    for path in sorted(CASES.glob("*.gqt")):
        text = path.read_text()
        if not text.endswith("\n"):
            text += "\n"
        group = 0
        run = False
        for m in SECTION.finditer(text):
            if m["kind"] != "seam":
                run = False
                continue
            if not run:
                group += 1
                run = True
            fields = fields_of(m["body"])
            rows.append((
                path.name,
                fields.get("at", ""),
                fields.get("action", ""),
                fields.get("subject", ""),
                f"{path.name}:{group}",
            ))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on an unknown seam or a mismatched action")
    args = parser.parse_args()

    seams, places, problems = catalog()
    uses = corpus()
    by_seam: dict[str, list[str]] = {name: [] for name in seams}
    store_actors: dict[str, int] = {}
    for case, at, action, subject, group in uses:
        if at in places or action in STORE_ACTIONS:
            store_actors[group] = store_actors.get(group, 0) + 1
            if store_actors[group] == 2:
                problems.append(f"{case}: two store actions before one step")
        if at in places and not subject:
            problems.append(f"{case}: names store place {at} without subject")
        if at in seams and at not in places and subject:
            problems.append(f"{case}: names engine seam {at} with a subject")
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
