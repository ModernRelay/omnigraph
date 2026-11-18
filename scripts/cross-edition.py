#!/usr/bin/env python3
"""Compare every GQT case the edition-2 merge base and this tree share.

The edition-2 side is the `omnigraph-gqt` runner built from the pinned merge
base with main (`EDITION_2_SOURCE_COMMIT` in `.github/workflows/ci.yml`); it
links that commit's engine, so it answers every case exactly as main did. The
current side is this tree's runner. For each case path present at both
commits, each side runs its own text of the case: the merge base's case with
the edition-2 runner, this tree's case with the current one.

Both texts get one rewrite before they run: their `--- runner` section is
replaced by a single deterministic-simulation environment (in-memory store,
seed 0), whose diagnostic trace (`--trace`) records every step's result as
evidence: the Arrow schema and rows of a read (sorted when the step's rows are
unordered), a write's affected counts, and every error. Nothing else of the
case changes, and both sides get the same rewrite, so a difference is the
engines', not the environment's.

Steps pair by ordinal (and loop binding and session). For each pair the script
compares the column names, the Arrow column types, the rows (as an unordered
multiset unless both steps' rows are ordered), the affected counts, and the
error: whether the step failed, and its diagnostic code (`T7`, `Q018`, ...)
when either side prints one. A difference passes only when
`crates/omnigraph-gqt/cross-edition.tsv` lists it, keyed by case, step and
aspect, naming the bullet of the GQ 3 RFC's Compatibility and reversibility
section that sanctions it (`BULLETS` below) and why. A listed row that matches
no difference fails too, so the table never outlives what it excuses.

    cross-edition.py run --edition-2-runner BIN --current-runner BIN \\
        --base-ref SHA --work DIR [--jobs N] [--case PATH ...]
    cross-edition.py --self-test
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CASES_DIR = "crates/omnigraph-gqt/cases"
TABLE = ROOT / "crates/omnigraph-gqt/cross-edition.tsv"
ASPECTS = ("columns", "types", "rows", "affected", "error", "steps", "requests")

# The bullets of the RFC's "Compatibility and reversibility" section a table
# row may cite, each with the sentence that sanctions a difference. A row
# names one key; the key is the citation, the sentence what a reviewer checks.
BULLETS = {
    "language.rrf-identical-arms": "Language: an `rrf` whose arms are identical becomes a type error with no rewrite; rewritten by hand.",
    "language.rrf-two-bindings": "Language: an `rrf` whose arms rank two bindings becomes a type error with no rewrite; rewritten by hand.",
    "language.grouped-order-meta-field": "Language: an order key over grouped rows that is a meta-field neither returned nor a key is refused.",
    "language.accepted-block-compared": "Language: a block compared with a column (`T40`) is accepted.",
    "language.accepted-unbound-triple": "Language: a triple with neither end bound (`T5`) is accepted.",
    "language.accepted-order-key-not-returned": "Language: an order key that is not a return item (`T42`) is accepted.",
    "language.accepted-item-beside-aggregates": "Language: a Boolean or property item beside aggregates (`T9`, `T58`) is accepted.",
    "language.accepted-lexical-under-not-or": "Language: a lexical predicate under `not` or `or` (`T38`) is accepted.",
    "language.diagnostic": "Language: no edition-2 text compiles after the break; it fails with its diagnostic, whose printed fix an agent applies.",
    "results.index-state": "Results: rows change where edition 2's answer depended on index state.",
    "results.letter-case": "Results: rows change where edition 2's answer depended on letter case.",
    "results.full-text-analysis": "Results: rows change on every full-text field where edition 2's analysis differs from its profile (`english_v1` by default: the token filter, the stop list's stems and NFC).",
    "results.bm25-f64": "Results: `bm25` scores are `bm25_v1`'s `F64` on every field, where edition 2 gave Lance's `F32`.",
    "results.full-text-index-required": "Results: a read that edition 2 refused with `FullTextIndexRequired` returns rows.",
    "results.keyed-load-keeps-omitted": "Results: a keyed `insert` or `merge` load that omits a property keeps its stored value where edition 2 erased it.",
    "results.fusion-window": "Results: fusion rows change where an arm's window differs from today's `limit`-sized one, and a fusion result loses the fan-out rows and population columns today's carries.",
    "results.limit-tie-rule": "Results: a read with `limit` and no `order` returns the first rows under the tie rule rather than the first rows a scan yields.",
    "results.integer-float-comparison": "Results: an integer property compared with a whole-number float literal compares as `F64`.",
    "results.distance-rename": "Language (respelling map): an unaliased `nearest` column `d._distance` becomes `_distance`, the one rename the fixed map carries.",
}

# Why a step's store requests may rise (aspect `requests`, under
# `--measure`): a rise is a difference like any other and passes only when a
# row names one of these.
COSTS = {
    "cost.exact-lexical-scan": "RFC section 4: the exact lexical scan answers every `match_terms` and `bm25`, and no query reads a full-text index, so a lexical read reads its text column's data files.",
    "cost.accepted-refusal": "Compatibility, Language: a text edition 2 refused is accepted, so the step runs a read where edition 2 refused before reading.",
}

# Differences this branch makes that no Compatibility bullet names. A row may
# cite one of these keys so that the job stays a check of everything else,
# but it is not a sanction: the job prints every such row as a warning, and
# the key leaves this list when the RFC's Compatibility section decides it
# (adds a bullet, which a row then cites) or the branch reverts the change.
OPEN = {
    "open:hop-range-self-loop": "A hop range other than the single hop no longer returns its start node through a self-loop (the RFC's core operator table, `Expand`, marks it *[choice]*); the Results bullet does not list it.",
    "open:avg-exact-integer": "`avg` over integers divides an exact total once, where edition 2 accumulated in F64 and lost precision; the Results bullet does not list it.",
    "open:engine-setting-removed": "The session setting `engine` (released in 0.12) is removed; no RFC section names it.",
    "open:bm25-score-rename": "An unaliased `bm25` column `d._score` becomes `_score`; the Language bullet's respelling map names `_distance` as the one rename.",
    "open:mutation-where-lexical": "A lexical predicate in a write's `where`, which edition 2 refused (`T44`), is accepted; the Language bullet's list of nine accepted refusals does not include it.",
    "open:index-pages-read-twice": "A filter's scalar index pages (`page_data`, `page_lookup`) are read by scan-access planning and again by execution, where edition 2 read them once.",
    "open:block-reads-table-again": "A correlated block reads its table's data file once more than the edition-2 read did.",
    "open:catalogue-operators": "The `@Operator` and logical-kind catalogue rows follow the new core operators (`RankFuse` and `TextSearch` leave, `Correlate`, `Fuse`, `Let`, `OptionalJoin`, `Score`, `TargetCut` and `Union` arrive); no Compatibility bullet names the catalogue.",
}

DIAGNOSTIC_CODE = re.compile(r"\b([QTPL]\d{1,3})\b")


# --------------------------------------------------------------------------
# Case text
# --------------------------------------------------------------------------

RUNNER_BODY = (
    "timeout_ms: {timeout}\n"
    "environments:\n"
    "  - target: omnigraph-engine-dst\n"
    "    storage: in-memory-object-store\n"
    "    seeds: [0]\n"
)


def simulated(text: str, timeout_ms: int = 60000) -> str:
    """The case with its runner section replaced by one DST environment."""
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    replaced = False
    while i < len(lines):
        if lines[i].rstrip("\r\n") == "--- runner" and not replaced:
            out.append(lines[i])
            i += 1
            while i < len(lines) and not lines[i].startswith("--- "):
                i += 1
            out.append(RUNNER_BODY.format(timeout=timeout_ms))
            replaced = True
            continue
        out.append(lines[i])
        i += 1
    if not replaced:
        raise ValueError("the case has no `--- runner` section")
    return "".join(out)


# --------------------------------------------------------------------------
# Observations
# --------------------------------------------------------------------------


@dataclass
class Observation:
    """What one step answered: a read's columns, types and rows, a write's
    affected counts, or the error it failed with."""

    columns: list[str] | None = None
    types: list[str] | None = None
    rows: list | None = None
    ordered: bool = False
    affected: tuple[int, int] | None = None
    error: str | None = None
    extra: list = field(default_factory=list)

    def code(self) -> str | None:
        if self.error is None:
            return None
        found = DIAGNOSTIC_CODE.search(self.error)
        return found.group(1) if found else None

    def to_json(self) -> dict:
        return {
            "columns": self.columns,
            "types": self.types,
            "rows": self.rows,
            "ordered": self.ordered,
            "affected": list(self.affected) if self.affected else None,
            "error": self.error,
        }


def step_key(row: dict) -> tuple:
    binding = row.get("loop_binding")
    return (
        int(row.get("step") or 0),
        json.dumps(binding, sort_keys=True) if binding is not None else "",
        str(row.get("session") or ""),
    )


ERROR_OBSERVATION = re.compile(r"actual (?:[a-z ]+ )?error: (.*)", re.S)


OPERATION = re.compile(r"operation: ordinal=\d+ .*? expected=(\w+)(.*)", re.S)
DECLARATION_NAME = re.compile(r'\bname: "([^"]*)"')
ORDINAL_FIELD = re.compile(r"ordinal: \d+,? ?")


def signature(text: str) -> str:
    """What identifies a step across the two texts of a case: its kind and its
    declared query name, or for a step without one its parsed statement."""
    found = OPERATION.match(text)
    if not found:
        return text
    kind, rest = found.groups()
    if kind in ("Query", "Mutate"):
        name = DECLARATION_NAME.search(rest)
        return f"{kind}:{name.group(1) if name else ''}"
    return f"{kind}:{ORDINAL_FIELD.sub('', rest)[:160]}"


def observations(trace: Path) -> tuple[dict[tuple, Observation], list[tuple[tuple, str]]]:
    """Every step's observation from one attempt's diagnostic trace, and the
    steps in the order they ran, each with its signature."""
    steps: dict[tuple, Observation] = {}
    order: list[tuple[tuple, str]] = []
    with trace.open() as handle:
        for line in handle:
            row = json.loads(line)
            kind = row.get("kind")
            if kind == "observation" and str(row.get("text", "")).startswith("operation: "):
                key = step_key(row)[:2]
                if not order or order[-1][0] != key:
                    order.append((key, signature(row["text"])))
            if kind == "evidence":
                record = row.get("record")
                value = row.get("value") or {}
                obs = steps.setdefault(step_key(row), Observation())
                if record == "query_result":
                    obs.columns = [f["name"] for f in value.get("schema", [])]
                    obs.types = [
                        f"{f['type']}{'' if f.get('nullable', True) else ' not null'}"
                        for f in value.get("schema", [])
                    ]
                    rows = value.get("rows")
                    if isinstance(rows, dict) and "Ok" in rows:
                        obs.rows = rows["Ok"]
                    elif isinstance(rows, dict) and "Err" in rows:
                        obs.error = f"rows unreadable: {rows['Err']}"
                    obs.ordered = bool(value.get("ordered"))
                elif record == "query_error":
                    obs.error = value.get("message", "")
                elif record == "parameter_error":
                    obs.error = value.get("message", "")
                elif record == "mutation_result":
                    if "error" in value:
                        obs.error = value["error"]
                    else:
                        obs.affected = (value.get("nodes", 0), value.get("edges", 0))
            elif kind == "observation" and row.get("step"):
                text = row.get("text", "")
                found = ERROR_OBSERVATION.match(text)
                if found:
                    obs = steps.setdefault(step_key(row), Observation())
                    if obs.error is None:
                        obs.error = found.group(1)
    return steps, order


def align(old: list[tuple[tuple, str]], new: list[tuple[tuple, str]]) -> dict[tuple, tuple]:
    """Current step -> edition-2 step: equal signatures in sequence order, and
    within a replaced run of equal length, position for position."""
    import difflib

    matcher = difflib.SequenceMatcher(
        a=[sig for _, sig in old], b=[sig for _, sig in new], autojunk=False
    )
    pairs: dict[tuple, tuple] = {}
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal" or (tag == "replace" and i2 - i1 == j2 - j1):
            for k in range(i2 - i1):
                pairs[new[j1 + k][0]] = old[i1 + k][0]
    return pairs


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def differences(old: Observation, new: Observation) -> list[tuple[str, str]]:
    """The aspects in which two observations of one step differ, each with a
    one-line description."""
    found: list[tuple[str, str]] = []
    if (old.error is None) != (new.error is None):
        found.append(
            (
                "error",
                f"edition 2 {'failed: ' + short(old.error) if old.error else 'succeeded'}; "
                f"current {'failed: ' + short(new.error) if new.error else 'succeeded'}",
            )
        )
        return found
    if old.error is not None:
        if old.code() != new.code():
            found.append(
                ("error", f"code {old.code() or '(none)'} became {new.code() or '(none)'}")
            )
        return found
    if old.columns is not None or new.columns is not None:
        if old.columns != new.columns:
            found.append(("columns", f"{old.columns} became {new.columns}"))
        elif old.types != new.types:
            found.append(("types", f"{old.types} became {new.types}"))
        old_rows = old.rows or []
        new_rows = new.rows or []
        if old.ordered and new.ordered:
            same = [canonical(r) for r in old_rows] == [canonical(r) for r in new_rows]
        else:
            same = Counter(canonical(r) for r in old_rows) == Counter(
                canonical(r) for r in new_rows
            )
        if not same:
            found.append(
                ("rows", f"{len(old_rows)} rows became {len(new_rows)}, contents differ")
            )
    if old.affected != new.affected:
        found.append(("affected", f"{old.affected} became {new.affected}"))
    return found


def short(text: str | None, limit: int = 160) -> str:
    if text is None:
        return ""
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


# --------------------------------------------------------------------------
# The sanctioned table
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Sanction:
    case: str
    step: str
    aspect: str
    bullet: str
    why: str
    line: int


def read_table(path: Path) -> list[Sanction]:
    """Rows of `case<TAB>step<TAB>aspect<TAB>bullet<TAB>why`; `#` comments and
    blank lines are ignored; `step` is an ordinal or `*` for every step."""
    rows: list[Sanction] = []
    errors: list[str] = []
    for number, raw in enumerate(path.read_text().splitlines(), start=1):
        if not raw.strip() or raw.startswith("#"):
            continue
        parts = raw.split("\t")
        if len(parts) != 5:
            errors.append(f"{path.name}:{number}: expected 5 tab-separated fields, found {len(parts)}")
            continue
        case, step, aspect, bullet, why = (p.strip() for p in parts)
        if not (step == "*" or step.isdigit() or (step.startswith("e2:") and step[3:].isdigit())):
            errors.append(
                f"{path.name}:{number}: step `{step}` is not a current ordinal, `e2:<ordinal>` or `*`"
            )
        if aspect not in ASPECTS:
            errors.append(f"{path.name}:{number}: aspect `{aspect}` is not one of {', '.join(ASPECTS)}")
        if aspect == "requests":
            if bullet not in COSTS and bullet not in OPEN:
                errors.append(f"{path.name}:{number}: `{bullet}` names no cost reason")
        elif bullet not in BULLETS and bullet not in OPEN:
            errors.append(f"{path.name}:{number}: bullet `{bullet}` names no Compatibility bullet")
        if len(why) < 20:
            errors.append(f"{path.name}:{number}: the reason is missing or too short to review")
        rows.append(Sanction(case, step, aspect, bullet, why, number))
    keys = Counter((r.case, r.step, r.aspect) for r in rows)
    for key, count in keys.items():
        if count > 1:
            errors.append(f"{path.name}: {key} is listed {count} times")
    if errors:
        raise SystemExit("\n".join(errors))
    return rows


def sanctioned(rows: list[Sanction], case: str, step: str, aspect: str) -> Sanction | None:
    for row in rows:
        if row.case == case and row.aspect == aspect and row.step in (step, "*"):
            return row
    return None


# --------------------------------------------------------------------------
# Running
# --------------------------------------------------------------------------


def paired_cases(base_ref: str) -> list[tuple[str, str]]:
    """(merge-base path, current path) of every case present at both commits:
    the same path, or a path git's rename detection pairs (the corpus moved
    `cases/v2/` to `cases/` in the break)."""

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(ROOT), *args], check=True, capture_output=True, text=True
        ).stdout

    base = {
        line for line in git("ls-tree", "-r", "--name-only", base_ref, "--", CASES_DIR).splitlines()
        if line.endswith(".gqt")
    }
    current = {str(p.relative_to(ROOT)) for p in (ROOT / CASES_DIR).rglob("*.gqt")}
    pairs = {path: path for path in base & current}
    for line in git("diff", "-M", "--name-status", base_ref, "--", CASES_DIR).splitlines():
        parts = line.split("\t")
        if parts[0].startswith("R") and len(parts) == 3 and parts[2] in current:
            pairs[parts[1]] = parts[2]
    return sorted(pairs.items(), key=lambda pair: pair[1])


def base_text(base_ref: str, path: str) -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), "show", f"{base_ref}:{path}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


@dataclass
class SideResult:
    status: str
    steps: dict[tuple, Observation]
    order: list
    message: str = ""
    failure: tuple | None = None
    costs: dict = field(default_factory=dict)


def run_side(
    runner: Path,
    text: str,
    work: Path,
    relative: str,
    timeout_s: int,
    reuse: bool = False,
    measure: bool = False,
) -> SideResult:
    case_path = work / "cases" / relative
    artifacts = work / "artifacts" / relative
    status_file = artifacts / "status.json"
    if reuse and status_file.exists():
        saved = json.loads(status_file.read_text())
        return read_side(artifacts, saved["status"], saved["message"])
    case_path.parent.mkdir(parents=True, exist_ok=True)
    case_path.write_text(simulated(text))
    if artifacts.exists():
        shutil.rmtree(artifacts)
    artifacts.mkdir(parents=True)
    env = dict(os.environ)
    env.setdefault("RUST_MIN_STACK", "16777216")
    env.pop("OMNIGRAPH_GQ_BLESS", None)
    try:
        proc = subprocess.run(
            [str(runner), str(case_path), "--trace", "--artifacts", str(artifacts)]
            + (["--measure"] if measure else []),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
            cwd=str(work),
        )
        output = proc.stdout + proc.stderr
        status = "passed" if proc.returncode == 0 else "failed"
    except subprocess.TimeoutExpired:
        output = f"timed out after {timeout_s} s"
        status = "timeout"
    # The output opens with trace paths; its end holds the verdict.
    tail = " ".join(output.split())[-900:]
    status_file.write_text(json.dumps({"status": status, "message": tail}))
    return read_side(artifacts, status, tail)


def read_side(artifacts: Path, status: str, message: str) -> SideResult:
    traces = sorted((artifacts / "trace").glob("*.jsonl")) if (artifacts / "trace").exists() else []
    first = None
    for trace in traces:
        with trace.open() as handle:
            start = json.loads(handle.readline())
        if start.get("replay") == 0:
            first = trace
            break
    steps, order = observations(first) if first else ({}, [])
    return SideResult(
        status,
        steps,
        order,
        message,
        failed_assertion(first) if first else None,
        step_costs(artifacts),
    )


def step_costs(artifacts: Path) -> dict[int, dict[str, int]]:
    """Each step's store metrics under `--measure` (seed 0): `requests` and
    one count per `<realm>_<kind>.<verb>` class."""
    costs: dict[int, dict[str, int]] = {}
    cost_dir = artifacts / "cost"
    if not cost_dir.exists():
        return costs
    for tsv in sorted(cost_dir.glob("*.tsv")):
        with tsv.open() as handle:
            header = handle.readline().rstrip("\n").split("\t")
            for line in handle:
                row = dict(zip(header, line.rstrip("\n").split("\t")))
                if row.get("slot") != "step" or row.get("seed") != "0":
                    continue
                metric = row.get("metric", "")
                if metric in ("makespan", "span", "simulated_us", "usd_micro", "bytes_read", "bytes_written"):
                    continue
                try:
                    value = int(float(row.get("value", "0")))
                except ValueError:
                    continue
                step = int(row.get("step", "0"))
                costs.setdefault(step, {})[metric] = value
        break
    return costs


def failed_assertion(trace: Path) -> tuple | None:
    """The (signature, message) of the step whose assertion failed, if any."""
    names: dict = {}
    with trace.open() as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("kind") == "observation" and str(row.get("text", "")).startswith("operation: "):
                names[step_key(row)[:2]] = signature(row["text"])
            if row.get("kind") == "evidence" and row.get("record") == "assertion":
                value = row.get("value") or {}
                if value.get("status") == "failed":
                    message = re.sub(r"\b[0-9A-HJKMNP-TV-Z]{26}\b", "<ulid>", value.get("message", ""))
                    return (names.get(step_key(row)[:2], ""), message)
    return None


def run(args: argparse.Namespace) -> int:
    table = read_table(Path(args.table))
    pairs = paired_cases(args.base_ref)
    if args.case:
        wanted = set(args.case)
        pairs = [pair for pair in pairs if pair[1] in wanted or pair[0] in wanted]
    cases = pairs
    if not cases:
        print("::error::no case exists at both commits", file=sys.stderr)
        return 1
    work = Path(args.work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    edition_2_runner = Path(args.edition_2_runner).resolve()
    current_runner = Path(args.current_runner).resolve()
    for label, runner in (("edition-2", edition_2_runner), ("current", current_runner)):
        if not (runner.is_file() and os.access(runner, os.X_OK)):
            print(f"::error::the {label} GQT runner {runner} is missing or not executable", file=sys.stderr)
            return 1

    def compare_case(pair: tuple[str, str]):
        old_path, relative = pair
        old = run_side(
            edition_2_runner,
            base_text(args.base_ref, old_path),
            work / "edition-2",
            relative,
            args.timeout,
            args.reuse,
            args.measure,
        )
        new = run_side(
            current_runner,
            (ROOT / relative).read_text(),
            work / "current",
            relative,
            args.timeout,
            args.reuse,
            args.measure,
        )
        return relative, old, new

    findings: list[dict] = []
    used: set[int] = set()
    unrunnable: list[str] = []
    shared_failures: list[str] = []
    costs: list[dict] = []
    compared_steps = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for relative, old, new in pool.map(compare_case, cases):
            case = relative.removeprefix(CASES_DIR + "/")
            if old.status == new.status == "failed" and old.failure and old.failure == new.failure:
                shared_failures.append(
                    f"{case}: both editions fail step {old.failure[0]} under simulation with "
                    f"`{short(old.failure[1], 200)}`; the steps after it are not compared"
                )
            else:
                for label, side in (("edition 2", old), ("current", new)):
                    if side.status != "passed":
                        why = f"step {side.failure[0]}: {short(side.failure[1], 400)}; " if side.failure else ""
                        unrunnable.append(f"{case}: {label} {side.status}: {why}{side.message}")
            pairs = align(old.order, new.order)
            matched_old = set(pairs.values())
            for key, sig in old.order:
                if key not in matched_old:
                    findings.append(
                        {
                            "case": case,
                            "step": f"e2:{key[0]}",
                            "aspect": "steps",
                            "detail": f"edition-2 step {key[0]} ({sig}) has no current counterpart",
                        }
                    )
            names = dict(new.order)
            for key in sorted(new.steps):
                op = key[:2]
                if op not in pairs:
                    continue
                old_key = pairs[op] + key[2:]
                if (
                    args.measure
                    and key[2] == ""
                    and key[1] == ""
                    and old_key[0] in old.costs
                    and key[0] in new.costs
                ):
                    before, after = old.costs[old_key[0]], new.costs[key[0]]
                    if after.get("requests", 0) > before.get("requests", 0):
                        findings.append(
                            {
                                "case": case,
                                "step": str(key[0]),
                                "aspect": "requests",
                                "detail": f"{before.get('requests', 0)} store requests became {after.get('requests', 0)}",
                            }
                        )
                    if before.get("requests") != after.get("requests"):
                        classes = sorted(set(before) | set(after))
                        costs.append(
                            {
                                "case": case,
                                "step": key[0],
                                "signature": names.get(op, ""),
                                "edition_2": before.get("requests", 0),
                                "current": after.get("requests", 0),
                                "classes": {
                                    c: after.get(c, 0) - before.get(c, 0)
                                    for c in classes
                                    if c != "requests" and after.get(c, 0) != before.get(c, 0)
                                },
                            }
                        )
                if old_key not in old.steps:
                    continue
                compared_steps += 1
                for aspect, detail in differences(old.steps[old_key], new.steps[key]):
                    findings.append(
                        {
                            "case": case,
                            "step": str(key[0]),
                            "edition_2_step": old_key[0],
                            "signature": names.get(op, ""),
                            "aspect": aspect,
                            "detail": detail,
                            "edition_2": old.steps[old_key].to_json(),
                            "current": new.steps[key].to_json(),
                        }
                    )

    unsanctioned = []
    for finding in findings:
        row = sanctioned(table, finding["case"], finding["step"], finding["aspect"])
        if row is None:
            unsanctioned.append(finding)
        else:
            used.add(row.line)
            finding["bullet"] = row.bullet
    compared = {current.removeprefix(CASES_DIR + "/") for _, current in cases}
    stale = [
        row
        for row in table
        if row.line not in used
        and row.case in compared
        and (args.measure or row.aspect != "requests")
    ]

    report = {
        "base_ref": args.base_ref,
        "cases": len(cases),
        "steps_compared": compared_steps,
        "differences": len(findings),
        "sanctioned": len(findings) - len(unsanctioned),
        "unsanctioned": unsanctioned,
        "stale_rows": [row.__dict__ for row in stale],
        "unrunnable": unrunnable,
        "findings": findings,
        "costs": costs,
    }
    (work / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True))
    by_bullet = Counter(f.get("bullet") for f in findings if f.get("bullet"))
    report["by_bullet"] = dict(by_bullet)
    (work / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True))
    print(
        f"cross-edition: {len(cases)} cases, {compared_steps} steps compared, "
        f"{len(findings)} differences, {len(findings) - len(unsanctioned)} sanctioned"
    )
    for bullet, count in sorted(by_bullet.items()):
        print(f"  {count:4} {bullet}")
    failed = False
    for item in unrunnable:
        failed = True
        print(f"::error::not comparable: {item}")
    for item in shared_failures:
        print(f"::warning::{item}")
    for finding in findings:
        if finding.get("bullet", "").startswith("open:"):
            print(
                f"::warning::not sanctioned by the RFC ({finding['bullet']}): {finding['case']} "
                f"step {finding['step']} {finding['aspect']}: {finding['detail']}"
            )
    for finding in unsanctioned:
        failed = True
        print(
            f"::error::unsanctioned difference: {finding['case']} step {finding['step']} "
            f"{finding['aspect']}: {finding['detail']}"
        )
    for row in stale:
        failed = True
        print(
            f"::error::{TABLE.name}:{row.line}: {row.case} step {row.step} {row.aspect} "
            "sanctions no difference; remove the row"
        )
    print(f"report: {work / 'report.json'}")
    return 1 if failed else 0


# --------------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------------


class SelfTest(unittest.TestCase):
    CASE = (
        "# issue: none\n--- runner\ntimeout_ms: 1000\nenvironments:\n"
        "  - target: omnigraph-engine\n    storage: local-filesystem\n--- schema\nnode A {}\n"
    )

    def test_runner_section_becomes_one_simulated_environment(self):
        text = simulated(self.CASE)
        self.assertIn("omnigraph-engine-dst", text)
        self.assertNotIn("local-filesystem", text)
        self.assertTrue(text.endswith("--- schema\nnode A {}\n"))

    def test_a_case_without_a_runner_section_is_refused(self):
        with self.assertRaises(ValueError):
            simulated("--- schema\n")

    def test_steps_align_by_kind_and_name_across_an_inserted_step(self):
        old = [((1, ""), "Query:a"), ((2, ""), "Query:b"), ((3, ""), "Mutate:c")]
        new = [((1, ""), "Query:a"), ((2, ""), "Query:new"), ((3, ""), "Query:b"), ((4, ""), "Mutate:c")]
        self.assertEqual(align(old, new), {(1, ""): (1, ""), (3, ""): (2, ""), (4, ""): (3, "")})
        renamed = [((1, ""), "Query:a"), ((2, ""), "Query:b2"), ((3, ""), "Mutate:c")]
        self.assertEqual(align(old, renamed)[(2, "")], (2, ""))

    def test_signature_names_the_declaration(self):
        text = 'operation: ordinal=4 line=Some(9) binding=None generation=0 expected=Query(QueryStep { ordinal: 4, source: "q", name: "rows" })'
        self.assertEqual(signature(text), "Query:rows")

    def test_unordered_rows_compare_as_multisets(self):
        a = Observation(columns=["x"], types=["Int64"], rows=[{"x": 1}, {"x": 2}])
        b = Observation(columns=["x"], types=["Int64"], rows=[{"x": 2}, {"x": 1}])
        self.assertEqual(differences(a, b), [])
        b.rows = [{"x": 2}, {"x": 2}]
        self.assertEqual([d[0] for d in differences(a, b)], ["rows"])

    def test_ordered_rows_compare_in_order_only_when_both_are_ordered(self):
        a = Observation(columns=["x"], types=["Int64"], rows=[{"x": 1}, {"x": 2}], ordered=True)
        b = Observation(columns=["x"], types=["Int64"], rows=[{"x": 2}, {"x": 1}], ordered=True)
        self.assertEqual([d[0] for d in differences(a, b)], ["rows"])
        b.ordered = False
        self.assertEqual(differences(a, b), [])

    def test_types_and_columns_are_aspects(self):
        a = Observation(columns=["x"], types=["Float32"], rows=[])
        b = Observation(columns=["x"], types=["Float64"], rows=[])
        self.assertEqual([d[0] for d in differences(a, b)], ["types"])
        b.columns = ["y"]
        self.assertEqual([d[0] for d in differences(a, b)], ["columns"])

    def test_errors_compare_by_failure_and_code(self):
        a = Observation(error="type error: T7: one")
        b = Observation(error="type error: T7: reworded")
        self.assertEqual(differences(a, b), [])
        b.error = "type error: T65: other"
        self.assertEqual([d[0] for d in differences(a, b)], ["error"])
        self.assertEqual(
            [d[0] for d in differences(a, Observation(rows=[], columns=[]))], ["error"]
        )

    def test_affected_counts_are_an_aspect(self):
        self.assertEqual(
            [d[0] for d in differences(Observation(affected=(1, 0)), Observation(affected=(2, 0)))],
            ["affected"],
        )

    def test_table_refuses_unknown_bullets_aspects_and_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.tsv"
            path.write_text("a.gqt\t1\trows\tno.such.bullet\tbecause it is a long enough reason\n")
            with self.assertRaises(SystemExit):
                read_table(path)
            path.write_text("a.gqt\t1\tshape\tresults.letter-case\tbecause it is a long enough reason\n")
            with self.assertRaises(SystemExit):
                read_table(path)
            row = "a.gqt\t1\trows\tresults.letter-case\tbecause it is a long enough reason\n"
            path.write_text(row + row)
            with self.assertRaises(SystemExit):
                read_table(path)
            path.write_text("# comment\n\n" + row)
            rows = read_table(path)
            self.assertEqual(len(rows), 1)
            self.assertIsNotNone(sanctioned(rows, "a.gqt", "1", "rows"))
            self.assertIsNone(sanctioned(rows, "a.gqt", "2", "rows"))

    def test_observations_read_a_trace(self):
        rows = [
            {"kind": "evidence", "record": "query_result", "step": 1, "value": {
                "schema": [{"name": "x", "type": "Int64", "nullable": False}],
                "rows": {"Ok": [{"x": 1}]}, "ordered": True}},
            {"kind": "evidence", "record": "mutation_result", "step": 2, "value": {"nodes": 3, "edges": 1}},
            {"kind": "evidence", "record": "query_error", "step": 3, "value": {"message": "type error: T82: x"}},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.jsonl"
            path.write_text("".join(json.dumps(r) + "\n" for r in rows))
            seen, _ = observations(path)
        self.assertEqual(seen[(1, "", "")].types, ["Int64 not null"])
        self.assertEqual(seen[(2, "", "")].affected, (3, 1))
        self.assertEqual(seen[(3, "", "")].code(), "T82")

    def test_checked_in_table_reads(self):
        read_table(TABLE)

    def test_every_workflow_pins_one_edition_2_commit(self):
        pins = {}
        for workflow in sorted((ROOT / ".github/workflows").glob("*.yml")):
            for found in re.findall(r"EDITION_2_SOURCE_COMMIT: ([0-9a-f]{40})", workflow.read_text()):
                pins.setdefault(found, []).append(workflow.name)
        self.assertEqual(len(pins), 1, pins)
        self.assertIn("ci.yml", next(iter(pins.values())))


def main() -> int:
    if "--self-test" in sys.argv[1:]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--edition-2-runner", required=True)
    r.add_argument("--current-runner", required=True)
    r.add_argument("--base-ref", required=True)
    r.add_argument("--work", required=True)
    r.add_argument("--table", default=str(TABLE))
    r.add_argument("--jobs", type=int, default=os.cpu_count() or 2)
    r.add_argument("--timeout", type=int, default=600)
    r.add_argument(
        "--measure",
        action="store_true",
        help="also compare each aligned step's store requests under the simulation; a rise is a difference",
    )
    r.add_argument(
        "--reuse",
        action="store_true",
        help="compare the traces an earlier run left under --work instead of running again",
    )
    r.add_argument("--case", action="append", help="a case path under the repository root")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
