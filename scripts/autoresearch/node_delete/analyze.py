#!/usr/bin/env python3
"""Tabulate node-delete GQT --measure reports into per-step realm counts.

Realms come from each request's path and dataset:
  manifest = __manifest and its branch trees/refs; history = __history;
  control = the StorageAdapter realm; tables are split by the table the
  dataset id names (table incarnation ids are assigned at init in schema
  order: node types sorted by name, then edge types sorted by name):
  person = node:Person, inc = edge tables incident to Person,
  other = every other table.

usage: analyze.py <artifacts-dir-or-report.json> [--verbs] [--json out]
Prints one row per tail step (gen_cases.TAIL_NAMES).
"""
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from gen_cases import F_TAIL_NAMES, TAIL_NAMES  # noqa: E402


def table_names(case_text):
    nodes, edges = [], []
    for line in case_text.split("--- seed")[0].splitlines():
        m = re.match(r"node (\w+)", line)
        if m:
            nodes.append(m.group(1))
        m = re.match(r"edge (\w+): (\w+) -> (\w+)", line)
        if m:
            edges.append((m.group(1), m.group(2), m.group(3)))
    order = sorted(nodes) + [e[0] for e in sorted(edges)]
    incident = {e[0] for e in edges if "Person" in (e[1], e[2])}
    out = {}
    for i, name in enumerate(order, start=1):
        if name == "Person":
            out[i] = "person"
        elif name in incident:
            out[i] = "inc"
        else:
            out[i] = "other"
    return out


def realm(req, tables):
    if req["dataset"] == "control" or req["class"].startswith("control"):
        return "control"
    path = req["path"]
    if "__manifest" in path or "/tree/" in path or req["dataset"] == "tree":
        return "manifest"
    if "__history" in path:
        return "history"
    m = re.match(r"^([0-9a-f]{16})-", req["dataset"])
    if m:
        return tables.get(int(m.group(1), 16), "other")
    if req["dataset"].startswith(".tmp"):
        return "source"
    return "manifest"  # branch tree datasets (b.<ulid>) belong to __manifest


def verb(v):
    v = v.replace("_failed", "")
    return {"get": "g", "head": "h", "list": "l", "put": "p", "delete": "d"}.get(v, v)


def load(path):
    if os.path.isdir(path):
        path = glob.glob(os.path.join(path, "invocation-*.json"))[0]
    return json.load(open(path))


REALMS = ("manifest", "history", "person", "inc", "other", "control")


def tail_rows(report, tables, trailing=0, names=None):
    names = names or TAIL_NAMES
    m = report["attempts"][0]["outcome"]["Ok"]["measurements"]
    steps = [e for e in m if e["slot"] == "step"]
    if trailing:
        steps = steps[:-trailing]
    steps = steps[-len(names):]
    rows = {}
    for name, e in zip(names, steps):
        log = e["value"]["log"]
        by, verbs = Counter(), defaultdict(Counter)
        for r in log:
            k = realm(r, tables)
            by[k] += 1
            verbs[k][verb(r["verb"])] += 1
        rows[name] = dict(total=len(log), bytes=e["value"]["bytes_read"],
                          **{k: by.get(k, 0) for k in REALMS},
                          verbs={k: dict(v) for k, v in verbs.items()})
    return rows


def main():
    args = sys.argv[1:]
    src = args[0]
    case_name = os.path.basename(src.rstrip("/"))
    cases = os.environ.get("CASES", "/src/ndwork/cases")
    text = open(os.path.join(cases, case_name + ".gqt")).read()
    tables = table_names(text)
    # Indexed cases end with one search probe after the tail.
    if case_name[0] in "fg":
        rows = tail_rows(load(src), tables, 0, F_TAIL_NAMES)
    else:
        rows = tail_rows(load(src), tables, 1 if "search($p.text" in text else 0)
    print("| step | total | manifest | history | Person | incident edges | other tables | control | bytes read |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for name, r in rows.items():
        if "restart" in name:
            continue
        cells = [str(r["total"])]
        for k in REALMS:
            c = r[k]
            if "--verbs" in args and c:
                c = f"{c} (" + " ".join(f"{a}{b}" for a, b in sorted(r["verbs"][k].items())) + ")"
            cells.append(str(c))
        cells.append(str(r["bytes"]))
        print(f"| {name} | " + " | ".join(cells) + " |")
    if "--json" in args:
        json.dump(rows, open(args[args.index("--json") + 1], "w"), indent=1)


if __name__ == "__main__":
    main()
