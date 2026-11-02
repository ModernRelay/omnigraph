#!/usr/bin/env python3
"""Markdown tables for the branch-ops report from two measurement directories.

usage: report_tables.py <baseline-dir> <final-dir>
Each directory holds <case>/invocation-*.json from run.sh.
"""

import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from analyze import TAIL, steps  # noqa: E402

DEPTHS = [("d0000", 0), ("i0032", 32), ("i0256", 256), ("i1024", 1024), ("i2048", 2048)]
OPS = [t for t in TAIL if "restart" not in t]


def load(directory, case):
    files = sorted(glob.glob(f"{directory}/{case}/invocation-*.json"), key=os.path.getmtime)
    if not files:
        return None
    st = steps(json.load(open(files[-1])))
    last = max(st)
    first = last - len(TAIL) + 1
    tail = {name: st[first + i] for i, name in enumerate(TAIL)}
    history = {s - 1: st[s] for s in st if 1 < s < first}
    return tail, history


def curve(directory, title):
    data = {case: load(directory, case) for case, _ in DEPTHS}
    head = "| operation | " + " | ".join(f"{d} commits" for _, d in DEPTHS) + " |"
    print(f"**{title}** (total requests per operation; one-row commits on `Person` before the tail)\n")
    print(head)
    print("|---|" + "---:|" * len(DEPTHS))
    first = data["i0032"][1] if data.get("i0032") else None
    row = ["one-row insert (history commit at that depth)"]
    for case, depth in DEPTHS:
        hist = data[case][1] if data[case] else {}
        row.append(str(hist[depth][0]) if depth in hist else "-")
    print("| " + " | ".join(row) + " |")
    for op in OPS:
        row = [op[4:]]
        for case, _ in DEPTHS:
            row.append(str(data[case][0][op][0]) if data[case] else "-")
        print("| " + " | ".join(row) + " |")
    print()


def realms(directory, case, title):
    data = load(directory, case)
    print(f"**{title}** (`{case}`: requests by realm, with verbs g=get h=head l=list p=put d=delete)\n")
    print("| operation | total | manifest | history | table | control |")
    print("|---|---:|---:|---:|---:|---:|")
    for op in OPS:
        total, by, verbs, _ = data[0][op]
        cells = []
        for r in ("manifest", "history", "table", "control"):
            c = by.get(r, 0)
            vs = " ".join(f"{k}{n}" for k, n in sorted(verbs[r].items())) if c else ""
            cells.append(f"{c} ({vs})" if c else "0")
        print(f"| {op[4:]} | {total} | " + " | ".join(cells) + " |")
    print()


def invariance(directory, title):
    cases = [("d0032_dead0256", "256 dead branches"), ("d0032_live0256", "256 live branches"),
             ("i0256_tables32", "32 tables, 256 commits"), ("d2048", "2048 commits updating one row")]
    data = {c: load(directory, c) for c, _ in cases}
    print(f"**{title}**\n")
    print("| operation | " + " | ".join(label for _, label in cases) + " |")
    print("|---|" + "---:|" * len(cases))
    for op in OPS:
        row = [op[4:]] + [str(data[c][0][op][0]) if data[c] else "-" for c, _ in cases]
        print("| " + " | ".join(row) + " |")
    print()


def main():
    base, final = sys.argv[1], sys.argv[2]
    curve(base, "Baseline growth curve")
    curve(final, "Final growth curve")
    realms(base, "i0256", "Baseline realm split at 256 commits")
    realms(final, "i0256", "Final realm split at 256 commits")
    invariance(base, "Baseline: other growth dimensions")
    invariance(final, "Final: other growth dimensions")


if __name__ == "__main__":
    main()
