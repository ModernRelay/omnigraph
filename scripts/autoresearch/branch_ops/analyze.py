#!/usr/bin/env python3
"""Tabulate a GQT --measure report into per-step realm counts.

Realms come from each request's path in the report's request log:
  manifest = __manifest (including __manifest/tree/<branch> lineage and refs)
  history  = __history
  table    = node/edge table datasets
  control  = the StorageAdapter realm
Each cell is `requests`; verbs are summarized as g(get)/h(head)/l(list)/p(put)/d(delete),
`_failed` folded in.

usage: analyze.py <report.json> [--tail N] [--sample 1,32,256,2048] [--verbs] [--json out]
"""

import json
import sys
from collections import Counter, defaultdict

TAIL = [
    "T01 mutate main warm", "T02 branch create warm", "T03 mutate branch warm",
    "T04 load append main warm", "T05 load merge branch warm", "T06 load merge main warm",
    "T07 load append branch warm", "T08 branch merge threeway warm", "T09 branch delete warm",
    "T10 branch create ff", "T11 mutate branch ff", "T12 branch merge fastforward warm",
    "T13 branch delete ff", "T14 restart", "T15 mutate main cold", "T16 restart",
    "T17 branch create cold", "T18 mutate branch c", "T19 restart", "T20 branch merge cold",
    "T21 restart", "T22 branch delete cold", "T23 restart", "T24 load append main cold",
]


def realm(req):
    if req["dataset"] == "control" or req["class"].startswith("control"):
        return "control"
    path = req["path"]
    if "/__manifest" in path or path.startswith("__manifest"):
        return "manifest"
    if "/__history" in path or path.startswith("__history"):
        return "history"
    return "table"


def verb(v):
    v = v.replace("_failed", "")
    return {"get": "g", "head": "h", "list": "l", "put": "p", "delete": "d", "copy": "c"}.get(v, v)


def steps(report):
    m = report["attempts"][0]["outcome"]["Ok"]["measurements"]
    out = {}
    for e in m:
        if e["slot"] != "step":
            continue
        log = e["value"]["log"]
        by = Counter()
        verbs = defaultdict(Counter)
        for r in log:
            by[realm(r)] += 1
            verbs[realm(r)][verb(r["verb"])] += 1
        out[e["step"]] = (len(log), by, verbs, log)
    return out


def fmt(total, by, verbs, show_verbs):
    cells = [str(total)]
    for r in ("manifest", "history", "table", "control"):
        c = by.get(r, 0)
        if show_verbs and c:
            vs = " ".join(f"{k}{n}" for k, n in sorted(verbs[r].items()))
            cells.append(f"{c} ({vs})")
        else:
            cells.append(str(c))
    return cells


def main():
    args = sys.argv[1:]
    path = args[0]
    show_verbs = "--verbs" in args
    sample = []
    if "--sample" in args:
        sample = [int(x) for x in args[args.index("--sample") + 1].split(",")]
    report = json.load(open(path))
    st = steps(report)
    last = max(st)
    first_tail = last - len(TAIL) + 1
    rows = []
    # Step 1 is the session-settings step; history commit d is step d + 1.
    for d in sample:
        if d + 1 in st:
            rows.append((f"history commit #{d}", *fmt(*st[d + 1][:3], show_verbs)))
    for i, name in enumerate(TAIL):
        s = first_tail + i
        if "restart" in name:
            continue
        rows.append((name, *fmt(*st[s][:3], show_verbs)))
    print("| step | total | manifest | history | table | control |")
    print("|---|---:|---:|---:|---:|---:|")
    for r in rows:
        print("| " + " | ".join(r) + " |")
    if "--json" in args:
        out = {name: {"total": st[first_tail + i][0], **st[first_tail + i][1]}
               for i, name in enumerate(TAIL)}
        out["history_mutates"] = {s - 1: st[s][0] for s in st if 1 < s < first_tail}
        json.dump(out, open(args[args.index("--json") + 1], "w"), indent=1)


if __name__ == "__main__":
    main()
