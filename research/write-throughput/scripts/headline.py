#!/usr/bin/env python3
"""Headline table: baseline vs best variant per (target, mode, writers),
medians of repetitions. usage: headline.py <results-dir>"""
import json, os, statistics, sys
from collections import defaultdict

root = sys.argv[1]
cells = defaultdict(list)
for name in os.listdir(root):
    if not name.startswith("opt-") or not name.endswith(".jsonl"):
        continue
    label, target = name[:-6].rsplit("-", 1)
    with open(os.path.join(root, name)) as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                cells[(target, r["mode"], r["writers"], label)].append(r)

def med(recs, f):
    return statistics.median(f(r) for r in recs)

print("| target | mode | writers | baseline w/s | baseline p99 ms | best variant | best w/s | best commits/s | best p99 ms | speed-up |")
print("|---|---|---:|---:|---:|---|---:|---:|---:|---:|")
for target in ("s3", "local"):
    for mode in ("same", "separate"):
        for writers in (1, 4, 16, 64):
            base = cells.get((target, mode, writers, "opt-baseline"))
            if not base:
                continue
            variants = [l for (t, m, w, l) in cells if (t, m, w) == (target, mode, writers) and l not in ("opt-baseline", "opt-baseline-claim")]
            best = max(variants, key=lambda l: med(cells[(target, mode, writers, l)], lambda r: r["writes_per_sec"]))
            b = cells[(target, mode, writers, best)]
            bw = med(base, lambda r: r["writes_per_sec"])
            vw = med(b, lambda r: r["writes_per_sec"])
            print(f"| {target} | {mode} | {writers} | {bw:.2f} | {med(base, lambda r: r['service_us']['p99'])/1000:.0f} | {best} | {vw:.2f} | {med(b, lambda r: r['commits_per_sec']):.2f} | {med(b, lambda r: r['service_us']['p99'])/1000:.0f} | {vw/bw:.1f}× |")
