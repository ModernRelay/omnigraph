#!/usr/bin/env python3
"""Render result JSONL files as markdown tables: commits/s (p50 ms, p99 ms)."""
import json, sys, collections
def load(label):
    rows = {}
    try:
        for line in open(f"research/write-throughput-wal/results/{label}.jsonl"):
            r = json.loads(line)
            rows[(r["backend"], r["latency_ms"], r["mode"], r["writers"])] = r
    except FileNotFoundError:
        pass
    return rows
labels = sys.argv[2:]
backend, lat, mode = sys.argv[1].split(":")
lat = int(lat)
data = {l: load(l) for l in labels}
print("| N | " + " | ".join(labels) + " |")
print("|---|" + "---|" * len(labels))
for n in (1, 4, 16, 64):
    cells = []
    for l in labels:
        r = data[l].get((backend, lat, mode, n))
        if r is None:
            cells.append("-")
        else:
            ok = "" if r.get("verified", True) else " UNVERIFIED"
            cells.append(f"{r['commits_per_s']:.1f} ({r['p50_ms']:.0f}, {r['p99_ms']:.0f}){ok}")
    print(f"| {n} | " + " | ".join(cells) + " |")
