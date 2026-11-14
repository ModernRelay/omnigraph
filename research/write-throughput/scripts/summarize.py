#!/usr/bin/env python3
"""Summarize instrument JSONL files into median-of-repetitions tables.

usage: summarize.py <results-dir> <label> [<label> ...]
Reads <dir>/<label>-<target>.jsonl for target in (s3, local).
"""
import json, os, statistics, sys
from collections import defaultdict

def load(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]

def main():
    root, labels = sys.argv[1], sys.argv[2:]
    cells = defaultdict(list)
    for label in labels:
        for target in ("s3", "local"):
            for rec in load(os.path.join(root, f"{label}-{target}.jsonl")):
                cells[(target, rec["mode"], rec["writers"], label)].append(rec)
    print("| target | mode | writers | variant | reps | writes/s (median, min–max) | commits/s | p50 ms | p99 ms | max+drain ms | req/write | reprep/write |")
    print("|---|---|---:|---|---:|---|---:|---:|---:|---:|---:|---:|")
    for key in sorted(cells, key=lambda k: (k[0], k[1], k[2], labels.index(k[3]))):
        recs = cells[key]
        wps = [r["writes_per_sec"] for r in recs]
        med = lambda xs: statistics.median(xs)
        print("| {} | {} | {} | {} | {} | {:.2f} ({:.2f}–{:.2f}) | {:.2f} | {:.0f} | {:.0f} | {:.0f} | {:.1f} | {:.2f} |".format(
            key[0], key[1], key[2], key[3], len(recs), med(wps), min(wps), max(wps),
            med([r["commits_per_sec"] for r in recs]),
            med([r["service_us"]["p50"] for r in recs]) / 1000,
            med([r["service_us"]["p99"] for r in recs]) / 1000,
            med([r["service_us"]["max_including_drain"] for r in recs]) / 1000,
            med([r["requests_per_write"]["total"] for r in recs]),
            med([r["engine_reprepares_per_write"] for r in recs]),
        ))

if __name__ == "__main__":
    main()
