#!/usr/bin/env python3
"""Summarize instrument JSONL files: one line per cell."""
import json, sys
rows = []
for path in sys.argv[1:]:
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
print(f"{'label':<22}{'be':<5}{'mode':<9}{'w':>3}{'t':>2} {'c/s':>7} {'p50':>8} {'p99':>8} {'maxall':>8} {'fair':>9} {'repr':>5} {'drv':>4} {'req/c':>6} {'mfst':>6} {'ok':>3}")
for r in rows:
    fair = f"{r['per_writer_min']}/{r['per_writer_max']}"
    rq = r['requests_per_commit']
    print(f"{r['label']:<22}{r['backend']:<5}{r['mode']:<9}{r['writers']:>3}{r['types']:>2} {r['commits_per_s']:>7} {r['p50_ms']:>8} {r['p99_ms']:>8} {r['max_any_incl_drain_ms']:>8} {fair:>9} {r['reprepares_per_commit']:>5} {r['driver_retries']:>4} {rq['total']:>6} {rq['manifest']['total']:>6} {'y' if r['verified'] else 'N':>3}")
