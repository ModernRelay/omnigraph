#!/usr/bin/env python3
"""Compare the three local routes of an engine-target scenario with a release `omnigraph-bench`.

Usage:
  scripts/compare-local-routes.py --bench target/release/omnigraph-bench --cache /tmp/bench-cache
  scripts/compare-local-routes.py --out out summarize
  scripts/compare-local-routes.py --bench ... --cache ... --passes 4 --repetitions 10 --scenario tiny-mutate

Routes (`omnigraph-bench run --local-route`):
  direct        a restored working copy, Lance's direct local file path (the default)
  object-store  the same restored copy, every read and write through the object store
  read-through  an empty clone root read through the frozen fixture; nothing is restored

A pass is one `omnigraph-bench run` invocation of `--repetitions` repetitions per route, a fresh worker process
per repetition. The routes run in one order in odd passes and in the reverse order in even passes, so a drift of
the machine reaches every route. Before a scenario's passes one repetition on the direct route fills the dataset
cache and is thrown away (<out>/<scenario>.warmup.*), so no measured run builds a fixture. `--out` must be a
directory that holds no earlier run: a rerun into a used directory is refused, so every pass file under it
belongs to the binary `binary.json` names, and `run.json` records the passes and repetitions asked for.

Two times are printed. `elapsed_us` is the bench's own sample, the scenario's measured step only; its p50 is
computed over every sample of a route and per pass. "process wall s per repetition" is the wall time of the whole
`run` process divided by its repetitions: the harness, the restore of the working copy (direct, object-store) or
the empty clone root (read-through), the workers and the checks after them; on the read-through route it also
carries one read of every frozen-fixture byte per run.

The threshold of the graph shallow clone RFC reads from this output: with four passes of ten repetitions, the p50
of the measured step over every read-through sample is no higher than 1.05 times the p50 over every object-store
sample. `summarize` prints that ratio as "x object-store" and a verdict line per scenario, marked as indicative
below four passes of ten repetitions; a pass that failed or is missing is named and withholds the verdict; the
exit code is 1 when any scenario's verdict is FAIL or withheld. Without `--scenario`, `summarize` reads the
scenarios from `run.json`.

The bench calls the platform probe, so run this from a terminal that may call it.
"""
import argparse
import hashlib
import json
import pathlib
import statistics
import subprocess
import sys
import time

ROUTES = ["direct", "object-store", "read-through"]
SCENARIOS = ["e2e-query-scan", "tiny-mutate", "e2e-merge-all-changed", "very-long-history-1000-write"]
LANCE = ["get", "put", "put_part", "head", "list", "delete", "copy", "rename", "multipart_complete",
         "multipart_abort"]
CONTROL = ["read_text", "read_text_if_exists", "read_text_versioned", "exists", "list_dir", "mutation_calls",
           "write_text", "delete"]
THRESHOLD = 1.05
PASSES = 4
REPETITIONS = 10


def invoke(bench, config, cache, out, scenario, route, repetitions, stem):
    """One `omnigraph-bench run`: its JSON, its stderr and the wall time of the whole process land under out/<stem>."""
    argv = [str(bench), "run", scenario, "--config", str(config), "--dataset-cache", str(cache),
            "--repetitions", str(repetitions), "--local-route", route, "--json"]
    started = time.monotonic()
    done = subprocess.run(argv, capture_output=True, text=True)
    wall = time.monotonic() - started
    (out / f"{stem}.json").write_text(done.stdout)
    (out / f"{stem}.stderr.log").write_text(done.stderr)
    (out / f"{stem}.wall").write_text(json.dumps({"wall_s": round(wall, 3), "repetitions": repetitions}))
    print(f"{stem}: exit={done.returncode} wall={wall:.1f}s", flush=True)
    if done.returncode != 0:
        print(done.stdout[-3000:])
        print(done.stderr[-3000:], file=sys.stderr)
    return done.returncode


def samples_of(out, scenario, route):
    """Every sample of a route, the p50 of each pass, each pass's process wall time per repetition, and the
    pass files that gave no sample (a failed run prints `completed_runs` and no `runs`)."""
    samples, passes, walls, without = [], [], [], []
    for path in sorted(out.glob(f"{scenario}.{route}.pass*.json")):
        before = len(samples)
        try:
            doc = json.loads(path.read_text())
        except json.JSONDecodeError:
            doc = {}
        for run in doc.get("runs") or []:
            one = run.get("samples", [])
            samples.extend(one)
            if one:
                passes.append(statistics.median(s["elapsed_us"] for s in one))
        if len(samples) == before:
            without.append(path.name)
        wall = path.with_suffix(".wall")
        if wall.exists():
            timing = json.loads(wall.read_text())
            walls.append(timing["wall_s"] / timing["repetitions"])
    return samples, passes, walls, without


def counts(sample, group, part, names):
    block = sample.get(group) or {}
    block = block.get(part, {}) if part else block
    return " ".join(f"{name}={block.get(name, 0)}" for name in names if block.get(name, 0))


def summarize(out, scenarios):
    binary = out / "binary.json"
    if binary.exists():
        build = json.loads(binary.read_text())
        print(f"binary: {build['binary']}, sha256 {build['sha256']}, modified {build['modified']}")
    asked = json.loads((out / "run.json").read_text()) if (out / "run.json").exists() else {}
    scenarios = scenarios or asked.get("scenarios") or SCENARIOS
    failed, withheld = 0, 0
    for scenario in scenarios:
        print(f"\n## {scenario}")
        print("route\tn\tmin_us\tp50_us\tmax_us\tx direct\tx object-store\tp50 per pass"
              "\tprocess wall s per repetition, per pass\twall p50 s\twall x direct")
        medians, wall_medians, rows, missing = {}, {}, [], []
        for route in ROUTES:
            samples, passes, walls, without = samples_of(out, scenario, route)
            for name in without:
                missing.append(f"{name} gave no sample")
            if asked.get("passes") and len(passes) != asked["passes"]:
                missing.append(f"{route}: {len(passes)} of {asked['passes']} passes have samples")
            if walls:
                wall_medians[route] = statistics.median(walls)
            if not samples:
                rows.append((route, None, passes, walls))
                continue
            elapsed = sorted(s["elapsed_us"] for s in samples)
            medians[route] = statistics.median(elapsed)
            rows.append((route, elapsed, passes, walls))
        for route, elapsed, passes, walls in rows:
            if elapsed is None:
                print(f"{route}\t0\t-\t-\t-\t-\t-\t-\t-\t-\t-")
                continue
            p50 = medians[route]
            vs_direct = f"{p50 / medians['direct']:.2f}x" if medians.get("direct") else "-"
            vs_object = f"{p50 / medians['object-store']:.2f}x" if medians.get("object-store") else "-"
            per_pass = ",".join(f"{value:.0f}" for value in passes)
            wall = ",".join(f"{value:.2f}" for value in walls)
            wall_p50 = f"{wall_medians[route]:.2f}" if route in wall_medians else "-"
            wall_vs = (f"{wall_medians[route] / wall_medians['direct']:.2f}x"
                       if route in wall_medians and wall_medians.get("direct") else "-")
            print(f"{route}\t{len(elapsed)}\t{elapsed[0]}\t{p50:.0f}\t{elapsed[-1]}\t{vs_direct}\t{vs_object}"
                  f"\t{per_pass}\t{wall}\t{wall_p50}\t{wall_vs}")
        print("route\tLance calls on __manifest\tLance calls on tables\tcontrol calls\tsame in every sample")
        for route in ROUTES:
            samples, _, _, _ = samples_of(out, scenario, route)
            if not samples:
                continue
            first = samples[0]

            def shape(sample):
                return (json.dumps(sample.get("logical_store_calls"), sort_keys=True),
                        json.dumps(sample.get("control_store_calls"), sort_keys=True))

            same = all(shape(s) == shape(first) for s in samples)
            print(f"{route}\t{counts(first, 'logical_store_calls', 'manifest', LANCE)}\t"
                  f"{counts(first, 'logical_store_calls', 'table', LANCE)}\t"
                  f"{counts(first, 'control_store_calls', None, CONTROL)}\t{same}")
        for line in missing:
            print(f"missing: {line}")
        if missing or "read-through" not in medians or "object-store" not in medians:
            print("verdict: none, a pass is missing or gave no sample")
            withheld += 1
            continue
        if not medians["object-store"]:
            print("verdict: none, the object-store p50 is zero")
            withheld += 1
            continue
        ratio = medians["read-through"] / medians["object-store"]
        outcome = "PASS" if ratio <= THRESHOLD else "FAIL"
        failed += outcome == "FAIL"
        below = (asked.get("passes", PASSES) < PASSES or asked.get("repetitions", REPETITIONS) < REPETITIONS)
        size = (f" (below the RFC's {PASSES} passes of {REPETITIONS} repetitions, indicative only)"
                if below else "")
        print(f"verdict: read-through p50 {medians['read-through']:.0f} us is {ratio:.3f}x the object-store p50 "
              f"{medians['object-store']:.0f} us over every sample, threshold {THRESHOLD:.2f}x: {outcome}{size}")
    return 1 if failed or withheld else 0


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", nargs="?", default="run", choices=["run", "summarize"])
    parser.add_argument("--bench", type=pathlib.Path, help="the release omnigraph-bench binary")
    parser.add_argument("--config", type=pathlib.Path, default=pathlib.Path("benchmarks/benchmarks.yaml"))
    parser.add_argument("--cache", type=pathlib.Path, help="the dataset cache the runs share")
    parser.add_argument("--repetitions", type=int, default=REPETITIONS)
    parser.add_argument("--passes", type=int, default=PASSES)
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("out"),
                        help="output directory of this run; a used one is refused, so every pass under it is "
                             "the named binary's")
    args = parser.parse_args(argv[1:])
    if args.command == "summarize":
        return summarize(args.out, args.scenario)
    scenarios = args.scenario or SCENARIOS
    if args.bench is None or args.cache is None:
        print("run needs --bench and --cache", file=sys.stderr)
        return 2
    if args.repetitions < 1 or args.passes < 1:
        print("run needs at least one pass of one repetition", file=sys.stderr)
        return 2
    if not args.bench.is_file():
        print(f"missing release binary: {args.bench}", file=sys.stderr)
        return 2
    if args.out.exists() and not args.out.is_dir():
        print(f"{args.out} is not a directory; pick another --out", file=sys.stderr)
        return 2
    used = sorted(path.name for path in args.out.glob("*.json")) if args.out.exists() else []
    if used:
        print(f"{args.out} holds an earlier run ({used[0]}, {len(used)} files); pick another --out",
              file=sys.stderr)
        return 2
    args.out.mkdir(parents=True, exist_ok=True)
    args.cache.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(args.bench.read_bytes()).hexdigest()
    (args.out / "binary.json").write_text(json.dumps({
        "binary": str(args.bench.resolve()),
        "sha256": digest,
        "modified": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(args.bench.stat().st_mtime)),
    }, indent=2))
    (args.out / "run.json").write_text(json.dumps({
        "passes": args.passes,
        "repetitions": args.repetitions,
        "scenarios": scenarios,
    }, indent=2))
    failed = 0
    for scenario in scenarios:
        if invoke(args.bench, args.config, args.cache, args.out, scenario, "direct", 1, f"{scenario}.warmup"):
            print(f"{scenario}: the warm-up run failed, so nothing was measured; stopping", file=sys.stderr)
            return 1
        for pass_index in range(1, args.passes + 1):
            order = ROUTES if pass_index % 2 else list(reversed(ROUTES))
            for route in order:
                stem = f"{scenario}.{route}.pass{pass_index}"
                failed += 1 if invoke(args.bench, args.config, args.cache, args.out, scenario, route,
                                      args.repetitions, stem) else 0
    verdict = summarize(args.out, scenarios)
    return 1 if failed or verdict else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
