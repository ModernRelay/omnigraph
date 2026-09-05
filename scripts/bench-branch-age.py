#!/usr/bin/env python3
"""Small, sequential graph-age diagnostics using the existing scenario harness."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = (
    "branch-create", "branch-create-from", "branch-list", "branch-delete",
    "general-merge-updates",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def source_identity():
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()
    return {"commit": git("rev-parse", "HEAD"), "tree": git("rev-parse", "HEAD^{tree}"),
            "clean": not git("status", "--porcelain=v1", "--untracked-files=normal")}


def matrix(smoke, extended, selected):
    shapes = [("smoke", 2, 1, 2, 2)] if smoke else [
        ("fresh", 0, 0, 2, 2), ("history16", 16, 0, 2, 2),
        ("history64", 64, 0, 2, 2), ("retired8", 0, 8, 2, 2),
    ]
    if extended:
        shapes += [("siblings8", 0, 0, 8, 2), ("tables8", 0, 0, 2, 8)]
    points = []
    for shape, history, retired, branches, tables in shapes:
        for scenario in selected:
            if shape in ("siblings8", "tables8") and scenario == "general-merge-updates":
                continue  # That existing merge owner has one table and two branches.
            params = {"rows": 16, "dims": 4, "seed": 42,
                      "history_commits": history, "retired_branches": retired}
            if scenario == "general-merge-updates":
                params.update(delta_rows=2, source_mode="update")
            else:
                params.update(branches=branches, tables=tables)
            points.append({"name": f"{shape}-{scenario}", "scenario": scenario, "params": params})
    return points


def admit(records, point, runs, identity, binary_hash):
    require(len(records) == runs, "Missing or extra repetitions")
    require([r.get("run") for r in records] == list(range(runs)), "Invalid repetition IDs")
    for record in records:
        require(record.get("exit_status") == 0, "Scenario failed")
        require(record.get("scenario") == point["scenario"], "Wrong scenario")
        require(all(record.get("params", {}).get(k) == v for k, v in point["params"].items()),
                "Requested parameters were not applied (possibly an older binary)")
        require(record.get("git_worktree_dirty") is False, "Dirty source during acquisition")
        commit = record.get("git_sha")
        require(isinstance(commit, str) and len(commit) >= 7 and identity["commit"].startswith(commit),
                "Source commit changed")
        require(record.get("git_tree_sha") == identity["tree"], "Source tree changed")
        require(record.get("benchmark_binary_sha256") == binary_hash, "Executable changed")
        for phase in ("setup", "operation", "verify"):
            status = record.get("phases", {}).get(phase, {})
            require(status.get("status") == "completed" and status.get("exit_status") == 0
                    and status.get("process_exit_status") == 0 and status.get("protocol_error") is None,
                    f"Incomplete or rejected {phase} phase")
        metrics = record["metrics"]
        require(metrics.get("production_path") is True, "Non-production operation")
        require(metrics.get("operation_wall_us", -1) >= 0, "Missing operation time")
        for label, parameter in (("history_commits", "history_commits"),
                                 ("retired_branches", "retired_branches")):
            require(metrics.get(f"setup_{label}_requested") == point["params"][parameter]
                    == metrics.get(f"setup_{label}_applied"), "Fixture aging was not applied")
        require(metrics.get("setup_age_content_verified") is True, "Missing age content proof")
        require(metrics["setup_main_history_after_age"] - metrics["setup_main_history_before_age"]
                == point["params"]["history_commits"], "Actual reachable history did not match")
        require(isinstance(metrics.get("operation_io_manifest_reads"), int), "Missing manifest I/O evidence")
        if point["scenario"] == "general-merge-updates":
            require(metrics.get("merge_outcome") == "merged" and metrics.get("final_rows") == 16,
                    "Merge did not verify the expected route and row count")
            require(metrics.get("verified_complete_main_rows") == 16
                    and metrics.get("verified_complete_source_rows") == 16,
                    "Merge did not verify every target and source row")
        else:
            require(metrics.get("verification_passed") is True, "Missing branch verification")


def summarize(point, records):
    summary = {**point, "samples": len(records)}
    for key in ("operation_wall_us", "operation_complete_wall_us", "operation_open_us",
                "operation_open_ms", "post_ack_reclaim_wait_us", "operation_io_manifest_reads",
                "operation_io_manifest_read_bytes", "operation_io_manifest_scan_count",
                "fixture_bytes", "fixture_files"):
        values = [r["metrics"].get(key) for r in records]
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values):
            summary[key] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
    summary["operation_peak_rss_bytes"] = statistics.median(r["operation_peak_rss_bytes"] for r in records)
    # Preserve each complete metric map, including deterministic cost counters,
    # instead of silently discarding new evidence fields from the Rust owner.
    summary["metrics"] = [r["metrics"] for r in records]
    return summary


def main():
    def terminate(signum, _frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--build-receipt", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--pause-seconds", type=float, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--scenario", choices=SCENARIOS, action="append")
    parser.add_argument("--smoke", action="store_true", help="one tiny aged/churned fixture per operation")
    parser.add_argument("--extended", action="store_true", help="also vary sibling count and table count separately")
    parser.add_argument("--plan", action="store_true", help="print parameters without building or running")
    args = parser.parse_args()
    require(1 <= args.runs <= 10, "--runs must be between 1 and 10")
    require(0 <= args.pause_seconds <= 60, "--pause-seconds must be between 0 and 60")
    require(1 <= args.timeout_seconds <= 600, "--timeout-seconds must be between 1 and 600")
    require(not (args.smoke and args.extended), "Choose smoke or extended")
    points = matrix(args.smoke, args.extended, list(dict.fromkeys(args.scenario or SCENARIOS)))
    if args.plan:
        print(json.dumps(points, indent=2))
        return
    require(args.binary and args.build_receipt and args.output,
            "Execution requires --binary, --build-receipt, and --output")
    binary = args.binary.resolve(strict=True)
    receipt = json.loads(args.build_receipt.read_text())
    identity = source_identity()
    require(identity["clean"], "Commit benchmark source before building and measuring")
    require(receipt["source"]["before"] == receipt["source"]["after"] == identity,
            "Build receipt must describe the current clean source")
    require(Path(receipt["source"]["directory"]).resolve() == ROOT, "Wrong build source directory")
    require(Path(receipt["build"]["cwd"]).resolve() == ROOT, "Wrong build working directory")
    require(receipt["build"]["exit_code"] == 0, "Build was not successful")
    command = receipt["build"]["command"]
    def option(name):
        values = [arg.split("=", 1)[1] for arg in command if arg.startswith(name + "=")]
        for index, arg in enumerate(command):
            if arg == name:
                require(index + 1 < len(command), f"Missing value for build option {name}")
                values.append(command[index + 1])
        require(len(values) <= 1, f"Ambiguous build option {name}")
        return values[0] if values else None
    require(command[:2] == ["cargo", "bench"] and "--locked" in command and "--no-run" in command
            and (option("-p") or option("--package")) == "omnigraph-engine"
            and option("--bench") == "scenarios" and option("--profile") in (None, "bench", "release"),
            "Expected a locked release scenario build receipt")
    binary_hash = sha256(binary)
    require(receipt["binary"]["sha256"] == binary_hash, "Saved binary differs from build receipt")
    require(Path(receipt["binary"]["path"]).resolve() == binary, "Wrong saved binary path")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
    environment.update(LANG="C", LC_ALL="C", LANCE_MEM_POOL_SIZE="268435456",
                       TOKIO_WORKER_THREADS="2", LANCE_CPU_THREADS="2", LANCE_IO_THREADS="2",
                       RAYON_NUM_THREADS="2", OMNIGRAPH_MERGE_LINEAGE="on")
    ledger = {"source": identity, "binary_sha256": binary_hash, "build_receipt": receipt,
              "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "environment": environment, "nice": 15, "pause_seconds": args.pause_seconds,
              "runs_per_point": args.runs, "points": points, "results": [],
              "claim_eligible": False, "durable_record": False, "complete": False}
    def save():
        (output / "execution.json").write_text(json.dumps(ledger, indent=2) + "\n")
    save()
    for index, point in enumerate(points):
        if index:
            time.sleep(args.pause_seconds)
        path = output / (point["name"] + ".jsonl")
        argv = ["nice", "-n", "15", str(binary), "--scenario", point["scenario"],
                "--runs", str(args.runs), "--out", str(path)]
        for key, value in point["params"].items():
            argv.extend(["--" + key.replace("_", "-"), str(value)])
        result = {"point": point["name"], "command": argv, "accepted": False}
        print(f"START {index + 1}/{len(points)} {point['name']}", flush=True)
        start = time.monotonic()
        try:
            with (output / (point["name"] + ".log")).open("w") as log:
                child = subprocess.Popen(argv, cwd=ROOT, env=environment, stdout=log,
                                         stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    result["exit_code"] = child.wait(timeout=args.timeout_seconds)
                except BaseException:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass  # The process group finished while cancellation arrived.
                    child.wait()
                    raise
            require(result["exit_code"] == 0, "Benchmark process failed; inspect its log")
            records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
            admit(records, point, args.runs, identity, binary_hash)
            require(source_identity() == identity, "Source changed during acquisition")
            result.update(accepted=True, summary=summarize(point, records))
        except BaseException as error:
            result["error"] = str(error)
            raise
        finally:
            result["elapsed_seconds"] = time.monotonic() - start
            ledger["results"].append(result)
            save()
        print(f"DONE {point['name']} {result['elapsed_seconds']:.2f}s", flush=True)
    require(sha256(binary) == binary_hash, "Executable changed during acquisition")
    ledger["complete"] = True
    ledger["completed_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()
    lines = ["# Small graph-age measurements", "", "Three separate child processes prepare, measure, and verify each repetition. "
             "Local directional observations; no tail-latency or controlled speedup claim.", "",
             "| Fixture / operation | Samples | Operation median ms | Operation RSS MiB |",
             "|---|---:|---:|---:|"]
    for result in ledger["results"]:
        s = result["summary"]
        lines.append(f"| {s['name']} | {s['samples']} | {s['operation_wall_us']['median'] / 1000:.3f} "
                     f"| {s['operation_peak_rss_bytes'] / 1048576:.2f} |")
    (output / "summary.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
