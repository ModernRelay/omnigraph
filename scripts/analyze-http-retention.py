#!/usr/bin/env python3
"""Validate and describe the current-only fixed-state HTTP retention control.

Usage: python3 scripts/analyze-http-retention.py /absolute/retention/directory
Writes retention-analysis.json and retention-analysis.md. This is descriptive
local evidence: no leak verdict, capacity estimate, or significance test.
"""

import argparse
import importlib.util
import json
from pathlib import Path
import statistics
import sys

_spec = importlib.util.spec_from_file_location("http_analysis", Path(__file__).with_name("analyze-http-perf.py"))
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)
require, load, digest = base.require, base.load, base.digest
MODES = tuple(f"{route}:{mode}" for route in ("export", "changes/baseline")
              for mode in ("fast", "pause", "abandon"))


def unsigned(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def census(value, streams=False):
    keys = ("offered", "completed", "refused", "abandoned", "failed")
    require(all(unsigned(value[key]) for key in keys), "Invalid request census")
    require(value["offered"] == sum(value[key] for key in keys[1:]), "Unaccounted requests")
    require(value["failed"] == 0 and value["errors"] == [], "Worker failures")
    require(len(value["samples"]) == value["offered"], "Missing attempt durations")
    previous_end = -1
    for sample in value["samples"]:
        require(len(sample) == 3 and all(unsigned(item) for item in sample)
                and sample[0] >= previous_end and sample[1] >= sample[0] and sample[2] > 0,
                "Invalid or overlapping serial-worker durations")
        previous_end = sample[1]
    if not streams:
        require(value["completed"] > 0 and value["refused"] == value["abandoned"] == 0,
                "Reader failed or made no progress")
        return
    modes = value["successful_modes"]
    require(set(modes) == set(MODES) and all(unsigned(count) and count > 0 for count in modes.values()),
            "Missing stream route/consumer mode")
    require(value["completed"] == sum(count for mode, count in modes.items() if not mode.endswith(":abandon"))
            and value["abandoned"] == sum(count for mode, count in modes.items() if mode.endswith(":abandon")),
            "Stream outcome/mode census disagrees")
    require(unsigned(value["completed_body_bytes"]) and value["completed_body_bytes"] > 0,
            "Missing verified complete response bytes")
    windows = value["body_windows_ms"]
    require(len(windows) == value["completed"] + value["abandoned"], "Missing admitted body windows")
    require(all(len(window) == 2 and all(unsigned(item) for item in window) and window[1] >= window[0]
                for window in windows), "Invalid body windows")


def validate(directory):
    session = load(directory / "session.json")
    require(session.get("format") == "omnigraph-http-fixed-retention-v1"
            and session.get("claim_eligible") is False, "Wrong retention format")
    config, protocol = session["config"], session["protocol"]
    count, seconds = config["repetitions"], config["seconds"]
    require(unsigned(count) and 1 <= count <= 4 and unsigned(seconds) and 10 <= seconds <= 600,
            "Invalid requested control size")
    require(protocol.get("full_duration") == (seconds >= 300)
            and protocol.get("full_replication") == (count >= 2)
            and protocol.get("readers") == 2 and protocol.get("writes") == 0
            and protocol.get("idle_seconds") == 10, "Wrong fixed-state protocol")
    source = Path(config["fixture_session"])
    source_session, logical, fixtures, _, _ = base.validate(source)
    require(session["source_session_sha256"] == digest(source / "session.json"), "Source session changed")
    require(session["fixture_descriptor_sha256"] == digest(source / "fixture-fragmented.json"),
            "Source fixture descriptor changed")
    require(session["sut"] == source_session["sut"]["current"], "Different current SUT")
    require(session["runtime_environment"] == source_session["runtime_environment"], "Runtime environment differs")
    physical = fixtures["fragmented"]["physical"]["sha256"]
    require(session["fixture_physical_sha256"] == physical
            and session["logical_export_sha256"] == logical["logical_export_sha256"]
            and session["layout"] == fixtures["fragmented"]["layout"], "Fixture identity differs")
    complete = load(directory / "complete.json")
    require(complete.get("status") == "complete" and complete.get("repetitions") == count
            and complete.get("final_sut_attestation") == session["sut"], "Incomplete control or SUT drift")
    paths = sorted(directory.glob("retention-[0-9]*.json"))
    require(len(paths) == count, "Missing or extra retention repetitions")
    records, inventory = [], []
    for index, path in enumerate(paths):
        record = load(path)
        require(record.get("format") == session["format"] and record.get("claim_eligible") is False
                and record.get("repetition") == index and record.get("status") == "complete"
                and record.get("error") is None, "Failed or duplicate repetition")
        require(record.get("physical_verified") is True
                and record.get("fixture_physical_sha256") == physical
                and record.get("post_fixture_physical_sha256") == physical
                and record.get("logical_export_sha256") == session["logical_export_sha256"],
                "Fixture changed during retention control")
        require(len(record["readers"]) == 2, "Missing reader")
        for reader in record["readers"]:
            census(reader)
        census(record["streams"], streams=True)
        window = record["load_window_ms"]
        require(len(window) == 2 and all(unsigned(item) for item in window)
                and seconds * 1000 <= window[1] - window[0] <= (seconds + 40) * 1000,
                "Incomplete or unbounded load window")
        previous_time = -1
        for row in record["rss"]:
            require(len(row) == 3 and unsigned(row[0]) and row[0] >= previous_time
                    and row[1] in (1, 2, 3, 4, 5) and base.positive(row[2]), "Invalid RSS series")
            previous_time = row[0]
        require(any(row[1] == 2 for row in record["rss"]) and any(row[1] == 5 for row in record["rss"]),
                "Missing load/idle RSS observations")
        require(base.positive(record["post_idle_rss_kib"]), "Missing post-idle RSS")
        require(len(record["recovery_reads"]) == 25 and all(
            len(row) == 2 and unsigned(row[0]) and base.positive(row[1]) for row in record["recovery_reads"]),
            "Incomplete recovery read verification")
        require(unsigned(record["final_export_slot_refusals"]) and unsigned(record["final_export_elapsed_ms"])
                and record["final_export_elapsed_ms"] <= 21_000, "Unbounded final snapshot")
        records.append(record)
        inventory.append({"path": path.name, "sha256": digest(path)})
    for name in ("session.json", "complete.json"):
        inventory.append({"path": name, "sha256": digest(directory / name)})
    return session, records, inventory


def med(values):
    return statistics.median(values) if values else None


def summarize(record, seconds):
    start = record["load_window_ms"][0]
    reads = [sample for reader in record["readers"] for sample in reader["samples"]]
    def interval(begin, end):
        latencies = [row[2] / 1000 for row in reads if begin <= row[0] < end]
        rss = [row[2] / 1024 for row in record["rss"] if row[1] == 2 and begin <= row[0] < end]
        return {"start_seconds": (begin - start) / 1000, "end_seconds": (end - start) / 1000,
                "reads": len(latencies), "read_median_ms": med(latencies),
                "read_p95_ms": base.percentile(latencies, 0.95) if len(latencies) >= 20 else None,
                "rss_samples": len(rss), "rss_median_mib": med(rss)}
    quarters = [interval(start + index * seconds * 250, start + (index + 1) * seconds * 250)
                for index in range(4)]
    minutes = [interval(start + offset * 1000, start + min(offset + 60, seconds) * 1000)
               for offset in range(0, seconds, 60)]
    first, last = quarters[0], quarters[-1]
    idle = [row[2] / 1024 for row in record["rss"] if row[1] == 5]
    return {"repetition": record["repetition"], "quarters": quarters, "minute_bins": minutes,
            "first_last_read_ratio": last["read_median_ms"] / first["read_median_ms"]
                if first["read_median_ms"] and last["read_median_ms"] else None,
            "first_last_rss_mib": last["rss_median_mib"] - first["rss_median_mib"]
                if first["rss_median_mib"] is not None and last["rss_median_mib"] is not None else None,
            "post_idle_rss_mib": record["post_idle_rss_kib"] / 1024,
            "idle_rss_median_mib": med(idle), "idle_rss_drift": base.quarter_drift(idle),
            "recovery_read_median_ms": med([row[1] / 1000 for row in record["recovery_reads"]]),
            "drain_milliseconds": record["load_window_ms"][1] - start - seconds * 1000,
            "final_export_slot_refusals": record["final_export_slot_refusals"],
            "final_export_elapsed_ms": record["final_export_elapsed_ms"],
            "census": {"readers": [{key: reader[key] for key in ("offered", "completed", "failed")}
                                    for reader in record["readers"]],
                       "streams": {key: value for key, value in record["streams"].items()
                                   if key not in ("samples", "body_windows_ms")}},
            "thermal_observation": record.get("thermal_observation")}


def analyze(directory):
    session, records, inventory = validate(directory)
    return {"format": "omnigraph-http-retention-analysis-v1", "claim_eligible": False,
            "session_directory": str(directory), "session": session, "inputs": inventory,
            "repetitions": [summarize(record, session["config"]["seconds"]) for record in records],
            "interpretation": "Descriptive fixed-state current-build control. RSS measures resident pages, not live allocations. No leak verdict; no A/B regression or loaded-system capacity claim."}


def number(value):
    return "unavailable" if value is None else f"{value:.2f}"


def markdown(result):
    session = result["session"]
    config = session["config"]
    receipt = session["sut"]["build_receipt"]
    lines = ["# Fixed-state HTTP retention control", "", result["interpretation"], "",
             f"{config['repetitions']} fresh-process repetitions × {config['seconds']} seconds; {'full duration and replication' if session['protocol']['full_duration'] and session['protocol']['full_replication'] else 'smoke acquisition only'}. Two readers and one stream consumer; no writes. Every full export and baseline body was verified, baseline cursors matched the frozen graph head, and exact fixture bytes remained unchanged.", "",
             f"Source `{receipt['source_commit']}`; binary `{session['sut']['binary_sha256']}`. Raw session: `{result['session_directory']}`. Provenance, census and thermal observations are in `retention-analysis.json`.", "",
             "Bins use request dispatch time and nominal load time, excluding bounded final drain. Read p95 is descriptive for sampled requests, not a population estimate. Stream latencies include deliberate pauses and validation and are not compared as server performance.", "",
             "| Rep | First-quarter read ms | Last-quarter read ms | Last/first | First-quarter RSS MiB | Last-quarter RSS MiB | RSS change MiB | After idle MiB | Recovery read ms |",
             "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for rep in result["repetitions"]:
        first, last = rep["quarters"][0], rep["quarters"][-1]
        values = (first["read_median_ms"], last["read_median_ms"], rep["first_last_read_ratio"],
                  first["rss_median_mib"], last["rss_median_mib"], rep["first_last_rss_mib"],
                  rep["post_idle_rss_mib"], rep["recovery_read_median_ms"])
        lines.append(f"| {rep['repetition']} | {' | '.join(number(value) for value in values)} |")
    for label, key in (("Quarter medians", "quarters"), ("Minute bins", "minute_bins")):
        lines += ["", f"## {label}", "", "| Rep | Seconds | Reads | Read median ms | Read p95 ms | RSS samples | RSS median MiB |",
                  "|---:|---|---:|---:|---:|---:|---:|"]
        for rep in result["repetitions"]:
            for row in rep[key]:
                lines.append(f"| {rep['repetition']} | {row['start_seconds']:g}–{row['end_seconds']:g} | {row['reads']} | {number(row['read_median_ms'])} | {number(row['read_p95_ms'])} | {row['rss_samples']} | {number(row['rss_median_mib'])} |")
    lines += ["", "Requests share server processes and are correlated; there are only the declared process repetitions. No writes occur, so this control cannot establish behavior of caches or allocations retained across live mutations. Ten seconds idle is not a heap-retention proof. Host isolation, capacity and live cloud backends remain unqualified."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    try:
        result = analyze(directory)
        (directory / "retention-analysis.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
        (directory / "retention-analysis.md").write_text(markdown(result))
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(f"Refused retention analysis: {error}", file=sys.stderr)
        return 1
    print(directory / "retention-analysis.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
