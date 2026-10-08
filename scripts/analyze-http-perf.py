#!/usr/bin/env python3
"""Validate and summarize the local controlled HTTP diagnostic (not an archive).

Usage: python3 scripts/analyze-http-perf.py /absolute/path/to/session
The default outputs are analysis.json and analysis.md beside the raw records.
Request samples stay nested within fresh-process repetitions; they are not
independent experiment repetitions. This script never produces a CI gate.
"""

import argparse
import hashlib
import json
import math
import plistlib
import shlex
from pathlib import Path
import statistics
import sys

FORMAT = "omnigraph-http-controlled-diagnostic-v1"
STATES = ("bulk", "fragmented", "maintenance-optimized")
ARMS = ("baseline", "current")
COMPARISON_RULE = "every paired-block ratio > 1 + maximum symmetric within-block same-arm gap + margin"
METRICS = ("query_median_ms", "query_p95_ms", "export_median_ms",
           "query_rss_mib", "export_rss_mib", "post_idle_rss_mib")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    require(path.stat().st_size <= 32 * 1024 * 1024, f"Oversized input: {path}")
    return json.loads(path.read_text(), parse_constant=lambda value: (_ for _ in ()).throw(
        ValueError(f"Non-finite JSON number: {value}")))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def positive(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def spread(values):
    return {"n": len(values), "median": statistics.median(values),
            "min": min(values), "max": max(values), "values": values}


def percentile(values, fraction):
    # Nearest rank; the report labels this as a within-repetition statistic.
    require(len(values) >= math.ceil(1 / (1 - fraction)), "Too few requests for percentile")
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def quarter_drift(values):
    width = len(values) // 4
    if width == 0:
        return None
    first, last = statistics.median(values[:width]), statistics.median(values[-width:])
    return {"first_quarter_median": first, "last_quarter_median": last,
            "last_over_first": last / first, "difference": last - first}


def summarize_rep(record):
    raw = record["raw_samples"]
    reads = [value / 1000 for value in raw["query_us"]]
    exports = [value / 1000 for value in raw["export_us"]]
    phases = {phase: [row[2] / 1024 for row in raw["rss"] if row[1] == phase]
              for phase in (1, 2, 3, 4)}
    result = {key: record[key] for key in ("order", "block", "position_in_block", "state", "arm")}
    result.update(query_median_ms=statistics.median(reads),
                  query_p95_ms=percentile(reads, 0.95) if len(reads) >= 20 else None,
                  export_median_ms=statistics.median(exports),
                  query_rss_mib=statistics.median(phases[2]) if phases[2] else None,
                  export_rss_mib=statistics.median(phases[3]) if phases[3] else None,
                  post_idle_rss_mib=raw["post_idle_rss_kib"] / 1024 if raw["post_idle_rss_kib"] else None,
                  query_drift=quarter_drift(reads),
                  query_rss_drift=quarter_drift(phases[2]),
                  idle_rss_drift=quarter_drift(phases[4]),
                  rss_samples_by_phase={str(key): len(value) for key, value in phases.items()},
                  rss_observed_peak_mib=max((row[2] / 1024 for row in raw["rss"]), default=None),
                  thermal_observation=record.get("thermal_observation", {"unavailable": "not captured"}))
    return result


def validate(directory):
    session = load(directory / "session.json")
    require(session.get("format") == FORMAT, "Unsupported session format")
    require(session.get("claim_eligible") is False, "This analyzer only accepts local diagnostic records")
    config, protocol = session["config"], session["protocol"]
    blocks = config["abba_blocks"]
    require(isinstance(blocks, int) and 1 <= blocks <= 6, "Invalid block count")
    require(protocol.get("margin_fraction") == 0.20, "Missing predeclared 20% margin")
    require(protocol.get("comparison_rule") == COMPARISON_RULE, "Different comparison rule")
    require(session.get("runtime_environment", {}).get("inheritance")
            == "cleared for fixture CLI and both server arms", "Uncontrolled inherited server environment")
    require(protocol.get("process") == "fresh per repetition", "Wrong process reset")
    require(protocol.get("warmup") == {"queries": 25, "exports": 2}, "Mixed warmup protocol")
    require("verification excluded" in protocol.get("timing", ""), "Missing timer boundary")
    require(session.get("machine"), "Missing machine identity")
    for arm in (*ARMS, "fixture_cli"):
        sut = session["fixture_cli"] if arm == "fixture_cli" else session["sut"][arm]
        receipt = sut["build_receipt"]
        require(receipt.get("profile") == "release", f"Non-release {arm}")
        require(sha(sut["binary_sha256"]) and sha(sut["build_receipt_sha256"]), f"Missing {arm} digests")
        require(receipt.get("binary_sha256") == sut["binary_sha256"], f"Mismatched {arm} receipt")
        require(receipt.get("source_clean") is True, f"Dirty source: {arm}")
        require(all(isinstance(receipt.get(key), str) and len(receipt[key]) == 40
                    and all(char in "0123456789abcdef" for char in receipt[key])
                    for key in ("source_commit", "source_tree")), f"Missing source identity: {arm}")
        require(receipt.get("compiler", {}).get("rustc_vv") and receipt.get("build_command"),
                f"Missing build provenance: {arm}")
        require(sha(receipt.get("cargo_lock_sha256")) and sha(receipt.get("cargo_config_sha256")),
                f"Missing Cargo provenance: {arm}")
        require(receipt.get("rustflags") == "" and receipt.get("cargo_encoded_rustflags") is None,
                f"Unexpected Rust flags: {arm}")
    baseline_receipt = session["sut"]["baseline"]["build_receipt"]
    current_receipt = session["sut"]["current"]["build_receipt"]
    for key in ("compiler", "features", "default_features", "profile_config",
                "cargo_lock_sha256", "cargo_config_sha256"):
        require(key in baseline_receipt and baseline_receipt[key] == current_receipt.get(key),
                f"Build conditions differ: {key}")
        require(session["fixture_cli"]["build_receipt"].get(key) == current_receipt.get(key),
                f"Fixture builder conditions differ: {key}")
    logical = load(directory / "logical-fixture.json")
    require(sha(logical.get("logical_export_sha256")) and logical.get("records", 0) > 0,
            "Missing logical fixture proof")
    fixtures = {}
    for state in STATES:
        fixture = load(directory / f"fixture-{state}.json")
        require(fixture.get("state") == state, f"Wrong fixture state: {state}")
        require(sha(fixture["physical"].get("sha256")), f"Missing physical digest: {state}")
        require(fixture.get("layout", {}).get("tables"), f"Missing accepted layout: {state}")
        fixtures[state] = fixture
    require(len({item["active_path"] for item in fixtures.values()}) == 1, "Different active fixture paths")
    complete = load(directory / "complete.json")
    expected_count = blocks * len(STATES) * 4
    require(complete.get("status") == "complete" and complete.get("repetitions") == expected_count,
            "Incomplete acquisition")
    require(complete.get("final_sut_attestation") == {
        **session["sut"], "fixture_cli": session["fixture_cli"]}, "Final executable attestation differs")
    paths = sorted(directory.glob("sample-*.json"))
    require(len(paths) == expected_count, "Missing or extra sample files")
    records, input_inventory = [], []
    export_sizes = set()
    for order, path in enumerate(paths):
        record = load(path)
        block, offset = divmod(order, len(STATES) * 4)
        state_index, position = divmod(offset, 4)
        state = STATES[(state_index + block) % len(STATES)]
        arm = ("baseline", "current", "current", "baseline")[position]
        require(all(record.get(key) == value for key, value in {
            "format": FORMAT, "status": "complete", "error": None, "order": order,
            "block": block, "position_in_block": position, "state": state, "arm": arm,
            "claim_eligible": False,
        }.items()), f"Failed, duplicate, reordered or wrong sample: {path.name}")
        require(record.get("binary_sha256") == session["sut"][arm]["binary_sha256"], "SUT changed")
        require(record.get("fixture_physical_sha256") == fixtures[state]["physical"]["sha256"],
                "Different physical fixtures in comparison")
        require(record.get("physical_verified") is True
                and record.get("post_fixture_physical_sha256") == record["fixture_physical_sha256"],
                "Missing read-only physical verification")
        require(record.get("layout") == fixtures[state]["layout"], "Physical layout drift")
        require(record.get("logical_export_sha256") == logical["logical_export_sha256"], "Logical data differs")
        raw = record["raw_samples"]
        require(raw.get("validated_query_count") == config["query_samples"], "Unvalidated query responses")
        require(raw.get("validated_export_count") == config["export_samples"], "Unvalidated export responses")
        for key, expected in (("query_us", config["query_samples"]), ("export_us", config["export_samples"]),
                              ("export_bytes", config["export_samples"])):
            require(len(raw[key]) == expected and all(positive(value) for value in raw[key]),
                    f"Incomplete or invalid {key}: {path.name}")
        export_sizes.update(raw["export_bytes"])
        previous_time = -1
        for row in raw["rss"]:
            require(len(row) == 3 and isinstance(row[0], int) and row[0] >= previous_time
                    and row[1] in (1, 2, 3, 4) and positive(row[2]), "Invalid RSS series")
            previous_time = row[0]
        require(raw["post_idle_rss_kib"] is None or positive(raw["post_idle_rss_kib"]), "Invalid idle RSS")
        records.append(record)
        input_inventory.append({"path": path.name, "sha256": digest(path)})
    require(len(export_sizes) == 1, "Different complete export sizes between arms or states")
    for name in ("session.json", "complete.json", "logical-fixture.json", *(f"fixture-{state}.json" for state in STATES)):
        input_inventory.append({"path": name, "sha256": digest(directory / name)})
    return session, logical, fixtures, records, input_inventory


def floor_for(reps, metric):
    gaps = []
    for block in sorted({rep["block"] for rep in reps}):
        for arm in sorted({rep["arm"] for rep in reps}):
            values = [rep[metric] for rep in reps if rep["block"] == block and rep["arm"] == arm]
            if len(values) != 2 or any(value is None for value in values):
                return None
            gaps.append(max(values) / min(values) - 1)
    return {"maximum_symmetric_relative_gap": max(gaps), "within_block_same_arm_gaps": gaps}


def compare(reps, metric, margin, blocks):
    noise = floor_for(reps, metric)
    if noise is None:
        return {"finding": "unavailable; missing RSS/insufficient requests"}
    ratios = []
    for block in range(blocks):
        def arm_value(arm):
            return statistics.geometric_mean(rep[metric] for rep in reps
                                             if rep["block"] == block and rep["arm"] == arm)
        ratios.append(arm_value("current") / arm_value("baseline"))
    threshold = 1 + noise["maximum_symmetric_relative_gap"] + margin
    finding = "no detected regression at the declared margin; not equivalence"
    if blocks < 3:
        finding = "insufficient blocks for a regression decision"
    elif min(ratios) > threshold:
        finding = "provisional local regression signal"
    elif max(ratios) < 1 / threshold:
        finding = "provisional local improvement signal"
    return {"finding": finding, "current_over_baseline_by_block": ratios,
            "ratio_summary": spread(ratios), "aa_noise": noise, "threshold_ratio": threshold}


def analyze(directory):
    session, logical, fixtures, records, inventory = validate(directory)
    reps = [summarize_rep(record) for record in records]
    blocks, margin = session["config"]["abba_blocks"], session["protocol"]["margin_fraction"]
    cells, comparisons, physical_effects = [], [], []
    for state in STATES:
        for arm in ARMS:
            selected = [rep for rep in reps if rep["state"] == state and rep["arm"] == arm]
            cell = {"state": state, "arm": arm, "repetitions": len(selected), "metrics": {}}
            for metric in METRICS:
                values = [rep[metric] for rep in selected if rep[metric] is not None]
                cell["metrics"][metric] = spread(values) if len(values) == len(selected) else None
            cells.append(cell)
        for metric in METRICS:
            comparisons.append({"state": state, "metric": metric, **compare(
                [rep for rep in reps if rep["state"] == state], metric, margin, blocks)})
    for arm in ARMS:
        for before, after in (("bulk", "fragmented"), ("fragmented", "maintenance-optimized")):
            for metric in METRICS:
                subset = [rep for rep in reps if rep["arm"] == arm and rep["state"] in (before, after)]
                if any(rep[metric] is None for rep in subset):
                    continue
                ratios = []
                floors = []
                for state in (before, after):
                    floors.append(floor_for([rep for rep in subset if rep["state"] == state], metric)
                                  ["maximum_symmetric_relative_gap"])
                for block in range(blocks):
                    def state_value(state):
                        return statistics.geometric_mean(rep[metric] for rep in subset
                                                         if rep["block"] == block and rep["state"] == state)
                    ratios.append(state_value(after) / state_value(before))
                physical_effects.append({"arm": arm, "before": before, "after": after, "metric": metric,
                    "after_over_before_by_block": ratios, "ratio_summary": spread(ratios),
                    "conservative_combined_aa_floor": max(floors),
                    "note": "State bundle comparison; not isolated fragment/history/index causality"})
    return {"format": "omnigraph-http-analysis-v1", "claim_eligible": False,
            "session_directory": str(directory), "inputs": inventory, "session": session,
            "logical_fixture": logical, "physical_fixtures": {key: value["layout"] for key, value in fixtures.items()},
            "repetitions": reps, "cells": cells, "code_comparisons": comparisons,
            "physical_state_comparisons": physical_effects,
            "limitations": ["Exploratory local file backend, fresh server per repetition; OS page cache uncontrolled.",
                "Six fresh-process reps per cell at the default three blocks; requests are nested samples, not independent reps.",
                "Within-repetition p95 uses 500 serial requests by default; not a loaded-system tail or capacity claim.",
                "RSS is sampled process residency, not live heap or allocation retention; five seconds idle cannot prove a leak or its absence.",
                "Comparison threshold is a conservative screening rule, not a confidence interval or hypothesis test.",
                "State treatments jointly change fragments, history and possibly indices; optimize is a maintenance bundle.",
                "No storage-call accounting, controlled host isolation or RFC 0039 durable archive; claim_eligible remains false."]}


def display(value):
    if value is None:
        return "unavailable"
    return f"{value['median']:.2f} [{value['min']:.2f}, {value['max']:.2f}]"


def machine_label(machine):
    identity = f"{machine.get('os', '?')}/{machine.get('arch', '?')}, {machine.get('available_parallelism', '?')} available CPUs"
    filesystem = machine.get("filesystem", {}).get("stdout", "")
    try:
        details = plistlib.loads(filesystem.encode())
        storage = f"{details.get('FilesystemName', details.get('FilesystemType', '?'))}, SSD={details.get('SolidState', '?')}"
    except (ValueError, TypeError, plistlib.InvalidFileException):
        storage = filesystem.strip() if len(filesystem) < 160 else "see raw filesystem evidence"
    hardware = machine.get("hardware", {})
    cpu = hardware.get("cpu_model", {}).get("stdout", "").strip()
    ram = hardware.get("physical_ram_bytes", {}).get("stdout", "").strip()
    details = f"{cpu}; {int(ram) / (1024 ** 3):.1f} GiB RAM; " if cpu and ram.isdigit() else ""
    return f"{details}{identity}; {storage}. Full machine/backend capture is in analysis.json."


def markdown(result):
    session = result["session"]
    lines = ["# Controlled HTTP comparison", "", "Exploratory local diagnostic; **not an RFC 0039 publishable benchmark record**.", "",
             f"Raw session: `{result['session_directory']}`. Full input digests and all per-repetition values are in `analysis.json`.", "",
             "## Acquisition", "", machine_label(session["machine"]), ""]
    for arm in ARMS:
        sut = session["sut"][arm]
        lines.append(f"- {arm}: source `{sut['build_receipt']['source_commit']}`; binary `{sut['binary_sha256']}`; build receipt `{sut['build_receipt_sha256']}`.")
    config, protocol = session["config"], session["protocol"]
    lines += ["", f"Toolchain: `{session['sut']['current']['build_receipt']['compiler']['rustc_vv'].splitlines()[0]}`.", "",
              f"{len(result['repetitions'])} fresh server processes; {config['query_samples']} measured serial queries and {config['export_samples']} complete exports per repetition. Warmup: 25 queries + 2 exports. Post-work idle: {protocol.get('post_idle_seconds', 5)} seconds. Loopback HTTP/1.1; timer stops after complete response bytes and before client verification. Same byte-verified fixture at one active path. OS page cache is uncontrolled.", "",
              f"Runtime environment: `{json.dumps(session['runtime_environment'], sort_keys=True)}`. Per-repetition thermal observations are retained in analysis.json; host isolation is not qualified.", "",
              "Reproduce the analyzer from the repository root:", "", "```sh",
              f"python3 scripts/analyze-http-perf.py {shlex.quote(result['session_directory'])}", "```", "",
              "Acquisition is deferred. Historical instrument sources and restoration requirements are preserved under `benchmarks/deferred/`.", "",
              "## Frozen fixture layout", "",
              "| State | Manifest version | Manifest fragments | Data fragments | Live data rows | Visible index entries |",
              "|---|---:|---:|---:|---:|---:|"]
    for state, layout in result["physical_fixtures"].items():
        tables = [item["layout"] for item in layout["tables"]]
        lines.append(f"| {state} | {layout.get('graph_manifest_version', '?')} | {layout.get('manifest', {}).get('fragments', '?')} | {sum(item.get('fragments', 0) for item in tables)} | {sum(item.get('rows', 0) for item in tables)} | {sum(len(item.get('visible_index_metadata', [])) for item in tables)} |")
    lines += ["",
              "## Fresh-process repetitions", "", "Each value is median [minimum, maximum] of repetition summaries; latency is milliseconds and RSS is MiB.", "",
              "| State | Build | Reps | Read median | Within-rep read p95 | Export median | Query RSS | Export RSS | Post-idle RSS |",
              "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for cell in result["cells"]:
        values = " | ".join(display(cell["metrics"][metric]) for metric in METRICS)
        lines.append(f"| {cell['state']} | {cell['arm']} | {cell['repetitions']} | {values} |")
    lines += ["", "## Code comparison", "", "B/A pairs use each ABBA block's geometric mean of its two current reps divided by its two baseline reps. The noise floor is the largest symmetric same-arm, same-state gap inside any block. A signal requires every paired block ratio to clear 1 + that floor + the predeclared 0.20 margin (inverse for improvement). No detected regression does not establish equality or exclude smaller regressions.", "",
              "| State | Metric | Current/baseline by block | A/A floor | Threshold | Finding |",
              "|---|---|---|---:|---:|---|"]
    for item in result["code_comparisons"]:
        if "ratio_summary" not in item:
            lines.append(f"| {item['state']} | {item['metric']} | — | — | — | {item['finding']} |")
            continue
        ratios = ", ".join(f"{ratio:.2f}×" for ratio in item["current_over_baseline_by_block"])
        lines.append(f"| {item['state']} | {item['metric']} | {ratios} | {item['aa_noise']['maximum_symmetric_relative_gap']:.1%} | {item['threshold_ratio']:.2f}× | {item['finding']} |")
    lines += ["", "## Physical-state contrasts", "", "After/before ratios are paired by block within each build. These compare state bundles; they cannot isolate fragments from accumulated history or maintenance/index changes. Their A/A floors are descriptive, not a causal-identification guarantee.", "",
              "| Build | State contrast | Metric | After/before median [min, max] | Combined A/A floor |",
              "|---|---|---|---:|---:|"]
    for item in result["physical_state_comparisons"]:
        lines.append(f"| {item['arm']} | {item['before']} → {item['after']} | {item['metric']} | {display(item['ratio_summary'])} | {item['conservative_combined_aa_floor']:.1%} |")
    lines += ["", "## Drift during each fixed-state repetition", "",
              "Each cell shows median [min, max] across processes. Query ratios compare the last request-count quarter with the first; RSS differences compare the last sampled quarter with the first within that phase. Short phases can have too few RSS samples. This is descriptive drift, not a memory-leak diagnosis.", "",
              "| State | Build | Read last/first | Query RSS last−first, MiB | Idle RSS last−first, MiB |",
              "|---|---|---:|---:|---:|"]
    for state in STATES:
        for arm in ARMS:
            reps = [rep for rep in result["repetitions"] if rep["state"] == state and rep["arm"] == arm]
            values = []
            for key, field in (("query_drift", "last_over_first"), ("query_rss_drift", "difference"),
                               ("idle_rss_drift", "difference")):
                samples = [rep[key][field] for rep in reps if rep[key] is not None]
                values.append(display(spread(samples)) if len(samples) == len(reps) else "unavailable")
            lines.append(f"| {state} | {arm} | {' | '.join(values)} |")
    lines += ["", "## Interpretation limits", ""] + [f"- {value}" for value in result["limitations"]]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    try:
        result = analyze(directory)
        (directory / "analysis.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
        (directory / "analysis.md").write_text(markdown(result))
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(f"Refused analysis: {error}", file=sys.stderr)
        return 1
    print(directory / "analysis.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
