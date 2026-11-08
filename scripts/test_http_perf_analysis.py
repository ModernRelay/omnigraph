#!/usr/bin/env python3
"""Synthetic admission and decision-rule checks for the HTTP diagnostic analyzer."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("analysis", Path(__file__).with_name("analyze-http-perf.py"))
analysis = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analysis)


def write(directory, name, value):
    (directory / name).write_text(json.dumps(value))


def fixture(directory):
    receipt = {"profile": "release", "binary_sha256": "a" * 64, "source_clean": True,
               "source_commit": "a" * 40, "source_tree": "b" * 40,
               "compiler": {"rustc_vv": "synthetic"}, "build_command": "synthetic release build",
               "cargo_lock_sha256": "c" * 64, "cargo_config_sha256": "d" * 64,
               "rustflags": "", "cargo_encoded_rustflags": None, "features": [],
               "default_features": True, "profile_config": {"opt_level": 2}}
    attestation = {"binary_sha256": "a" * 64, "build_receipt_sha256": "e" * 64, "build_receipt": receipt}
    session = {"format": analysis.FORMAT, "claim_eligible": False,
               "config": {"abba_blocks": 3, "query_samples": 20, "export_samples": 8},
               "protocol": {"margin_fraction": 0.20, "comparison_rule": analysis.COMPARISON_RULE,
                            "process": "fresh per repetition",
                            "warmup": {"queries": 25, "exports": 2}, "timing": "verification excluded"},
               "machine": {"kind": "synthetic"}, "fixture_cli": attestation,
               "runtime_environment": {"inheritance": "cleared for fixture CLI and both server arms"},
               "sut": {arm: attestation for arm in analysis.ARMS}}
    write(directory, "session.json", session)
    write(directory, "complete.json", {"status": "complete", "repetitions": 36,
          "final_sut_attestation": {**session["sut"], "fixture_cli": attestation}})
    write(directory, "logical-fixture.json", {"logical_export_sha256": "f" * 64, "records": 2})
    layout = {"tables": [{"type_key": "synthetic", "layout": {"fragments": 1}}]}
    for state in analysis.STATES:
        write(directory, f"fixture-{state}.json", {"state": state, "active_path": "/synthetic",
              "physical": {"sha256": "0" * 64}, "layout": layout})
    for order in range(36):
        block, offset = divmod(order, 12)
        state_index, position = divmod(offset, 4)
        state = analysis.STATES[(state_index + block) % 3]
        arm = ("baseline", "current", "current", "baseline")[position]
        write(directory, f"sample-{order:03}-{state}-{arm}.json", {
            "format": analysis.FORMAT, "claim_eligible": False, "status": "complete", "error": None,
            "order": order, "block": block, "position_in_block": position, "state": state, "arm": arm,
            "binary_sha256": "a" * 64, "fixture_physical_sha256": "0" * 64,
            "post_fixture_physical_sha256": "0" * 64, "physical_verified": True,
            "logical_export_sha256": "f" * 64, "layout": layout,
            "raw_samples": {"validated_query_count": 20, "validated_export_count": 8,
                            "query_us": [1000 if arm == "baseline" else 1400] * 20,
                            "export_us": [2000] * 8, "export_bytes": [99] * 8,
                            "rss": [[i * 100, phase, 100000] for i, phase in enumerate([1, 2, 2, 3, 4, 4])],
                            "post_idle_rss_kib": 100000}})


def retention_fixture(directory):
    source, destination = directory / "source", directory / "retention"
    source.mkdir()
    destination.mkdir()
    fixture(source)
    source_session = analysis.load(source / "session.json")
    descriptor = analysis.load(source / "fixture-fragmented.json")
    session = {"format": "omnigraph-http-fixed-retention-v1", "claim_eligible": False,
               "config": {"fixture_session": str(source), "repetitions": 1, "seconds": 10},
               "protocol": {"full_duration": False, "full_replication": False,
                            "readers": 2, "writes": 0, "idle_seconds": 10},
               "sut": source_session["sut"]["current"],
               "source_session_sha256": analysis.digest(source / "session.json"),
               "fixture_descriptor_sha256": analysis.digest(source / "fixture-fragmented.json"),
               "runtime_environment": source_session["runtime_environment"],
               "fixture_physical_sha256": "0" * 64, "logical_export_sha256": "f" * 64,
               "layout": descriptor["layout"]}
    write(destination, "session.json", session)
    write(destination, "complete.json", {"status": "complete", "repetitions": 1,
          "final_sut_attestation": session["sut"]})
    reader = {"offered": 25, "completed": 25, "refused": 0, "abandoned": 0,
              "failed": 0, "errors": [],
              "samples": [[1000 + i * 400, 1001 + i * 400, 1000] for i in range(25)]}
    streams = {"offered": 6, "completed": 4, "refused": 0, "abandoned": 2,
               "failed": 0, "errors": [], "successful_modes": {key: 1 for key in retention.MODES},
               "samples": [[1000 + i * 300, 1001 + i * 300, 1000] for i in range(6)],
               "body_windows_ms": [[1000 + i * 300, 1001 + i * 300] for i in range(6)],
               "completed_body_bytes": 999}
    write(destination, "retention-0.json", {
        "format": session["format"], "claim_eligible": False, "status": "complete", "error": None,
        "repetition": 0, "physical_verified": True, "fixture_physical_sha256": "0" * 64,
        "post_fixture_physical_sha256": "0" * 64, "logical_export_sha256": "f" * 64,
        "readers": [reader, reader], "streams": streams, "load_window_ms": [1000, 11000],
        "rss": [[0, 1, 99999]] + [[1000 + i * 250, 2, 100000 + i] for i in range(40)]
                + [[11000 + i * 250, 5, 100100] for i in range(40)],
        "post_idle_rss_kib": 100100, "recovery_reads": [[11000 + i, 1000] for i in range(25)],
        "final_export_slot_refusals": 0, "final_export_elapsed_ms": 100})
    return destination


retention_spec = importlib.util.spec_from_file_location("retention", Path(__file__).with_name("analyze-http-retention.py"))
retention = importlib.util.module_from_spec(retention_spec)
retention_spec.loader.exec_module(retention)


class RetentionTests(unittest.TestCase):
    def test_complete_control_and_descriptive_bins(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = retention_fixture(Path(temporary))
            result = retention.analyze(directory)
            self.assertFalse(result["claim_eligible"])
            rep = result["repetitions"][0]
            self.assertEqual(sum(row["reads"] for row in rep["quarters"]), 50)
            self.assertEqual(sum(row["reads"] for row in rep["minute_bins"]), 50)
            self.assertEqual(rep["first_last_read_ratio"], 1)
            self.assertIn("smoke acquisition only", retention.markdown(result))

    def test_incomplete_mode_and_body_census_refused(self):
        for field, value in (("successful_modes", {}), ("body_windows_ms", []), ("failed", 1)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                directory = retention_fixture(Path(temporary))
                record = analysis.load(directory / "retention-0.json")
                record["streams"][field] = value
                write(directory, "retention-0.json", record)
                with self.assertRaises(ValueError):
                    retention.analyze(directory)


class AnalysisTests(unittest.TestCase):
    def test_valid_complete_matrix_and_repetition_unit(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            fixture(directory)
            result = analysis.analyze(directory)
            self.assertEqual(len(result["repetitions"]), 36)
            self.assertTrue(all(cell["repetitions"] == 6 for cell in result["cells"]))
            read = next(item for item in result["code_comparisons"] if item["metric"] == "query_median_ms")
            self.assertEqual(read["finding"], "provisional local regression signal")
            self.assertIn("not an RFC 0039", analysis.markdown(result))
            self.assertFalse(result["claim_eligible"])

    def test_incomplete_wrong_or_unvalidated_records_refused(self):
        corruptions = [("order", 1), ("arm", "current"), ("binary_sha256", "1" * 64),
                       ("logical_export_sha256", "1" * 64), ("status", "failed"),
                       ("physical_verified", False), ("post_fixture_physical_sha256", "1" * 64)]
        for key, value in corruptions + [("raw_samples", {})]:
            with self.subTest(field=key), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                fixture(directory)
                path = next(directory.glob("sample-000-*.json"))
                record = analysis.load(path)
                record[key] = value
                write(directory, path.name, record)
                with self.assertRaises((ValueError, KeyError)):
                    analysis.analyze(directory)

    def test_noise_floor_and_all_blocks_required(self):
        reps = []
        for block in range(3):
            for arm, values in (("baseline", [1, 1.5]), ("current", [1.4, 1.4])):
                reps += [{"block": block, "arm": arm, "metric": value} for value in values]
        result = analysis.compare(reps, "metric", 0.20, 3)
        self.assertAlmostEqual(result["threshold_ratio"], 1.70)
        self.assertTrue(result["finding"].startswith("no detected"))
        for rep in reps:
            rep["metric"] = 1 if rep["arm"] == "baseline" else (1.4 if rep["block"] < 2 else 1.1)
        self.assertTrue(analysis.compare(reps, "metric", 0.20, 3)["finding"].startswith("no detected"))


if __name__ == "__main__":
    unittest.main()
