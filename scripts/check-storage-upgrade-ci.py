#!/usr/bin/env python3
"""Require every storage migration coverage scope, with no empty or skipped runs."""

import argparse
import json
import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTEXT = "Storage Upgrade Compatibility"
FEATURES = "omnigraph-engine/failpoints,omnigraph-cluster/failpoints"
CASES = (
    "genuine_v09_explicit_storage_upgrade_preserves_history",
    "genuine_v010_explicit_storage_upgrade_preserves_history",
    "storage_upgrade_refuses_cluster_path_aliases",
    "genuine_v09_storage_upgrade_refuses_ambiguous_branch_names",
)
ENGINE_CASES = (
    "storage_upgrade_check_has_no_local_store_effects",
    "storage_upgrade_interruption_boundaries_retry_without_mixed_visibility",
    "storage_upgrade_recovery_refuses_foreign_head_movement",
    "storage_upgrade_tracks_metadata_writes_and_no_payload_effects",
    "storage_upgrade_policy_denial_precedes_effects",
    "storage_upgrade_refuses_unknown_ownership_and_source",
    "storage_upgrade_refuses_preexisting_recovery_without_healing",
    "storage_upgrade_current_main_refuses_legacy_branch_without_effects",
    "storage_upgrade_history_budget_precedes_manifest_reads",
)
SCOPES = {
    "crossversion": (
        'cargo test --workspace --locked --test crossversion_upgrade --features "$FAILPOINT_FEATURES" storage_upgrade -- --test-threads=1',
        "crates/omnigraph-cli/tests/crossversion_upgrade.rs",
        "",
    ),
    "engine": (
        "cargo test --locked -p omnigraph-engine --lib --features failpoints db::manifest::upgrade::tests -- --test-threads=1",
        "crates/omnigraph/src/db/manifest/upgrade/tests.rs",
        "db::manifest::upgrade::tests::",
    ),
    "lance": (
        "cargo test --locked -p omnigraph-engine --test lance_version_columns --features failpoints -- --test-threads=1",
        "crates/omnigraph/tests/lance_version_columns.rs",
        "",
    ),
    "protocol": (
        "cargo test --locked -p omnigraph-engine --test forbidden_apis --features failpoints -- --test-threads=1",
        "crates/omnigraph/tests/forbidden_apis.rs",
        "",
    ),
}


def scope_script(scope: str) -> str:
    command = SCOPES[scope][0]
    return (
        "set -euo pipefail\n"
        f'test_log="$RUNNER_TEMP/storage-upgrade-{scope}.log"\n'
        f'{command} 2>&1 | tee "$test_log"\n'
        f'python3 scripts/check-storage-upgrade-ci.py --check-log {scope} "$test_log"'
    )


def validate(workflow: str, policy: dict) -> list[str]:
    failures = []
    match = re.search(
        r"^  storage_upgrade_compatibility:\n(.*?)(?=^  \w+:|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        return ["CI must define storage_upgrade_compatibility"]
    job = match.group(1)
    trigger = workflow.split("\njobs:\n", 1)[0]
    if re.search(r"^\s+(?:paths|paths-ignore):", trigger, re.MULTILINE):
        failures.append("CI storage compatibility must not filter changed paths")
    for event in ("pull_request", "push"):
        if not re.search(rf"^  {event}:", trigger, re.MULTILINE):
            failures.append(f"CI must run storage compatibility on {event}")
    if re.search(r"^    (?:if|needs|continue-on-error):", job, re.MULTILINE):
        failures.append("storage compatibility must run unconditionally and fail closed")
    if re.search(r"^      (?:- |  )(?:if|continue-on-error):", job, re.MULTILINE):
        failures.append("storage compatibility steps must not skip or ignore failures")
    if not re.search(rf"^  FAILPOINT_FEATURES: {re.escape(FEATURES)}$", trigger, re.MULTILINE):
        failures.append("storage compatibility requires the canonical workspace failpoint features")
    for token in (
        f"name: {CONTEXT}",
        "OMNIGRAPH_REQUIRE_STORAGE_UPGRADE_TESTS: '1'",
        "VERSION=v0.9.0",
        "VERSION=v0.10.0",
        "OMNIGRAPH_V09_BIN=",
        "OMNIGRAPH_V6_BIN=",
        "bash scripts/install.sh",
        "run: python3 scripts/check-storage-upgrade-ci.py --self-test",
    ):
        if token not in job:
            failures.append(f"storage compatibility is missing {token!r}")
    scripts = re.findall(
        r"^        run: \|\n((?:^          .*\n|^\n)+)", job, re.MULTILINE
    )
    scripts = {"\n".join(line[10:] for line in body.rstrip().splitlines()) for body in scripts}
    for scope in SCOPES:
        if scope_script(scope) not in scripts:
            failures.append(f"storage compatibility requires the exact fail-closed {scope} command and log check")
    contexts = policy.get("required_status_checks", {}).get("contexts", [])
    if CONTEXT not in contexts:
        failures.append(f"branch protection must require {CONTEXT}")
    return failures


def expected_cases(scope: str, root: Path = ROOT) -> set[str]:
    _, source, prefix = SCOPES[scope]
    names = set(re.findall(
        r"^#\[(?:tokio::)?test[^\n]*\]\n(?:#\[[^\n]*\]\n)*(?:async )?fn (\w+)\(",
        (root / source).read_text(), re.MULTILINE,
    ))
    if scope == "crossversion":
        names = {name for name in names if "storage_upgrade" in name} | set(CASES)
    elif scope == "engine":
        names |= set(ENGINE_CASES)
    return {prefix + name for name in names}


def validate_log(log: str, expected: set[str]) -> list[str]:
    failures = []
    log = re.sub(r"\x1b\[[0-9;]*m", "", log)
    if not expected:
        failures.append("required test inventory is empty")
    if re.search(r"\b(?:skipping|skipped)\b", log, re.IGNORECASE):
        failures.append("required storage upgrade coverage skipped")
    passed = set(re.findall(r"^test ([\w:]+) \.\.\. ok$", log, re.MULTILINE))
    for name in sorted(expected - passed):
        failures.append(f"required storage upgrade case {name} did not pass")
    summaries = re.findall(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; \d+ filtered out;", log, re.MULTILINE
    )
    if len(summaries) != 1:
        failures.append("required test run must have exactly one successful summary")
    elif summaries[0] != (str(len(passed)), "0", "0", "0") or not passed:
        failures.append("required test run must execute positive coverage with no failures or ignored cases")
    return failures


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.policy = json.loads((ROOT / ".github/branch-protection.json").read_text())

    def test_current_configuration(self):
        self.assertEqual(validate(self.workflow, self.policy), [])

    def test_missing_or_changed_execution_fails(self):
        for scope, (command, _, _) in SCOPES.items():
            for replacement in ("true", f"if false; then {command}; fi", command + " || true"):
                with self.subTest(scope=scope, replacement=replacement):
                    changed = self.workflow.replace(command, replacement)
                    self.assertNotEqual(changed, self.workflow)
                    self.assertTrue(validate(changed, self.policy))

    def test_old_package_feature_selection_fails(self):
        changed = self.workflow.replace(
            "cargo test --workspace --locked --test crossversion_upgrade",
            "cargo test --locked -p omnigraph-cli --test crossversion_upgrade",
        )
        self.assertTrue(validate(changed, self.policy))

    def test_conditional_job_and_steps_fail(self):
        for line in ("    if: false\n", "    needs: classify_changes\n", "    continue-on-error: true\n"):
            changed = self.workflow.replace("  storage_upgrade_compatibility:\n", "  storage_upgrade_compatibility:\n" + line)
            self.assertTrue(validate(changed, self.policy))
        for line in ("        if: false\n", "        continue-on-error: true\n"):
            changed = self.workflow.replace("      - name: Run required storage upgrade engine tests\n", "      - name: Run required storage upgrade engine tests\n" + line)
            self.assertNotEqual(changed, self.workflow)
            self.assertTrue(validate(changed, self.policy))

    def test_missing_log_check_and_failpoints_fail(self):
        for scope in SCOPES:
            changed = self.workflow.replace(f"--check-log {scope}", "--check-log invalid")
            self.assertTrue(validate(changed, self.policy))
        self.assertTrue(validate(self.workflow.replace(FEATURES, ""), self.policy))

    def test_missing_required_context_fails(self):
        self.assertTrue(validate(self.workflow, {}))

    def test_log_requires_every_case_and_positive_unskipped_summary(self):
        good = "test alpha ... ok\ntest beta ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out; finished in 1s\n"
        expected = {"alpha", "beta"}
        self.assertEqual(validate_log(good, expected), [])
        for log in (
            "", good.replace("test beta ... ok\n", ""),
            good.replace("test beta ... ok", "test beta ... ignored"),
            good.replace("2 passed", "0 passed"),
            good.replace("0 ignored", "1 ignored"),
            good + "skipping explicit storage upgrade: missing predecessor\n",
            good + good,
        ):
            with self.subTest(log=log):
                self.assertTrue(validate_log(log, expected))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--check-log", nargs=2, metavar=("SCOPE", "PATH"))
    args = parser.parse_args()
    if args.self_test:
        result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(GuardTests))
        return 0 if result.wasSuccessful() else 1
    if args.check_log:
        scope, path = args.check_log
        if scope not in SCOPES:
            parser.error(f"unknown test scope: {scope}")
        failures = validate_log(Path(path).read_text(), expected_cases(scope))
    else:
        failures = validate(
            (ROOT / ".github/workflows/ci.yml").read_text(),
            json.loads((ROOT / ".github/branch-protection.json").read_text()),
        )
    if failures:
        for failure in failures:
            print(f"Storage upgrade CI: {failure}", file=sys.stderr)
        return 1
    print("Storage upgrade CI OK (required predecessors, refusal, recovery, Lance and protocol coverage).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
