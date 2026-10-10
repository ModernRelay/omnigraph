#!/usr/bin/env python3
"""Require every storage format coverage scope, with no empty or skipped runs."""

import argparse
import importlib
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ci_gate  # noqa: E402  (scripts/ci_gate.py, the CI Gate job table)

workflow_parser = importlib.import_module("check-merge-group-triggers")


ROOT = Path(__file__).resolve().parents[1]
CONTEXT = "Storage Upgrade Compatibility"
JOB = "storage_upgrade_compatibility"
FEATURES = "omnigraph-engine/failpoints,omnigraph-cluster/failpoints"
STAMP_13_SOURCE_COMMIT = "c0a4519f38d65dc1356728981983da8b096cfbef"
CASES = (
    "storage_upgrade_current_binary_reports_already_current_on_a_fresh_graph",
    "genuine_v13_storage_upgrade_preserves_history",
    "storage_upgrade_refuses_cluster_path_aliases",
    "genuine_v0_11_0_storage_upgrade_preserves_history",
    "genuine_v0_11_0_storage_upgrade_after_predecessor_cleanup",
    "genuine_v0_10_0_to_stamp_8_storage_upgrade_preserves_history",
    "genuine_v0_10_0_to_stamp_9_by_default_storage_upgrade_preserves_history",
)
ENGINE_CASES = (
    "a_fresh_graph_is_already_current_and_nothing_is_written",
    "restamped_current_layout_is_refused_as_source",
    "a_target_other_than_the_served_format_is_unsupported",
    "a_pending_conversion_with_a_valid_intent_is_reported_by_check_as_pending",
    "a_pending_conversion_with_an_unreadable_intent_is_unknown_ownership",
    "check_has_no_local_store_effects",
    "a_source_root_is_converted_once_and_is_then_current",
    "history_leftovers_refuse_before_fence",
    "over_bound_record_refuses_before_fence",
    "census_over_bound_refuses_before_reads",
    "policy_denial_precedes_effects",
    "recovery_sidecars_refuse_under_the_route_of_the_stamp",
    "a_staged_schema_object_at_the_root_refuses_before_the_contract_is_read",
    "an_unreadable_root_contract_refuses_as_unsupported_source",
    "a_store_failure_reading_the_root_contract_fails_preflight_and_a_rerun_routes",
    "a_live_ref_stamped_differently_from_main_fails_preflight",
    "the_schema_state_v0_11_0_writes_converts_with_or_without_an_unread_field",
    "stamp_8_refuses_v3_system_columns_and_stamp_9_converts_the_legacy_ones",
    "a_live_schema_apply_lock_is_retired_and_a_retired_one_converts",
    "a_schema_apply_lock_beside_staging_or_a_sidecar_stays_refused",
    "an_over_bound_root_schema_object_refuses_as_unsupported_source",
    "a_root_contract_whose_columns_the_tables_lack_refuses_before_any_write",
    "a_root_contract_that_does_not_describe_a_live_ref_refuses",
    "a_stamp_9_root_reached_from_6_by_the_released_upgrade_converts_its_retained_versions",
    "cleanup_after_upgrade_with_keep_four_and_older_than",
    "merge_with_legacy_base_pins_and_collector_keeps_it",
    "leftover_merge_input_tag_on_bookkeeping_version_resolves",
    "commit_list_and_change_feed_cross_the_upgrade",
    "numeric_snapshot_below_upgrade",
    "fork_of_deleted_branch_serves_inherited_version",
    "retired_commit_graphs_serve_legacy_heads",
    "named_and_fresh_fork_conversions_pass_equivalence",
    "fork_head_and_writer_head_release_equal_copies",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility",
    "interrupted::interruption_boundaries_retry_from_stamp_9",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock",
    "interrupted::interruption_boundaries_retry_from_stamp_8",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_between_legacy_files_1_to_3",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_between_legacy_files_4_to_5",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_after_legacy",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_after_stage_1_to_2",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_after_stage_3_to_4",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_after_branch_1_to_2",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_after_branch_3_to_4",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_before_activation",
    "interrupted::interruption_boundaries_retry_without_mixed_visibility_after_activation",
    "interrupted::interruption_boundaries_retry_from_stamp_9_between_legacy_files_1_to_3",
    "interrupted::interruption_boundaries_retry_from_stamp_9_between_legacy_files_4_to_5",
    "interrupted::interruption_boundaries_retry_from_stamp_9_after_legacy",
    "interrupted::interruption_boundaries_retry_from_stamp_9_after_stage_1_to_2",
    "interrupted::interruption_boundaries_retry_from_stamp_9_after_stage_3_to_4",
    "interrupted::interruption_boundaries_retry_from_stamp_9_after_branch_1_to_2",
    "interrupted::interruption_boundaries_retry_from_stamp_9_after_branch_3_to_4",
    "interrupted::interruption_boundaries_retry_from_stamp_9_before_activation",
    "interrupted::interruption_boundaries_retry_from_stamp_9_after_activation",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_between_legacy_files_1_to_3",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_between_legacy_files_4_to_5",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_after_legacy",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_after_stage_1_to_2",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_after_stage_3_to_4",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_after_branch_1_to_2",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_after_branch_3_to_4",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_before_activation",
    "interrupted::interruption_boundaries_retry_from_stamp_9_with_a_schema_apply_lock_after_activation",
    "interrupted::interruption_boundaries_retry_from_stamp_8_between_legacy_files_1_to_3",
    "interrupted::interruption_boundaries_retry_from_stamp_8_between_legacy_files_4_to_5",
    "interrupted::interruption_boundaries_retry_from_stamp_8_after_legacy",
    "interrupted::interruption_boundaries_retry_from_stamp_8_after_stage_1_to_2",
    "interrupted::interruption_boundaries_retry_from_stamp_8_after_stage_3_to_4",
    "interrupted::interruption_boundaries_retry_from_stamp_8_after_branch_1_to_2",
    "interrupted::interruption_boundaries_retry_from_stamp_8_after_branch_3_to_4",
    "interrupted::interruption_boundaries_retry_from_stamp_8_before_activation",
    "interrupted::interruption_boundaries_retry_from_stamp_8_after_activation",
    "interrupted::a_fenced_stamp_8_or_9_rerun_reads_the_archived_contract_not_the_root_objects",
    "interrupted::a_fenced_stamp_9_rerun_reads_the_archived_contract_not_a_widened_pg",
    "interrupted::a_fenced_stamp_9_rerun_reads_the_archived_contract_not_a_corrupt_ir",
    "interrupted::a_fenced_stamp_9_rerun_reads_the_archived_contract_not_a_staged_pg",
    "interrupted::a_fenced_locked_stamp_9_rerun_reads_the_archived_contract_not_a_staged_pg",
    "interrupted::a_fenced_stamp_8_rerun_reads_the_archived_contract_not_a_respelled_root",
    "interrupted::a_fenced_stamp_9_rerun_without_its_archived_contract_asks_for_the_backup",
    "interrupted::partial_legacy_write_is_completed_on_retry",
    "interrupted::resume_after_directory_skips_census",
    "interrupted::fenced_census_applies_source_bounds_and_directory_resume_reads_no_head",
    "interrupted::foreign_layout_version_refuses",
    "interrupted::a_source_changed_after_the_fence_refuses_as_plan_changed",
    "interrupted::modified_legacy_object_refuses_resume",
    "interrupted::unreadable_legacy_object_asks_for_a_rerun",
)
SCOPES = {
    # Every scope selects the `Test Workspace` packages with the canonical
    # failpoint features, one graph for the whole job and the same one `Test
    # Workspace` builds; a scope with its own selection resolved a third
    # graph (docs/dev/ci.md, cache rule).
    "crossversion": (
        'cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked --test crossversion_upgrade --features "$FAILPOINT_FEATURES" storage_upgrade -- --test-threads=1',
        "crates/omnigraph-cli/tests/crossversion_upgrade.rs",
        "",
    ),
    "engine": (
        'cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked --lib --features "$FAILPOINT_FEATURES" db::upgrade::tests -- --test-threads=1',
        "crates/omnigraph/src/db/upgrade/tests.rs",
        "db::upgrade::tests::",
    ),
    "lance": (
        'cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked --test lance_version_columns --features "$FAILPOINT_FEATURES" -- --test-threads=1',
        "crates/omnigraph/tests/lance_version_columns.rs",
        "",
    ),
    "protocol": (
        'cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked --test forbidden_apis --features "$FAILPOINT_FEATURES" -- --test-threads=1',
        "crates/omnigraph/tests/forbidden_apis.rs",
        "",
    ),
}
OLD_SCOPE_COMMANDS = (
    'cargo test --workspace --locked --test crossversion_upgrade --features "$FAILPOINT_FEATURES" storage_upgrade -- --test-threads=1',
    "cargo test --locked -p omnigraph-engine --lib --features failpoints db::upgrade::tests -- --test-threads=1",
    "cargo test --locked -p omnigraph-engine --test lance_version_columns --features failpoints -- --test-threads=1",
    "cargo test --locked -p omnigraph-engine --test forbidden_apis --features failpoints -- --test-threads=1",
)
PREDECESSOR_TOKENS = (
    "OMNIGRAPH_REQUIRE_STORAGE_UPGRADE_TESTS: '1'",
    f"STAMP_13_SOURCE_COMMIT: {STAMP_13_SOURCE_COMMIT}",
    'git worktree add --detach "$v13_source" "$STAMP_13_SOURCE_COMMIT"',
    'echo "OMNIGRAPH_V13_BIN=$v13_bin" >> "$GITHUB_ENV"',
)
PREDECESSOR_SCRIPT = """\
set -euo pipefail
v13_source="$RUNNER_TEMP/omnigraph-stamp-13"
v13_bin="$RUNNER_TEMP/omnigraph-stamp-13-bin"
git fetch --no-tags --depth=1 origin "$STAMP_13_SOURCE_COMMIT"
git worktree add --detach "$v13_source" "$STAMP_13_SOURCE_COMMIT"
cargo build --locked \\
  --manifest-path "$v13_source/Cargo.toml" \\
  --package omnigraph-cli \\
  --bin omnigraph \\
  --target-dir "$GITHUB_WORKSPACE/target"
cp "$GITHUB_WORKSPACE/target/debug/omnigraph" "$v13_bin"
test -x "$v13_bin"
# Clean path-package artifacts through both workspace manifests while
# retaining shared registry dependencies: the predecessor and current
# packages share names and versions, so either manifest alone can
# miss a stale path identity and link the wrong rlib.
cargo clean --workspace --locked \\
  --manifest-path "$v13_source/Cargo.toml" \\
  --target-dir "$GITHUB_WORKSPACE/target"
cargo clean --workspace --locked \\
  --target-dir "$GITHUB_WORKSPACE/target"
echo "OMNIGRAPH_V13_BIN=$v13_bin" >> "$GITHUB_ENV\""""
RELEASE_TOKENS = (
    'echo "OMNIGRAPH_V6_BIN=$v6_dir/omnigraph" >> "$GITHUB_ENV"',
    'echo "OMNIGRAPH_V011_BIN=$v011_dir/omnigraph" >> "$GITHUB_ENV"',
)
RELEASE_SCRIPT = """\
set -euo pipefail
v6_dir="$RUNNER_TEMP/omnigraph-v010"
v011_dir="$RUNNER_TEMP/omnigraph-v011"
# The installer downloads the official archive and verifies its
# SHA256 before extraction. An exact VERSION never falls back to edge.
# A failed download leaves nothing behind, so each release gets three
# attempts before this blocking job fails.
install_release() {
  for attempt in 1 2 3; do
    REPO_SLUG=ModernRelay/omnigraph VERSION="$1" INSTALL_DIR="$2" \\
      bash scripts/install.sh && return 0
    echo "install of $1 failed on attempt $attempt/3; retrying"
    sleep 10
  done
  return 1
}
install_release v0.10.0 "$v6_dir"
test -x "$v6_dir/omnigraph"
[[ "$("$v6_dir/omnigraph" --version)" == "omnigraph 0.10.0" ]] \\
  || { echo "::error::the storage upgrades of a 0.10.0 graph require the genuine v0.10.0 CLI"; exit 1; }
install_release v0.11.0 "$v011_dir"
test -x "$v011_dir/omnigraph"
[[ "$("$v011_dir/omnigraph" --version)" == "omnigraph 0.11.0" ]] \\
  || { echo "::error::the release storage upgrade requires the genuine v0.11.0 CLI"; exit 1; }
echo "OMNIGRAPH_V6_BIN=$v6_dir/omnigraph" >> "$GITHUB_ENV"
echo "OMNIGRAPH_V011_BIN=$v011_dir/omnigraph" >> "$GITHUB_ENV\""""


def scope_script(scope: str) -> str:
    command = SCOPES[scope][0]
    return (
        "set -euo pipefail\n"
        f'test_log="$RUNNER_TEMP/storage-upgrade-{scope}.log"\n'
        f'{command} 2>&1 | tee "$test_log"\n'
        f'python3 scripts/check-storage-upgrade-ci.py --check-log {scope} "$test_log"'
    )


def validate(workflow: str, table: dict | None = None) -> list[str]:
    """Failures of the job's shape in `workflow`; `table` is ci_gate.JOBS unless a test substitutes it."""
    table = ci_gate.JOBS if table is None else table
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
        "run: python3 scripts/check-storage-upgrade-ci.py --self-test",
        *PREDECESSOR_TOKENS,
        *RELEASE_TOKENS,
    ):
        if token not in job:
            failures.append(f"storage compatibility is missing {token!r}")
    scripts = re.findall(
        r"^        run: \|\n((?:^          .*\n|^\n)+)", job, re.MULTILINE
    )
    scripts = {"\n".join(line[10:] for line in body.rstrip().splitlines()) for body in scripts}
    if PREDECESSOR_SCRIPT not in scripts:
        failures.append("storage compatibility requires the exact genuine stamp-13 build script")
    if RELEASE_SCRIPT not in scripts:
        failures.append("storage compatibility requires the exact released v0.10.0 and v0.11.0 install script")
    for scope in SCOPES:
        if scope_script(scope) not in scripts:
            failures.append(f"storage compatibility requires the exact fail-closed {scope} command and log check")
    lines = workflow_parser.strip_comments(workflow)
    bounds = workflow_parser.job_bounds(lines)
    gate = workflow_parser.job_keys(lines, *bounds["ci_gate"]) if "ci_gate" in bounds else {}
    needed = set(workflow_parser.list_values(lines, *gate["needs"])) if "needs" in gate else set()
    if JOB not in needed:
        failures.append(f"CI Gate must need {JOB}")
    if table.get(JOB) != ci_gate.ALWAYS:
        failures.append(f"scripts/ci_gate.py must list {JOB} as {ci_gate.ALWAYS!r}, never skipped")
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
    # A workspace selection runs one binary per member; the owner's binary
    # executes the scope and every other binary filters to nothing.
    executed = [summary for summary in summaries if summary != ("0", "0", "0", "0")]
    if len(executed) != 1:
        failures.append("required test run must have exactly one successful summary that executed tests")
    elif executed[0] != (str(len(passed)), "0", "0", "0") or not passed:
        failures.append("required test run must execute positive coverage with no failures or ignored cases")
    return failures


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / ".github/workflows/ci.yml").read_text()

    def test_current_configuration(self):
        self.assertEqual(validate(self.workflow), [])

    def test_gate_needs_list_forms(self):
        head, separator, gate = self.workflow.partition("  ci_gate:\n")
        inline = re.search(r"^    needs: \[(.*?)\]$", gate, re.MULTILINE)
        self.assertIsNotNone(inline)
        jobs = [job.strip() for job in inline.group(1).split(",")]
        for needed in (jobs, [job for job in jobs if job != JOB]):
            for form in (
                "    needs: [" + ", ".join(needed) + "]",
                "    needs:\n" + "\n".join(f"      - '{job}' # dependency" for job in needed),
                "    needs: [\n" + "\n".join(f'      "{job}", # dependency' for job in needed) + "\n    ]",
                "    needs:\n      [" + ", ".join(needed) + "]",
                "    needs:\n      [\n" + "\n".join(f'        "{job}", # dependency' for job in needed) + "\n      ]",
            ):
                with self.subTest(needed=needed, form=form):
                    changed = head + separator + gate.replace(inline.group(0), form)
                    failures = validate(changed)
                    if JOB in needed:
                        self.assertEqual(failures, [])
                    else:
                        self.assertTrue(any(f"CI Gate must need {JOB}" in failure for failure in failures), failures)

    def test_missing_or_changed_execution_fails(self):
        for scope, (command, _, _) in SCOPES.items():
            for replacement in ("true", f"if false; then {command}; fi", command + " || true"):
                with self.subTest(scope=scope, replacement=replacement):
                    changed = self.workflow.replace(command, replacement)
                    self.assertNotEqual(changed, self.workflow)
                    self.assertTrue(validate(changed))

    def test_old_package_feature_selection_fails(self):
        changed = self.workflow.replace(
            "cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked --test crossversion_upgrade",
            "cargo test --locked -p omnigraph-cli --test crossversion_upgrade",
        )
        self.assertNotEqual(changed, self.workflow)
        self.assertTrue(validate(changed))
        for (command, _, _), old in zip(SCOPES.values(), OLD_SCOPE_COMMANDS):
            with self.subTest(old=old):
                changed = self.workflow.replace(command, old)
                self.assertNotEqual(changed, self.workflow)
                self.assertTrue(validate(changed))

    def test_conditional_job_and_steps_fail(self):
        for line in ("    if: false\n", "    needs: classify_changes\n", "    continue-on-error: true\n"):
            changed = self.workflow.replace("  storage_upgrade_compatibility:\n", "  storage_upgrade_compatibility:\n" + line)
            self.assertTrue(validate(changed))
        for line in ("        if: false\n", "        continue-on-error: true\n"):
            changed = self.workflow.replace("      - name: Run required storage upgrade engine tests\n", "      - name: Run required storage upgrade engine tests\n" + line)
            self.assertNotEqual(changed, self.workflow)
            self.assertTrue(validate(changed))

    def test_missing_log_check_and_failpoints_fail(self):
        for scope in SCOPES:
            changed = self.workflow.replace(f"--check-log {scope}", "--check-log invalid")
            self.assertTrue(validate(changed))
        self.assertTrue(validate(self.workflow.replace(FEATURES, "")))

    def test_missing_predecessor_build_or_requirement_fails(self):
        for token in PREDECESSOR_TOKENS:
            with self.subTest(token=token):
                changed = self.workflow.replace(token, "")
                self.assertNotEqual(changed, self.workflow)
                self.assertTrue(validate(changed))
        moved = self.workflow.replace(STAMP_13_SOURCE_COMMIT, "0" * 40)
        self.assertNotEqual(moved, self.workflow)
        self.assertTrue(validate(moved))

    def test_removed_or_altered_predecessor_build_line_fails(self):
        exact = ["storage compatibility requires the exact genuine stamp-13 build script"]
        lines = PREDECESSOR_SCRIPT.splitlines()
        block = "".join(f"          {line}\n" for line in lines)
        self.assertEqual(self.workflow.count(block), 1)
        for index, line in enumerate(lines):
            for replacement in ([], [f"true # {line}"], [f"{line} || true"]):
                with self.subTest(line=line, replacement=replacement):
                    altered = lines[:index] + replacement + lines[index + 1:]
                    changed = self.workflow.replace(
                        block, "".join(f"          {line}\n" for line in altered)
                    )
                    failures = validate(changed)
                    self.assertTrue(set(exact) <= set(failures), failures)
        swapped = self.workflow.replace("--package omnigraph-cli", "--package omnigraph-server")
        self.assertNotEqual(swapped, self.workflow)
        self.assertEqual(validate(swapped), exact)

    def storage_job(self):
        start = self.workflow.index("  storage_upgrade_compatibility:\n")
        end = self.workflow.index("\n  v5_v10_format_fence:\n", start)
        return self.workflow[start:end]

    def test_missing_release_binary_export_fails(self):
        job = self.storage_job()
        for token in RELEASE_TOKENS:
            with self.subTest(token=token):
                self.assertEqual(job.count(token), 1)
                changed = self.workflow.replace(job, job.replace(token, ""))
                failures = validate(changed)
                self.assertIn(f"storage compatibility is missing {token!r}", failures)

    def test_removed_or_altered_release_install_line_fails(self):
        exact = ["storage compatibility requires the exact released v0.10.0 and v0.11.0 install script"]
        job = self.storage_job()
        lines = RELEASE_SCRIPT.splitlines()
        block = "".join(f"          {line}\n" for line in lines)
        self.assertEqual(job.count(block), 1)
        for index, line in enumerate(lines):
            for replacement in ([], [f"true # {line}"], [f"{line} || true"]):
                with self.subTest(line=line, replacement=replacement):
                    altered = lines[:index] + replacement + lines[index + 1:]
                    changed = self.workflow.replace(job, job.replace(
                        block, "".join(f"          {line}\n" for line in altered)
                    ))
                    failures = validate(changed)
                    self.assertTrue(set(exact) <= set(failures), failures)
        for before, after in (
            ("install_release v0.11.0", "install_release v0.12.0"),
            ("for attempt in 1 2 3", "for attempt in 1"),
            ("bash scripts/install.sh && return 0", "bash scripts/install.sh; return 0"),
        ):
            with self.subTest(before=before, after=after):
                moved = self.workflow.replace(job, job.replace(before, after))
                self.assertNotEqual(moved, self.workflow)
                self.assertEqual(validate(moved), exact)

    def test_gate_must_need_the_job_and_never_let_it_skip(self):
        self.assertEqual(validate(self.workflow), [])
        self.assertIn(f"scripts/ci_gate.py must list {JOB} as 'always', never skipped", validate(self.workflow, {}))
        self.assertIn(f"scripts/ci_gate.py must list {JOB} as 'always', never skipped", validate(self.workflow, {**ci_gate.JOBS, JOB: ci_gate.FULL_CI}))
        for dropped in (f"{JOB}, ", f", {JOB}"):
            with self.subTest(dropped=dropped):
                changed = re.sub(rf"^(    needs: \[.*?){re.escape(dropped)}(.*?\])$", r"\1\2", self.workflow, count=1, flags=re.MULTILINE)
                self.assertNotEqual(changed, self.workflow)
                self.assertIn(f"CI Gate must need {JOB}", validate(changed))
        self.assertIn(f"CI Gate must need {JOB}", validate(self.workflow.partition("  ci_gate:\n")[0]))

    def test_expected_cases_name_the_genuine_journey_and_every_engine_case(self):
        crossversion = expected_cases("crossversion")
        for name in CASES:
            self.assertIn(name, crossversion)
        engine = expected_cases("engine")
        for name in ENGINE_CASES:
            self.assertIn("db::upgrade::tests::" + name, engine)

    def test_every_named_case_exists_in_its_source(self):
        # A case is an `fn` or a `name: …;` row of the `retry_journeys!` macro.
        for scope, cases in (("crossversion", CASES), ("engine", ENGINE_CASES)):
            source = (ROOT / SCOPES[scope][1]).read_text()
            for name in cases:
                with self.subTest(name=name):
                    bare = name.rsplit("::", 1)[-1]
                    self.assertRegex(source, rf"(?m)\bfn {bare}\(|^\s+{bare}: ", msg=name)

    def test_log_requires_every_case_and_positive_unskipped_summary(self):
        good = "test alpha ... ok\ntest beta ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out; finished in 1s\n"
        expected = {"alpha", "beta"}
        self.assertEqual(validate_log(good, expected), [])
        filtered = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0s\n"
        self.assertEqual(validate_log(filtered + good + filtered, expected), [])
        for log in (
            "", good.replace("test beta ... ok\n", ""),
            good + filtered.replace("0 ignored", "1 ignored"),
            good.replace("test beta ... ok", "test beta ... ignored"),
            good.replace("2 passed", "0 passed"),
            good.replace("0 ignored", "1 ignored"),
            good + "skipping genuine stamp-13 storage upgrade: OMNIGRAPH_V13_BIN is unset\n",
            good + "skipping genuine v0.11.0 storage upgrade: OMNIGRAPH_V011_BIN is unset\n",
            good + "skipping genuine v0.10.0 storage upgrade: OMNIGRAPH_V6_BIN is unset\n",
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
        failures = validate((ROOT / ".github/workflows/ci.yml").read_text())
    if failures:
        for failure in failures:
            print(f"Storage upgrade CI: {failure}", file=sys.stderr)
        return 1
    print("Storage upgrade CI OK (genuine stamp-13 route, genuine v0.11.0 and v0.10.0 release route, cluster refusal, engine protocol, Lance and registry coverage).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
