#!/usr/bin/env python3
"""The verdict of the `CI Gate` job: every job that blocks `main` reported as it must.

GitHub counts a skipped job as a passed required check, so a rule listing the
blocking jobs one by one admits a job that never ran, and the list itself
lives on a settings page that nothing reviews. `ci.yml`'s `ci_gate` job needs
every blocking job, runs with `if: always()`, and hands this script their
results (`NEEDS_JSON`, the `needs` context as JSON), the event (`EVENT_NAME`,
`EVENT_REF`) and the classifier's `run_full_ci` output (`RUN_FULL_CI`). `JOBS`
mirrors each job's own `if:` condition: a `skipped` result is accepted exactly
when that condition would have been false on this event, `success` is
accepted, `failure` and `cancelled` are refused whether or not the job could
have skipped, and a result set that is not exactly the table's jobs, an
unsupported event, an empty ref, a push to anything but `main` or a `v*` tag,
or a classifier output other than `true`/`false` is refused.
`scripts/check-merge-group-triggers.py` holds the gate's `needs:` list equal to
`JOBS` and the gate's wiring to the shape `ci.yml` carries. Run from the
repository root; `--self-test` runs the in-memory cases and exits.
"""

from __future__ import annotations

import json
import os
import sys

ALWAYS = "always"
FULL_CI = "full_ci"
PR_QUEUE_FULL_CI = "pr_queue_full_ci"
PR_QUEUE_MAIN_PUSH_FULL_CI = "pr_queue_main_push_full_ci"
AUDIT_EVENTS = "audit_events"

# Job id → when the job runs, read off its `if:` in ci.yml. ALWAYS: no condition.
JOBS = {
    "classify_changes": ALWAYS,
    "check_agents_md": ALWAYS,
    "workflow_action_pins": ALWAYS,
    "cargo_deny": ALWAYS,
    "graph_vocabulary_guard": AUDIT_EVENTS,
    "fmt": PR_QUEUE_FULL_CI,
    "lint": PR_QUEUE_MAIN_PUSH_FULL_CI,
    "test": FULL_CI,
    "storage_upgrade_compatibility": ALWAYS,
    "test_aws_feature": ALWAYS,
}
EVENTS = ("pull_request", "merge_group", "push", "workflow_dispatch")
RESULTS = ("success", "failure", "cancelled", "skipped")


def expected(rule: str, event: str, ref: str, run_full_ci: str) -> bool:
    """Whether a job under `rule` runs on this event, per its `if:` in ci.yml."""
    gated = event in ("pull_request", "merge_group")
    full = run_full_ci == "true"
    if rule == ALWAYS:
        return True
    if rule == FULL_CI:
        return full
    if rule == PR_QUEUE_FULL_CI:
        return gated and full
    if rule == PR_QUEUE_MAIN_PUSH_FULL_CI:
        return (gated or (event == "push" and ref == "refs/heads/main")) and full
    if rule == AUDIT_EVENTS:
        return not gated
    raise ValueError(f"unknown rule {rule!r}")


def judge(needs_json: str, event: str, ref: str, run_full_ci: str) -> list[str]:
    """Reasons the gate is red; empty when every blocking job reported as it must."""
    failures: list[str] = []
    if event not in EVENTS:
        failures.append(f"unsupported event {event!r}")
    if not ref:
        failures.append("empty event ref")
    elif event == "push" and ref != "refs/heads/main" and not ref.startswith("refs/tags/v"):
        failures.append(f"push to {ref!r} is neither main nor a version tag")
    try:
        needs = json.loads(needs_json)
    except ValueError as error:
        return [*failures, f"needs is not JSON: {error}"]
    if not isinstance(needs, dict):
        return [*failures, "needs is not a JSON object"]
    for job in sorted(set(JOBS) - set(needs)):
        failures.append(f"{job}: not in needs")
    for job in sorted(set(needs) - set(JOBS)):
        failures.append(f"{job}: in needs but not in the gate table")
    results: dict[str, str] = {}
    for job in JOBS:
        if job not in needs:
            continue
        entry = needs[job]
        result = entry.get("result") if isinstance(entry, dict) else None
        if result not in RESULTS:
            failures.append(f"{job}: unknown result {result!r}")
        else:
            results[job] = result
    if results.get("classify_changes") != "success":
        failures.append("classify_changes: did not succeed, so run_full_ci is unknown")
    if run_full_ci not in ("true", "false"):
        failures.append(f"run_full_ci is {run_full_ci!r}, not 'true' or 'false'")
    if failures:
        return failures
    for job, result in results.items():
        if result in ("failure", "cancelled"):
            failures.append(f"{job}: {result}")
        elif result == "skipped" and expected(JOBS[job], event, ref, run_full_ci):
            failures.append(f"{job}: skipped on {event} with run_full_ci={run_full_ci}, where it must run")
    return failures


def self_test() -> None:
    def results(**over: str) -> str:
        base = {job: "success" for job in JOBS}
        base.update(over)
        return json.dumps({job: {"result": result} for job, result in base.items()})

    def ok(needs: str, event: str, ref: str, full: str) -> None:
        verdict = judge(needs, event, ref, full)
        assert verdict == [], (event, ref, full, verdict)

    def red(fragment: str, needs: str, event: str, ref: str, full: str) -> None:
        verdict = judge(needs, event, ref, full)
        assert any(fragment in line for line in verdict), (fragment, event, ref, full, verdict)

    pr = ("pull_request", "refs/pull/1/merge")
    queue = ("merge_group", "refs/heads/gh-readonly-queue/main/pr-1-0123456789abcdef")
    main = ("push", "refs/heads/main")
    tag = ("push", "refs/tags/v1.2.3")
    dispatch = ("workflow_dispatch", "refs/heads/feature")
    docs = {"graph_vocabulary_guard": "skipped", "fmt": "skipped", "lint": "skipped", "test": "skipped"}

    ok(results(graph_vocabulary_guard="skipped"), *pr, "true")
    ok(results(), *pr, "true")
    ok(results(**docs), *pr, "false")
    ok(results(graph_vocabulary_guard="skipped"), *queue, "true")
    ok(results(**docs), *queue, "false")
    ok(results(fmt="skipped"), *main, "true")
    ok(results(fmt="skipped", lint="skipped", test="skipped"), *main, "false")
    ok(results(fmt="skipped", lint="skipped"), *tag, "true")
    ok(results(fmt="skipped", lint="skipped"), *dispatch, "true")
    ok(results(fmt="skipped", lint="skipped"), ("workflow_dispatch", "refs/tags/v9.9.9")[0], "refs/tags/v9.9.9", "true")

    red("test: skipped", results(graph_vocabulary_guard="skipped", test="skipped"), *pr, "true")
    red("fmt: skipped", results(graph_vocabulary_guard="skipped", fmt="skipped"), *queue, "true")
    red("lint: skipped", results(graph_vocabulary_guard="skipped", lint="skipped"), *pr, "true")
    red("lint: skipped", results(fmt="skipped", lint="skipped"), *main, "true")
    red("graph_vocabulary_guard: skipped", results(fmt="skipped", graph_vocabulary_guard="skipped"), *main, "true")
    red("graph_vocabulary_guard: skipped", results(fmt="skipped", lint="skipped", graph_vocabulary_guard="skipped"), *tag, "true")
    for job, rule in JOBS.items():
        if rule == ALWAYS and job != "classify_changes":
            red(f"{job}: skipped", results(graph_vocabulary_guard="skipped", **{job: "skipped"}), *pr, "true")
            red(f"{job}: skipped", results(**docs, **{job: "skipped"}), *pr, "false")
    red("classify_changes: did not succeed", results(classify_changes="skipped"), *pr, "")
    red("classify_changes: did not succeed", results(classify_changes="failure"), *pr, "")

    for job in JOBS:
        for result in ("failure", "cancelled"):
            fragment = "classify_changes: did not succeed" if job == "classify_changes" else f"{job}: {result}"
            red(fragment, results(**{**docs, job: result}), *pr, "false")
            red(fragment, results(**{**docs, job: result}), *queue, "false")
            red(fragment, results(**{"graph_vocabulary_guard": "skipped", job: result}), *pr, "true")

    missing = json.loads(results(graph_vocabulary_guard="skipped"))
    del missing["test"]
    red("test: not in needs", json.dumps(missing), *pr, "true")
    extra = json.loads(results(graph_vocabulary_guard="skipped"))
    extra["azurite_integration"] = {"result": "success"}
    red("azurite_integration: in needs but not in the gate table", json.dumps(extra), *pr, "true")
    red("needs is not JSON", "{", *pr, "true")
    red("needs is not a JSON object", "[]", *pr, "true")
    red("needs is not a JSON object", "null", *pr, "true")
    odd = json.loads(results(graph_vocabulary_guard="skipped"))
    odd["test"] = {"result": "neutral"}
    red("test: unknown result 'neutral'", json.dumps(odd), *pr, "true")
    odd["test"] = "success"
    red("test: unknown result None", json.dumps(odd), *pr, "true")
    red("run_full_ci is 'maybe'", results(graph_vocabulary_guard="skipped"), *pr, "maybe")
    red("run_full_ci is ''", results(graph_vocabulary_guard="skipped"), *pr, "")
    red("unsupported event 'schedule'", results(), "schedule", "refs/heads/main", "true")
    red("empty event ref", results(graph_vocabulary_guard="skipped"), "pull_request", "", "true")
    red("neither main nor a version tag", results(fmt="skipped", lint="skipped"), "push", "refs/heads/feature", "true")
    red("neither main nor a version tag", results(fmt="skipped", lint="skipped"), "push", "refs/tags/nightly", "true")
    assert expected(FULL_CI, "pull_request", "refs/pull/1/merge", "true")
    assert not expected(PR_QUEUE_MAIN_PUSH_FULL_CI, "push", "refs/tags/v1.0.0", "true")
    try:
        expected("other", *pr, "true")
    except ValueError:
        pass
    else:
        raise AssertionError("unknown rule accepted")
    print("self-test ok")


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        self_test()
        return 0
    needs_json = os.environ.get("NEEDS_JSON", "")
    event = os.environ.get("EVENT_NAME", "")
    ref = os.environ.get("EVENT_REF", "")
    run_full_ci = os.environ.get("RUN_FULL_CI", "")
    failures = judge(needs_json, event, ref, run_full_ci)
    for failure in failures:
        print(f"::error::CI Gate: {failure}")
    if failures:
        return 1
    print(f"ok: {len(JOBS)} blocking jobs reported as required on {event} ({ref}, run_full_ci={run_full_ci})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
