#!/usr/bin/env python3
"""Keep automatic public releases behind the exact-SHA vocabulary audit."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / ".github" / "workflows"
JOB_RE = re.compile(r"^  ([a-zA-Z0-9_]+):\s*$", re.MULTILINE)
# Publishers of the platform archives, built once by `build-release-archives.yml`.
ARCHIVE_PUBLISHERS = ("release-edge.yml", "release.yml")
# Publishers that attest what they publish; the attestation names the run's
# commit, so each must refuse to attest a build of another one.
ATTESTING_PUBLISHERS = ("release-edge.yml", "release.yml", "publish-image.yml")
# Publishers that run on a schedule, auditing the commit they select, instead
# of being called by the CI run that audited it.
SCHEDULED_PUBLISHERS = ("release-edge.yml",)


def job_block(text: str, name: str) -> str:
    jobs = text.split("\njobs:\n", 1)
    if len(jobs) != 2:
        raise ValueError("workflow has no jobs mapping")
    body = jobs[1]
    matches = list(JOB_RE.finditer(body))
    for index, match in enumerate(matches):
        if match.group(1) != name:
            continue
        end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        return body[match.start() : end]
    raise ValueError(f"missing job {name}")


def require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def main() -> int:
    failures: list[str] = []
    ci = (WORKFLOW_ROOT / "ci.yml").read_text()

    graph = job_block(ci, "graph_vocabulary_guard")
    require(
        "if: github.event_name != 'pull_request'" in graph,
        "ci.yml: Graph Vocabulary Guard must remain a job-level PR skip",
        failures,
    )

    automatic_calls = {
        "release_tag_after_vocabulary": "./.github/workflows/release.yml",
        "publish_tag_image_after_vocabulary": "./.github/workflows/publish-image.yml",
        "publish_tag_crates_after_vocabulary": "./.github/workflows/publish-crates.yml",
    }
    for job, target in automatic_calls.items():
        block = job_block(ci, job)
        require(
            "graph_vocabulary_guard" in block,
            f"ci.yml:{job}: must need Graph Vocabulary Guard",
            failures,
        )
        require(
            f"uses: {target}" in block,
            f"ci.yml:{job}: must call {target}",
            failures,
        )
        require(
            "ci_run_id:" in block and "source_ref:" in block,
            f"ci.yml:{job}: must pass exact CI-run and source identity",
            failures,
        )

    publishers = {
        "release-edge.yml": ("build_release", "publish_edge_release"),
        "release.yml": ("build_release", "publish_release", "update_homebrew_tap"),
        "publish-image.yml": ("publish_image",),
        "publish-crates.yml": ("publish_crates",),
    }
    for filename, writer_jobs in publishers.items():
        text = (WORKFLOW_ROOT / filename).read_text()
        trigger = text.split("\njobs:\n", 1)[0]
        require(
            "\n  push:" not in trigger,
            f"{filename}: direct push publication bypasses the vocabulary gate",
            failures,
        )
        if filename in SCHEDULED_PUBLISHERS:
            require(
                "  schedule:" in trigger
                and "  workflow_dispatch:" in trigger
                and "  workflow_call:" not in trigger,
                f"{filename}: must run on its schedule and by manual dispatch only",
                failures,
            )
        else:
            require(
                "  workflow_call:" in trigger and "  workflow_dispatch:" in trigger,
                f"{filename}: must support audited CI calls and manual backfills",
                failures,
            )
        gate = job_block(text, "vocabulary_gate")
        require(
            "uses: ./.github/workflows/release-vocabulary-gate.yml" in gate,
            f"{filename}: must call the shared exact-SHA gate",
            failures,
        )
        require(
            "actions: read" in gate and "contents: read" in gate,
            f"{filename}: nested gate call needs Actions and contents read permissions",
            failures,
        )
        require(
            "needs.vocabulary_gate.outputs.source_sha" in text,
            f"{filename}: release source must use the audited SHA",
            failures,
        )
        if filename in ARCHIVE_PUBLISHERS:
            require(
                "uses: ./.github/workflows/build-release-archives.yml" in text
                and "pattern: release-omnigraph-*" in text,
                f"{filename}: publication must select only the shared build's release artifacts",
                failures,
            )
        if filename in ATTESTING_PUBLISHERS:
            require(
                '"$GITHUB_SHA" == "$SOURCE_SHA"' in text
                and "uses: actions/attest-build-provenance@" in text,
                f"{filename}: provenance must name the built commit, the run's own",
                failures,
            )
        if filename == "release-edge.yml":
            require(
                "git ls-remote origin refs/heads/main" in text
                and 'git merge-base --is-ancestor "$SOURCE_SHA" "$current_main"' in text,
                "release-edge.yml: only an audited commit on main may be published",
                failures,
            )
            require(
                "\nconcurrency:\n  group: release-edge\n  cancel-in-progress: false\n" in text,
                "release-edge.yml: runs must not overlap between publication and the rolling-channel checks",
                failures,
            )
            gate_call = job_block(text, "vocabulary_gate")
            require(
                "source_ref: ${{ github.sha }}" in gate_call and "wait_minutes:" in gate_call,
                "release-edge.yml: a scheduled build audits its own commit, waiting for its CI run",
                failures,
            )
            require(
                "require_ci_gate: true" in gate_call,
                "release-edge.yml: a pinnable edge build needs its commit's whole CI to pass",
                failures,
            )
            require(
                "superseded_ok: true" in gate_call
                and "needs.vocabulary_gate.outputs.superseded != 'true'" in job_block(text, "build_release"),
                "release-edge.yml: a superseded commit must end the run without a build",
                failures,
            )
            require(
                "scripts/edge_release.py check-version" in job_block(text, "prepare_edge_release"),
                "release-edge.yml: an edge version must sort after the latest stable release",
                failures,
            )
            require(
                'git merge-base --is-ancestor "$current" "$SOURCE_SHA"' in text,
                "release-edge.yml: the rolling edge tag must only move forward",
                failures,
            )
        elif filename == "release.yml":
            require(
                '"refs/tags/${RELEASE_TAG}" "refs/tags/${RELEASE_TAG}^{}"' in text,
                "release.yml: tag must be revalidated before publication",
                failures,
            )
        else:
            require(
                '"refs/tags/${RELEASE_TAG}" "refs/tags/${RELEASE_TAG}^{}"' in text,
                f"{filename}: tag must be revalidated before publication",
                failures,
            )
        for writer_job in writer_jobs:
            block = job_block(text, writer_job)
            require(
                "vocabulary_gate" in block,
                f"{filename}:{writer_job}: writer must need the vocabulary gate",
                failures,
            )

    build = (WORKFLOW_ROOT / "build-release-archives.yml").read_text()
    build_trigger = build.split("\njobs:\n", 1)[0]
    require(
        "  workflow_call:" in build_trigger
        and "\n  push:" not in build_trigger
        and "workflow_dispatch" not in build_trigger,
        "build-release-archives.yml: archives are built only for an audited publisher",
        failures,
    )
    require(
        "contents: write" not in build,
        "build-release-archives.yml: the build must not write; its caller publishes",
        failures,
    )
    require(
        "name: release-${{ matrix.asset_name }}" in build
        and "Verify exact release artifact set" in build,
        "build-release-archives.yml: archives must be uploaded and verified as one exact set",
        failures,
    )

    gate = (WORKFLOW_ROOT / "release-vocabulary-gate.yml").read_text()
    require(
        '.conclusion == "success"' in gate,
        "release-vocabulary-gate.yml: only a successful audit may authorize release",
        failures,
    )
    require(
        '.event == "push" or .event == "workflow_dispatch"' in gate,
        "release-vocabulary-gate.yml: pull-request checks must not authorize release",
        failures,
    )
    require(
        "deadline=$(( $(date +%s) + WAIT_MINUTES * 60 ))" in gate,
        "release-vocabulary-gate.yml: waiting for an audit must be bounded",
        failures,
    )
    require(
        'passed("CI Gate")' in gate,
        "release-vocabulary-gate.yml: require_ci_gate must check the same run's CI Gate",
        failures,
    )
    require(
        'all(.status == "completed" and .conclusion == "cancelled")' in gate
        and '"$current_main" != "$SOURCE_SHA"' in gate,
        "release-vocabulary-gate.yml: only a cancelled commit that main moved past is superseded",
        failures,
    )

    # Tags before v0.14.0 predate the stamp; the cutoff is written in Python in
    # release.yml and in bash in publish-image.yml, so the two must agree.
    release_cutoff = re.search(r">= \((\d+), (\d+), (\d+)\)", (WORKFLOW_ROOT / "release.yml").read_text())
    image_cutoff = re.search(r"\(\( version >= (\d+) \)\)", (WORKFLOW_ROOT / "publish-image.yml").read_text())
    require(
        release_cutoff is not None
        and image_cutoff is not None
        and int(release_cutoff.group(1)) * 1_000_000 + int(release_cutoff.group(2)) * 1_000 + int(release_cutoff.group(3))
        == int(image_cutoff.group(1)),
        "release.yml and publish-image.yml must share one release-attestation cutoff",
        failures,
    )

    if failures:
        print("Release vocabulary gate violations:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print("Release vocabulary gates OK (3 CI-called and 1 scheduled publisher, exact-SHA source).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
