#!/usr/bin/env python3
"""Keep automatic public releases behind the exact-SHA vocabulary audit."""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / ".github" / "workflows"
JOB_RE = re.compile(r"^  ([a-zA-Z0-9_]+):\s*$", re.MULTILINE)


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
        "release_edge_after_vocabulary": "./.github/workflows/release-edge.yml",
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
        "release-edge.yml": ("prepare_edge_release", "build_release"),
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
        if filename == "release-edge.yml":
            require(
                "git ls-remote origin refs/heads/main" in text,
                "release-edge.yml: stale audits must not move the rolling edge tag",
                failures,
            )
        elif filename == "release.yml":
            require(
                "name: release-${{ matrix.asset_name }}" in text
                and "pattern: release-omnigraph-*" in text
                and "Verify exact release artifact set" in text,
                "release.yml: publication must select and verify only release artifacts",
                failures,
            )
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

    if failures:
        print("Release vocabulary gate violations:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print("Release vocabulary gates OK (4 automatic publishers, exact-SHA source).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
