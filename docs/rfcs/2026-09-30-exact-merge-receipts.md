---
rfc: "2026-09-30-exact-merge-receipts"
title: "Exact merge receipts"
track: maintainer
status: accepted
implementation: complete
authors:
  - OmniGraph maintainers
created: 2026-09-30
updated: 2026-10-07
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Exact merge receipts

## Decision

Implement A2 of [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md#rollout).
Carry the merge's publication result to its caller. A later history read can
return another writer's commit and cannot establish this operation's receipt.
The engine already produces the required commit; retain it instead of
discarding it. Publication, merge classification and storage formats do not change.

The engine's `branch_merge` and `branch_merge_as` return `MergeResult` containing
`outcome: MergeOutcome` and `commit: Option<GraphCommit>`. `FastForward` and
`Merged` carry the commit produced by that merge. `AlreadyUpToDate` carries no
commit and publishes nothing. The invariant holds at construction; callers do
not reconstruct or repair a receipt from HEAD, history or a source snapshot.

## Caller contract

`POST /branches/merge`, GQ `branch merge` through `POST /mutate`, and both CLI
access paths return the same `CommitOutput` in `commit`: `graph_commit_id`,
`graph_manifest_version`, `graph_branch`, `parent_commit_id`,
`merged_parent_commit_id`, `actor_id` and `created_at` (Unix microseconds).
Nullable metadata retains the publication's value. Fast-forward publishes its
own target commit with the source head as merged parent; it does not return the
source commit. `already_up_to_date` explicitly returns `commit: null`.

The dedicated CLI trims source and target names before opening storage or
submitting HTTP work, matching the engine's canonical names. Empty names refuse
before dispatch. Receipt validation and optional deletion use those same names.

Optional source deletion remains a subsequent, separately authorized effect.
When requested, `branch_deleted` is `true` on success or `false` on failure.
Failure preserves the merge result and adds `branch_delete_error_details` as
the shared `ErrorOutput`, including its typed code and details where available.
Without a deletion request, both deletion fields are absent. The old
`branch_delete_error` string is removed. Successful merge with failed optional
deletion remains HTTP 200 and CLI exit 0; it is never a reason to replay the merge.

Missing or inconsistent required receipt evidence is a protocol error, with no
history fallback or automatic replay. Lost, truncated or failed response delivery
does not prove that the merge failed. The CLI makes one submission; callers must
reconcile uncertain effects before taking another action. This slice supplies no
durable request identity, result lookup or execution ownership after disconnect.
Whole-command retry classification remains A3; server-owned writes remain B.

## Compatibility and invariants

These receipts belong to the single current contract admitted by
[HTTP contract admission](2026-09-30-v012-http-admission.md). HTTP integrations must
consume the required merge receipt and structured deletion error. Embedded Rust
callers adapt to `MergeResult`; no old-return-type wrapper or wire alias remains.
No migration, graph reset or additional publication is required.

The existing manifest publication is the sole commit authority. Receipt
construction adds no storage read, write, history scan or mutable cache. Merge
and deletion retain their respective policy checks. This changes result plumbing
over pinned Lance 11.0.0, with no new Lance behavior assumption. The full branch,
tag and versioning pages in the [Lance reading map](../dev/lance.md#branches-tags-and-cleanup)
were reviewed; native per-dataset versions remain distinct from graph commits.

## Evidence and rollout

Extend existing owners; Rust is required for receipt metadata, concurrency,
authorization, process delivery and request counts, not new GQ row semantics.

| Gate | Required evidence | Owners |
|---|---|---|
| T1 | No-op, fast-forward and three-way receipts; advance the target after A's outcome is established but before its response is constructed, then require A's exact metadata or null. Substituting the later commit must fail the assertion. | Engine `merge_fast_forward`/`branching`/`failpoints`; server `data_routes`; CLI `cli_data`/`parity_matrix` |
| T2 compound | Merge succeeds, source deletion is denied or fails: exact receipt, typed error, retained source, correct target, exit 0 and no second merge. | Server `auth_policy`; CLI `parity_matrix`/`system_remote` |
| T2 delivery | Suppress or truncate a successful response; separately return proxy 504 or expire caller wait. Prove one submission and inspect independently committed state; never infer no effect from delivery loss. | CLI `cli_data`/`system_remote` and their existing process support |
| Contract | Required receipt shape, no legacy deletion field, generated OpenAPI, existing merge and transport regressions. | API types; server `openapi`; focused engine/server/CLI suites |

Qualification on 2026-09-30 reproduced the old history-reread race before the
fix: A returned B's later commit. The corrected regression covers both HTTP
doors and all three outcomes, with a separate engine target-replacement race.
The 195 focused engine tests, API/server/CLI suites, 15 loopback process tests,
434 GQT tests, AWS-feature server suite, seam guard and both workspace Clippy
graphs passed. The eight delivery-loss cases cover both merge commands under
disconnect, truncation, proxy 504 and caller-process abandonment after upstream
success; the last case is not production request-deadline qualification. The
existing DST storage-cost golden passed unchanged, without regeneration.

The [server-testing proposal](2026-09-26-self-contained-server-testing.md#server-coverage-requirements)
retains the broader T/B matrix. This slice does not add a harness protocol or
claim throughput, memory or cloud qualification. Existing deterministic merge
cost owners remain the place to detect added publication work. Land engine,
shared API, server, CLI, generated contract and user guidance together; record
executed gates before marking implementation complete.

## Decision log

- 2026-09-30: Accepted A2 on the maintainer's instruction to build the described
  exact merge receipt slice, including null no-op receipts and structured
  optional deletion failure with exit 0. The broader server runtime RFC remains
  draft; this decision does not accept A3, request ownership or online deployment.

- 2026-09-30: Qualified canonical branch names at the dedicated CLI boundary
  while hardening the reviewed stack.

- 2026-10-07: Replaced the Compatibility sentence tying receipts to v0.12 with the current HTTP admission decision; receipt semantics are unchanged.
