# Transactions in OmniGraph

OmniGraph does not have `BEGIN` / `COMMIT` / `ROLLBACK`. Branches do that job. This page explains the model, when to use which primitive, and shows worked examples for the patterns that come up most.

The architectural rule lives in [`docs/invariants.md`](invariants.md) §VI.23:

> **Atomicity is per-query.** Every `.gq` query is atomic via the substrate's atomic-commit primitive. **No cross-query `BEGIN`/`COMMIT`; branches and merges fill that role for agent workflows.**

If you need to coordinate multiple queries atomically, you fork a branch, run mutations on it, and merge when you're satisfied. If something goes wrong, you delete the branch.

## The atomicity model

Two primitives, two scopes:

| Scope | Primitive | Atomic? | Failure mode |
|---|---|---|---|
| **One `.gq` query** (any number of statements inside) | The query itself — handled by the publisher's atomic manifest commit | Yes — all statements land together or none of them do | The publisher never publishes; target unchanged |
| **Many queries that must succeed together** | Branches: `branch_create` → run N queries on the branch → `branch_merge` | Yes — the merge is a single atomic publish | Drop the branch (`branch_delete`); main is unaffected |

Snapshot isolation is per-query — every read inside one query sees one consistent manifest version. Two concurrent queries on the same branch see independent snapshots; the publisher's CAS catches racing writes.

## Comparison with `BEGIN` / `COMMIT`

| Postgres / MySQL | OmniGraph |
|---|---|
| `BEGIN; … ; COMMIT` | `branch_create review/X` → mutations on `review/X` → `branch_merge review/X --into main` |
| `ROLLBACK` | `branch_delete review/X` |
| Connection-bound session state | Branch-scoped lineage on disk |
| Locks (or MVCC + abort on conflict) | Snapshot isolation per query + three-way merge at branch-join |
| Transaction is invisible to ops | Branch is a durable artifact (visible in `branch_list`, queryable, time-travelable) |

The trade-off: branches are heavier than a connection-scoped transaction (they exist on disk, have a name, show up in `branch_list`), but they fit the agent-as-user model — agents naturally fork branches to plan, batch, and review work. And they're durable: if your process crashes mid-workflow, the branch survives and you can pick up where you left off.

## Worked examples

### 1. Single query, multi-statement (atomic by default)

A `.gq` query with multiple `insert` / `update` statements is one transaction. Either all statements land together at publish time, or none do.

```gq
query register_employee_with_team($name: String, $age: I32, $team: String) {
    insert Person { name: $name, age: $age }
    insert WorksAt { from: $name, to: $team }
}
```

```bash
omnigraph change --query ./mutations.gq --name register_employee_with_team \
    --params '{"name":"Alice","age":30,"team":"Acme"}' ./repo.omni
```

If the second statement fails (e.g. `Acme` doesn't exist), the publisher never publishes; `Alice` is not in the database. Atomic.

### 2. Two separate queries on `main` (NOT atomic)

```bash
# Query 1
omnigraph change --query ./mutations.gq --name register_employee --params '{"name":"Alice","age":30}' ./repo.omni

# Query 2 — runs after Query 1 has already published
omnigraph change --query ./mutations.gq --name link_to_team --params '{"name":"Alice","team":"Acme"}' ./repo.omni
```

These are **two publishes** on `main`. If Query 2 fails, Query 1's effects are already visible. There is no `ROLLBACK` for Query 1.

If you want both-or-neither, you have two options:
- Combine them into a single `.gq` query (option 1 above), or
- Use a branch (option 3 below).

### 3. Many queries, atomic via a branch

The pattern when you need to run multiple queries — possibly across multiple commands, agents, or sessions — and have them succeed or fail as a unit.

```bash
# Fork a working branch from main.
omnigraph branch create --from main onboarding/2026-04-25 ./repo.omni

# Run any number of mutations on the branch — each one is its own publish on the branch.
# Concurrent reads of `main` are unaffected.
omnigraph change --branch onboarding/2026-04-25 \
    --query ./mutations.gq --name register_employee \
    --params '{"name":"Alice","age":30}' ./repo.omni

omnigraph change --branch onboarding/2026-04-25 \
    --query ./mutations.gq --name register_employee \
    --params '{"name":"Bob","age":25}' ./repo.omni

omnigraph change --branch onboarding/2026-04-25 \
    --query ./mutations.gq --name link_to_team \
    --params '{"name":"Alice","team":"Acme"}' ./repo.omni

# Inspect the branch — read queries work just like on main.
omnigraph read --branch onboarding/2026-04-25 \
    --query ./queries.gq --name list_employees ./repo.omni

# Happy with what's on the branch? Merge it. This is one atomic publish:
# `main` flips to include every commit on the branch.
omnigraph branch merge onboarding/2026-04-25 --into main ./repo.omni

# OR: not happy? Throw it away. `main` is untouched.
# omnigraph branch delete onboarding/2026-04-25 ./repo.omni
```

Properties:
- Each query on the branch is its own publisher commit — so they're individually atomic. Per-query CAS works on branches just like on main.
- The branch lives on disk. Process crash mid-workflow? Re-open and resume.
- Multiple agents can work on different branches in parallel without blocking each other.
- The merge is a three-way merge at the row level. Conflicts surface as `OmniError::MergeConflicts(Vec<MergeConflict>)`, with structured kinds (`DivergentInsert`, `DivergentUpdate`, `DeleteVsUpdate`, …) so callers can handle them programmatically.

### 4. Coordinating multiple agents

Two agents writing to the same graph independently:

```bash
# Agent A
omnigraph branch create --from main agent-a/work ./repo.omni
omnigraph change --branch agent-a/work … ./repo.omni
# … many mutations …
omnigraph branch merge agent-a/work --into main ./repo.omni

# Agent B (running concurrently)
omnigraph branch create --from main agent-b/work ./repo.omni
omnigraph change --branch agent-b/work … ./repo.omni
# … many mutations …
omnigraph branch merge agent-b/work --into main ./repo.omni
```

Each agent sees a consistent snapshot of `main` at the time it forked. The first merge to `main` lands as a fast-forward (or a no-op if no concurrent change). The second merge runs three-way: rows touched by both branches surface as `MergeConflict`s for the caller to resolve.

This is the workflow MR-797 / agentic loops are designed around: **branches are the unit of "an agent's working set."**

## Failure modes

| Scenario | What happens | Caller action |
|---|---|---|
| Single query fails mid-flight | Publisher never publishes; target unchanged | Read the error, decide whether to retry |
| Concurrent writers race the same `(table, branch)` | Publisher CAS rejects the loser with `ManifestConflictDetails::ExpectedVersionMismatch` | Refresh handle, retry the query |
| Branch with N successful mutations, then merge fails (three-way conflict) | Each individual mutation already committed on the branch; merge surfaces `MergeConflicts` | Inspect, decide whether to keep working on the branch, abandon it (`branch_delete`), or resolve and re-merge |
| Process crashes mid-branch-workflow | Each completed mutation on the branch is durable | Re-open the repo, continue where you left off |

## When to use what

| Intent | Use |
|---|---|
| One conceptual change, multiple statements | One `.gq` query with multiple statements |
| Bulk import of a related set of records | One `omnigraph load` (the loader is one atomic query under the hood) |
| Many independent changes, no coordination needed | Many separate queries on `main`. Each is its own atomic unit. |
| "Do these N things, all together or not at all" | Branch → run N queries → merge |
| "Try things, evaluate, then commit" | Branch → mutate → read/inspect → merge or delete |
| "Multiple agents writing concurrently" | One branch per agent, merge to `main` at end of agent task |
| "Long-running workflow that may span sessions or process restarts" | Branch (durable on disk) |

## What this model can't do

- **Cross-query atomicity on `main` without a branch.** If you don't want to fork a branch, multiple queries on `main` publish independently. There is no implicit transaction.
- **Long-running interactive transactions.** No `BEGIN` over a connection. Branches are the durable equivalent.
- **Cross-graph (cross-repo) transactions.** Each repo is its own atomicity domain.
- **"Pessimistic" locks** that serialize writers before they reach the storage layer. Snapshot-MVCC + publisher CAS handles concurrency optimistically; the loser retries.

## See also

- [`docs/branches-commits.md`](branches-commits.md) — branch and commit-graph mechanics.
- [`docs/merge.md`](merge.md) — three-way merge details and conflict kinds.
- [`docs/query-language.md`](query-language.md) — `.gq` syntax for the multi-statement queries used above.
- [`docs/runs.md`](runs.md) — the per-query commit pipeline that gives single-query atomicity.
- [`docs/invariants.md`](invariants.md) §VI.23 — the architectural rule.
