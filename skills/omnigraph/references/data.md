# Data Changes & Branches

## Contents
- Choose the right write command
- `mutate` — single edits
- `load` — bulk JSONL (`--mode`, `--from`)
- Branches: review before merge
- Destructive ops go through a branch
- Branch commands
- Inspecting state after changes

How to modify data safely in Omnigraph.

## Choose the Right Write Command

`load` is the one bulk-JSONL command — local **or** remote, against any
existing branch, with a **required** `--mode`. `mutate` is for single typed
edits.

| Task | Command | Why |
|------|---------|-----|
| Add/update a single entity | `mutate` with a named mutation | typechecked, parameterized, auditable |
| Bulk upsert by `@key` | `load --mode merge` | preserves rows not in the file |
| Additive-only bulk | `load --mode append` | fails on key collision |
| Clean-slate reseed | `load --mode overwrite` | **destructive** — wipes the branch |
| Bulk load onto a fresh review branch | `load --from main --mode merge --branch <name>` | forks `<name>` from `main`, loads onto it, leaves it for review |

> **`--mode` is required** — there is no default. Overwrite is destructive, so
> the CLI never picks a mode for you.
>
> **Local and remote are one command.** `load` works against a local repo URI
> (writing storage directly) *and* a remote `omnigraph-server` endpoint (the
> server orchestrates the write and publishes one atomic commit). See
> [`references/remote-ops.md`](remote-ops.md) for remote-specific concerns
> (504 handling, write-verification ritual).

## `mutate` — Single Edits

Goes through the running server (the configured default graph, or an alias):

```bash
omnigraph mutate add_signal \
  --query mutations.gq \
  --params '{"slug":"sig-foo","name":"Foo","brief":"...","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}'
```

Or via an alias:

```bash
omnigraph alias add-signal sig-foo "Foo" "..." 2026-04-14T00:00:00Z 2026-04-14T00:00:00Z 2026-04-14T00:00:00Z
```

Prefer `mutate` for interactive edits, mutations called from agents, and anything you want typechecked at call time.

## `load` — Bulk JSONL

JSONL format:

```jsonl
{"type":"Signal","data":{"id":"sig-foo","slug":"sig-foo","name":"Foo","brief":"...","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}}
{"edge":"FormsPattern","from":"sig-foo","to":"pat-bar","data":{}}
```

- Nodes: `{"type":"<NodeType>","data":{...props...}}` — `id` equals `slug`
- Edges: `{"edge":"<EdgeType>","from":"<src_slug>","to":"<dst_slug>","data":{...edge_props...}}`

Load command:

```bash
omnigraph load --data seed.jsonl --mode merge s3://my-bucket/repos/spike-intel
```

`--from <base>` forks a missing `--branch` from `<base>` before loading (the
one-shot review-branch flow below). Without `--from`, the target `--branch`
(default `main`) must already exist.

### `--mode` semantics

- **`overwrite`** (destructive) — replaces every node/edge table on the branch with the file's contents. **Staged**: the loader validates node/edge constraints, referential integrity, and edge cardinality *before* any data moves, so a bad file fails before touching the branch. Safe on a **first** load; risky afterward. Don't run it against `main` in production without a branch backup path.
- **`merge`** (upsert) — for each row, insert if `@key` is new, update if it exists. Rows not in the file are preserved. The safe default for incremental bulk updates.
- **`append`** (strict insert) — fails on key collision. Use when you're certain every row is new.

### `merge` does NOT recompute embeddings

If you change seed rows that feed into `@embed("source")` via `load --mode merge`, the source field updates but the embedding stays stale.

**Fix:** run `omnigraph embed --reembed_all` after, or use `load --mode overwrite` once (which re-triggers embedding on load).

### `overwrite` is destructive

Wipes the entire branch's data for every node and edge type. Use only for:
- First-time seed
- Intentional full reseed on a feature branch
- Recovery scenarios

Never on `main` without a branch backup.

## Branches: Review Before Merge

Branches exist for **data review**, not schema changes. Schema goes straight to `main` via `plan` + `apply`.

### The review loop

```bash
REPO=s3://my-bucket/repos/spike-intel

# 1. Create feature branch from main
omnigraph branch create --from main staging-2026-04-14 --store $REPO

# 2. Load delta onto the branch (merge mode is typical for review)
omnigraph load --data delta.jsonl --branch staging-2026-04-14 --mode merge $REPO

# 3. Verify on the branch (reads can target --branch or --snapshot)
omnigraph query recent_signals --query queries/signals.gq --branch staging-2026-04-14 --store $REPO

# 4. Merge to main when happy
omnigraph branch merge staging-2026-04-14 --into main --store $REPO

# 5. Optionally delete the branch
omnigraph branch delete staging-2026-04-14 --store $REPO
```

### Fork a branch in one shot with `--from`

- Bare `load` operates on an existing branch (default `main`).
- `load --from main --branch <name>` forks `<name>` from `main`, loads onto it, and leaves it for review — the whole review-branch flow in one command.

Use `--from` for anything you want reviewed before it touches `main`.

### Keep branches short-lived

Long-lived branches compound merge risk. The usual flow is: create → load → verify → merge → delete, all in the same session. A week-old feature branch is a yellow flag.

### Schema apply blocks non-main branches

`omnigraph schema apply` rejects the request if any non-main branches exist. Merge or delete them first. This is enforced — it's not just a guideline.

## Destructive Ops Go Through a Branch

For any bulk load that could disrupt downstream queries (overwriting a heavily-referenced node type, removing edges en masse, reseeding a core table), use a feature branch:

```bash
omnigraph load --data risky.jsonl --branch recovery-2026-04-14 \
  --from main --mode overwrite $REPO
# inspect, diff, verify reads
omnigraph branch merge recovery-2026-04-14 --into main --store $REPO
```

## Branch Commands (quick reference)

```bash
omnigraph branch create --from main <branch-name> --store $REPO
omnigraph branch list --store $REPO
omnigraph branch merge <branch-name> --into main --store $REPO
omnigraph branch delete <branch-name> --store $REPO
```

All support `--json` for automation-friendly output. Address the graph with a
positional `file://`/`s3://` URI (shown), `--store <uri>`, or `--server <name>`.

## Inspecting State After Changes

```bash
omnigraph snapshot $REPO --branch main --json           # tables + row counts
omnigraph export $REPO --branch main > graph.jsonl      # full JSONL dump
omnigraph commit list $REPO --branch main --json        # history
```

`export` is the right tool for large-snapshot inspection — don't try to page through the whole graph with read queries.

> **Cluster note:** everything in this file applies unchanged in cluster
> deployments — the control plane owns schema/queries/policies; rows, loads,
> and branches stay on the data plane against the derived graph roots
> (`<dir>/graphs/<id>.omni`, or `<storage>/graphs/<id>.omni` for an S3-backed
> cluster).
