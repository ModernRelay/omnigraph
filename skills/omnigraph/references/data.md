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
| Bulk upsert by logical entity ID | `load --mode merge` | preserves rows not in the file; keyed node IDs derive from `@key` |
| Additive-only bulk | `load --mode append` | fails on key collision |
| Replace complete batches by type | `load --mode overwrite` | **destructive for represented types**; absent types remain |
| Bulk load onto a fresh review branch | `load --from main --mode merge --branch <name>` | forks `<name>` from `main`, loads onto it, leaves it for review |

> **`--mode` is required** — there is no default. Overwrite is destructive, so
> the CLI never picks a mode for you.

> **Per-load bounds.** One keyed load (`append`/`merge`)
> stages at most **8,192 entities and 32 MiB of Arrow memory per touched type**; a larger
> batch is refused up front (HTTP 413, typed `resource_limit`) with no durable
> effect — split it into chunks, each an atomic graph commit. `overwrite`
> escapes the row ceiling but not the 32 MiB strict-input Arrow preflight
> (`strict_input_arrow_bytes`), so a bulk replacement above ~32 MiB is one
> `overwrite` chunk followed by `merge` chunks. Also: against a non-local
> target, `--mode overwrite` (like `cleanup` and `branch delete`) requires
> explicit `--yes` consent in non-interactive runs.
>
> **Direct and served are one command.** `load` works against a graph store
> (writing storage directly) *and* an `omnigraph-server` endpoint (the
> server orchestrates the write and publishes one atomic commit). See
> [`references/remote-ops.md`](remote-ops.md) for remote-specific concerns
> (504 handling, write-verification ritual).

## `mutate` — Single Edits

Runs either directly (`--store`) or through a server (`--server`/profile):

```bash
omnigraph mutate add_signal \
  --query mutations.gq \
  --params '{"slug":"sig-foo","name":"Foo","brief":"...","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}'
```

Or invoke a served stored mutation by name:

```bash
omnigraph mutate add_signal --server intel-dev --graph spike \
  --params '{"slug":"sig-foo","name":"Foo","brief":"...","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}'
```

Prefer `mutate` for interactive edits, mutations called from agents, and anything you want typechecked at call time.

## `load` — Bulk JSONL

JSONL format:

```jsonl
{"type":"Signal","data":{"slug":"sig-foo","name":"Foo","brief":"...","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}}
{"edge":"FormsPattern","from":"sig-foo","to":"pat-bar","data":{}}
```

- Nodes: `{"type":"<NodeType>","data":{...props...}}`. A keyed node derives
  `id` from its complete typed key tuple; omit `id` in hand-authored keyed
  input. An unkeyed node gets a generated id unless one is supplied.
- Edges: `{"edge":"<EdgeType>","from":"<src_id>","to":"<dst_id>","data":{...edge_props...}}`.
  Edges also use generated or supplied ids.

Load command:

```bash
omnigraph load --data seed.jsonl --mode merge s3://my-bucket/repos/spike-intel
```

`--from <base>` forks a missing `--branch` from `<base>` before loading (the
one-shot review-branch flow below). Without `--from`, the target `--branch`
(default `main`) must already exist.

### `--mode` semantics

- **`overwrite`** (destructive by represented type) — replaces each node or
  edge type present in the batch; types absent from the batch stay unchanged.
  The loader validates constraints and referential integrity before publication.
  Use a review branch for an established graph.
- **`merge`** (upsert) — inserts or updates each row by logical entity `id`
  (derived from the typed `@key` tuple for keyed nodes). Rows not in the file
  are preserved. The safe default for incremental bulk updates.
- **`append`** (strict insert) — fails on entity-ID collision. Use when you're
  certain every row is new.

With `--json`, an effectful `mutate` or `load` returns the exact published
`commit`; a no-op mutation returns `commit: null`. For compare-and-swap writes,
feeds, and diff inspection, see [`changes.md`](changes.md).

### Embeddings are explicit input

`@embed` does not populate vectors during `merge` or `overwrite`. Supply
vectors in the JSONL, or run the offline `omnigraph embed` file transformation
and load its output. See [`search.md`](search.md).

### `overwrite` is scoped but destructive

It removes existing entities of every type represented in the batch. Use it
for a complete type replacement, preferably on a review branch. Do not assume
that it clears unrelated types or the whole branch.

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

# 4. Merge to main and delete the source only after publication
omnigraph branch merge staging-2026-04-14 --into main --delete-branch --store $REPO
```

### Fork a branch in one shot with `--from`

- Bare `load` operates on an existing branch (default `main`).
- `load --from main --branch <name>` forks `<name>` from `main`, loads onto it, and leaves it for review — the whole review-branch flow in one command.

Use `--from` for anything you want reviewed before it touches `main`.

### Keep branches short-lived

Long-lived branches compound merge risk. The usual flow is: create → load →
verify → `merge --delete-branch`, all in the same session. Source deletion only
happens after a successful merge publication.

### Schema apply blocks non-main branches

`omnigraph schema apply` rejects the request if any non-main branches exist. Merge or delete them first. This is enforced — it's not just a guideline.

## Destructive Ops Go Through a Branch

For any bulk load that could disrupt downstream queries (overwriting a
heavily-referenced node type, removing edges en masse, or reseeding a core
type), use a feature branch:

```bash
omnigraph load --data risky.jsonl --branch recovery-2026-04-14 \
  --from main --mode overwrite $REPO
# inspect, diff, verify reads
omnigraph branch merge recovery-2026-04-14 --into main --delete-branch --store $REPO
```

## Branch Commands (quick reference)

```bash
omnigraph branch create --from main <branch-name> --store $REPO
omnigraph branch list --store $REPO
omnigraph branch merge <branch-name> --into main --delete-branch --store $REPO
omnigraph branch delete <branch-name> --store $REPO
```

All support `--json` for automation-friendly output. Address the graph with a
positional `file://`/`s3://`/preview `az://` URI (shown), `--store <uri>`, or
`--server <name>`.

## Inspecting State After Changes

```bash
omnigraph snapshot $REPO --branch main --json           # node/edge entity counts
omnigraph export $REPO --branch main > graph.jsonl      # full JSONL dump
omnigraph commit list $REPO --branch main --json        # history
```

`export` is the right tool for large-snapshot inspection — don't try to page through the whole graph with read queries.

> **Cluster note:** everything in this file applies unchanged in cluster
> deployments — the control plane owns schema/queries/policies; rows, loads,
> and branches stay on the data plane against the derived graph roots
> (`<dir>/graphs/<id>.omni`, or `<storage>/graphs/<id>.omni` for an S3-backed
> cluster).
