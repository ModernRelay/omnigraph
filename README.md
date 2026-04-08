# Omnigraph

Omnigraph is a Lance-native typed property graph database.

Active workspace crates:

- `crates/omnigraph-compiler`: shared schema/query parser, typechecker, catalog, and IR lowering
- `crates/omnigraph`: Omnigraph storage/runtime, branching, merge, change detection, and query execution
- `crates/omnigraph-cli`: CLI for repo init/load/branch/snapshot commands

## CLI

```bash
# Initialize a repo from a schema
cargo run -p omnigraph-cli -- init --schema ./schema.pg ./demo.omni

# Load JSONL data into the default branch (local repos only)
cargo run -p omnigraph-cli -- load --data ./data.jsonl ./demo.omni

# Load JSONL data into a specific branch (local repos only)
cargo run -p omnigraph-cli -- load --branch feature-x --data ./data.jsonl ./demo.omni

# Control local load mode explicitly
cargo run -p omnigraph-cli -- load --mode merge --data ./data.jsonl ./demo.omni

# Emit machine-readable local load output
cargo run -p omnigraph-cli -- load --data ./data.jsonl --json ./demo.omni

# Create or reuse a reviewable branch and ingest JSONL into it
cargo run -p omnigraph-cli -- ingest --data ./data.jsonl --branch ingest/customer-sync ./demo.omni

# Auto-create the ingest branch from an explicit base branch
cargo run -p omnigraph-cli -- ingest --data ./data.jsonl --branch ingest/customer-sync --from main ./demo.omni

# Remote ingest sends inline JSONL to omnigraph-server (v1 limit: 32 MiB)
cargo run -p omnigraph-cli -- ingest --config ./omnigraph.yaml --data ./data.jsonl --branch ingest/customer-sync --json

# Create a branch
cargo run -p omnigraph-cli -- branch create --uri ./demo.omni --from main feature-x

# List branches
cargo run -p omnigraph-cli -- branch list --uri ./demo.omni

# Merge a branch into main
cargo run -p omnigraph-cli -- branch merge --uri ./demo.omni feature-x

# Merge into an explicit target branch
cargo run -p omnigraph-cli -- branch merge --uri ./demo.omni feature-x --into experiment

# Show branch snapshot
cargo run -p omnigraph-cli -- snapshot ./demo.omni --branch feature-x --json
```

## Repo Layout

After `init`, a repo directory contains:

```text
<repo>/
  _schema.pg
  __manifest/
  nodes/{hash}/
  edges/{hash}/
  _graph_commits.lance/        # appears once branch history/merge metadata is initialized
  _graph_runs.lance/           # appears once transactional runs are initialized
```

`__manifest` is the graph publish boundary. Omnigraph branches are Lance
branches on `__manifest`, and sub-table dataset branches are created lazily on
first branch-local write.

`load` remains the lower-level local bulk loader. `ingest` is the reviewable
bulk-ingest path: it writes into a normal named branch so the result can be
inspected with `snapshot`, `read`, and `export` before a later explicit merge.

## Merge Semantics

Current branch merge behavior is conservative by design:

- Omnigraph uses merge-base-aware three-way merge over graph tables keyed by persisted `id`
- source-only changes are adopted, target-only changes are kept
- if both branches make the same final row change, the merge succeeds without conflict
- if both branches change the same row differently, Omnigraph raises a typed conflict instead of guessing
- the merged candidate graph is re-validated for orphan edges, uniqueness, cardinality, and value constraints before publish

`branch merge` returns one of `already_up_to_date`, `fast_forward`, or `merged`.
Smarter behavior like property-wise auto-merge or manual conflict resolution is intentionally deferred.

## Test Fixtures

Example datasets in `crates/omnigraph/tests/fixtures/`:

- `test.pg` / `test.jsonl` / `test.gq` — basic Person/Company graph
- `signals.pg` / `signals.jsonl` — AI industry signals
- `search.pg` / `search.jsonl` / `search.gq` — text/vector/BM25/RRF search fixture

## lance-explore

Companion Python CLI for inspecting the graph-visible pinned Lance datasets in a
repo. Lives at `../lance-explore`.

It uses `omnigraph snapshot --json` as the source of truth for graph state, then
opens the concrete tables at the pinned `table_version` / `table_branch` from
that snapshot.

```bash
cd ../lance-explore
poetry run lance-explore ls <repo>
poetry run lance-explore info <repo> <type>
poetry run lance-explore show <repo> <type>
poetry run lance-explore node <repo> <slug>
```
