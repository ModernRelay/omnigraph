# Omnigraph

Bootstrap repository for the Omnigraph branching prototype.

This repo intentionally starts small:

- `crates/omnigraph-engine`: copied Nanograph engine code as the execution baseline
- `crates/omnigraph-repo`: minimal repo/branch/snapshot metadata layer
- `crates/omnigraph-cli`: small CLI for repo bootstrap commands

## CLI

```bash
# Initialize a repo from a schema
cargo run -p omnigraph-cli -- init --repo ./demo.omni --schema ./schema.pg

# Load JSONL data into the default branch
cargo run -p omnigraph-cli -- load --repo ./demo.omni --data ./data.jsonl

# Load JSONL data into a specific branch
cargo run -p omnigraph-cli -- load --repo ./demo.omni --branch feature-x --data ./data.jsonl

# Create a branch
cargo run -p omnigraph-cli -- branch create --repo ./demo.omni --from main feature-x

# Show branch snapshot
cargo run -p omnigraph-cli -- snapshot --repo ./demo.omni --branch feature-x --json
```

## Repo Layout

After `init` and `load`, a repo directory contains:

```
_repo.json                    # repo metadata (format version, default branch)
_schema.pg                    # schema source
_refs/branches/main.json      # branch head (snapshot id, schema hash, datasets)
_db/
  graph.manifest.json          # maps type names to Lance dataset paths
  schema.pg                    # engine copy of schema
  schema.ir.json               # compiled schema IR
  nodes/{hash}/                # per-type Lance datasets for nodes
  edges/{hash}/                # per-type Lance datasets for edges
_db_branches/{branch}/         # non-default branch databases
```

## Test Fixtures

Example datasets in `crates/omnigraph-engine/tests/fixtures/`:

- `test.pg` / `test.jsonl` / `test.gq` — basic Person/Company graph
- `signals.pg` / `signals.jsonl` — AI industry signals (Signal, Pattern, Tech, Company)

## lance-explore

Companion Python CLI for inspecting Lance datasets in a repo. Lives at `../lance-explore`.

```bash
cd ../lance-explore
poetry run lance-explore ls <repo>              # list datasets + manifest metadata
poetry run lance-explore info <repo> <type>      # Lance metadata: versions, branches, indices, fragments
poetry run lance-explore show <repo> <type>      # tabular data
poetry run lance-explore node <repo> <slug>      # node properties + connected edges
```

## Next

Engine integration for branch-scoped reads and writes.
