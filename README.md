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

# Load JSONL data into the default branch
cargo run -p omnigraph-cli -- load --data ./data.jsonl ./demo.omni

# Load JSONL data into a specific branch
cargo run -p omnigraph-cli -- load --branch feature-x --data ./data.jsonl ./demo.omni

# Control load mode explicitly
cargo run -p omnigraph-cli -- load --mode merge --data ./data.jsonl ./demo.omni

# Emit machine-readable load output
cargo run -p omnigraph-cli -- load --data ./data.jsonl --json ./demo.omni

# Create a branch
cargo run -p omnigraph-cli -- branch create ./demo.omni --from main feature-x

# List branches
cargo run -p omnigraph-cli -- branch list ./demo.omni

# Merge a branch into main
cargo run -p omnigraph-cli -- branch merge ./demo.omni feature-x

# Merge into an explicit target branch
cargo run -p omnigraph-cli -- branch merge ./demo.omni feature-x --target experiment

# Show branch snapshot
cargo run -p omnigraph-cli -- snapshot ./demo.omni --branch feature-x --json
```

## Repo Layout

After `init`, a repo directory contains:

```text
<repo>/
  _schema.pg
  _manifest.lance/
  nodes/{hash}/
  edges/{hash}/
  _graph_commits.lance/        # appears once branch history/merge metadata is initialized
```

## Test Fixtures

Example datasets in `crates/omnigraph/tests/fixtures/`:

- `test.pg` / `test.jsonl` / `test.gq` — basic Person/Company graph
- `signals.pg` / `signals.jsonl` — AI industry signals
- `search.pg` / `search.jsonl` / `search.gq` — text/vector/BM25/RRF search fixture

## lance-explore

Companion Python CLI for inspecting Lance datasets in a repo. Lives at `../lance-explore`.

```bash
cd ../lance-explore
poetry run lance-explore ls <repo>
poetry run lance-explore info <repo> <type>
poetry run lance-explore show <repo> <type>
poetry run lance-explore node <repo> <slug>
```
