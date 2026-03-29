# Omnigraph CLI Cheatsheet

All examples use the `og` alias and the demo context graph.

## Setup

```bash
# Build
cargo build -p omnigraph-cli

# Init repo from schema
og init --schema demo/context.pg ./demo/context.omni

# Load seed data
og load --data demo/context.jsonl ./demo/context.omni
```

## Snapshot

```bash
# Show main branch snapshot
og snapshot ./demo/context.omni

# JSON output
og snapshot ./demo/context.omni --json

# Specific branch
og snapshot ./demo/context.omni --branch feature-x
```

## Read Queries

```bash
# Run a single-query file
og read ./demo/context.omni --query demo/context.gq --name get_actor \
  --params '{"slug": "andrew"}'

# Params from file
og read ./demo/context.omni --query demo/context.gq --name get_actor \
  --params-file demo/params.json

# JSON output
og read ./demo/context.omni --query demo/context.gq --name proposed_decisions --json

# Read from a specific branch
og read ./demo/context.omni --query demo/context.gq --name strong_signals \
  --branch feature-x

# Read from a pinned snapshot
og read ./demo/context.omni --query demo/context.gq --name strong_signals \
  --snapshot <snapshot-id>
```

### Example queries from context.gq

```bash
# Lookup
og read ./demo/context.omni --query demo/context.gq --name get_decision \
  --params '{"slug": "create-360-ai-infra"}'

# Who owns a decision
og read ./demo/context.omni --query demo/context.gq --name decision_owner \
  --params '{"slug": "create-360-ai-infra"}'

# What signals triggered a decision
og read ./demo/context.omni --query demo/context.gq --name decision_triggers \
  --params '{"slug": "create-360-ai-infra"}'

# Full audit trail: signal -> decision -> traces
og read ./demo/context.omni --query demo/context.gq --name decision_audit \
  --params '{"slug": "create-360-ai-infra"}'

# Decisions with no supporting evidence
og read ./demo/context.omni --query demo/context.gq --name unsupported_decisions

# Signals that haven't triggered any decision
og read ./demo/context.omni --query demo/context.gq --name unacted_signals

# All decisions owned by an actor
og read ./demo/context.omni --query demo/context.gq --name decisions_by_actor \
  --params '{"actor": "andrew"}'

# All traces recorded by an actor
og read ./demo/context.omni --query demo/context.gq --name actor_traces \
  --params '{"actor": "jorge"}'
```

## Change Queries (Mutations)

```bash
# Run a mutation
og change ./demo/context.omni --query mutations.gq --name insert_signal \
  --params '{"slug": "new-signal", "title": "New signal"}'

# Against a specific branch
og change ./demo/context.omni --query mutations.gq --name insert_signal \
  --branch feature-x --params '{"slug": "new-signal", "title": "New signal"}'

# JSON output
og change ./demo/context.omni --query mutations.gq --name insert_signal --json \
  --params '{"slug": "new-signal", "title": "New signal"}'
```

## Branches

```bash
# Create a branch from main
og branch create --uri ./demo/context.omni --from main feature-x

# Create from another branch
og branch create --uri ./demo/context.omni --from feature-x feature-x-2

# List branches
og branch list --uri ./demo/context.omni

# Merge branch into main
og branch merge --uri ./demo/context.omni feature-x --into main
```

## Transactional Runs

```bash
# List runs
og run list ./demo/context.omni

# Show a run
og run show --uri ./demo/context.omni <run-id>

# Publish a run
og run publish --uri ./demo/context.omni <run-id>

# Abort a run
og run abort --uri ./demo/context.omni <run-id>
```

## Server Mode

```bash
# Start server from project config
omnigraph-server --config demo/omnigraph.yaml

# Override the bind address
omnigraph-server --config demo/omnigraph.yaml --bind 0.0.0.0:9090
```

### Server API endpoints

```
GET  /healthz                 Health check
GET  /snapshot?branch=main    Show snapshot
POST /read                    Execute read query
POST /change                  Execute mutation
GET  /runs                    List runs
GET  /runs/{run_id}           Show run
POST /runs/{run_id}/publish   Publish run
POST /runs/{run_id}/abort     Abort run
```

### Remote CLI (point og at a server)

```bash
# Project defaults
cat demo/omnigraph.yaml

# Read via alias + config
og read --config demo/omnigraph.yaml --alias get_actor andrew

og snapshot --config demo/omnigraph.yaml

og run list --config demo/omnigraph.yaml
```

## Output Modes

`read` supports `--format table|kv|csv|jsonl|json`. Other commands still use human output by default plus `--json` for machine-readable output.
