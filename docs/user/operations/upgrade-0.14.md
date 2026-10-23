# Upgrade from 0.13 to 0.14

**Existing 0.13 graphs do not need rebuilding for 0.14.** The main work is
updating configuration, commands and clients.

## Before upgrading

Finish pending deployments and resolve uncertain writes with the old CLI.
Stop application traffic, servers and writers. Preserve the old executable,
operator home and journals, and verify a backup of the **whole graph or cluster
storage root**. Upgrade the CLI, server and SDK together; do not mix versions
against one root. Rollback after new writes means restoring that backup.

0.14 opens the graph format written by 0.13. For older graphs or unfinished
operations, follow the [storage upgrade](upgrade.md) and
[recovery guidance](../cli/targets.md#upgrade-from-the-old-format).

## Rebuild operator configuration

Back up `~/.omnigraph/config.yaml` (or the file in your `OMNIGRAPH_HOME`), then
replace its old routing sections. **There is no automatic importer.**

Before:

```yaml
servers:
  prod:
    url: https://graph.example.com
defaults:
  server: prod
  default_graph: knowledge
```

After:

```yaml
version: 1
targets:
  prod:
    kind: server
    endpoint: https://graph.example.com
    default_graph: knowledge
    auth: {provider: token, source: {store: {}}}
defaults:
  target: prod
```

Use your actual endpoint and graph name. Install its token again:

```bash
printf '%s\n' "$SERVER_TOKEN" | omnigraph credentials set --target prod --stdin
```

Old resource profiles become targets too. `login NAME` and implicit token
environment overrides are gone; credentials must have an explicit source.
For other target kinds and managed sign-in, use the
[configuration guide](../cli/targets.md).

## Replace CLI invocations

Prefix these commands with `omnigraph`:

| Before | After |
|---|---|
| `query people --store ./graph.omni --query queries.gq` | `query people --target ./graph.omni --query queries.gq` |
| `load ./graph.omni --data batch.jsonl --mode merge` | `load --target ./graph.omni --data batch.jsonl --mode merge` |
| `query people --server prod --graph knowledge` | `query people --target prod --graph knowledge` |
| `query people --profile NAME` | `query people --target NAME` after recreating that binding as a target |

`init ./graph.omni --schema schema.pg` is unchanged and creates a **new** graph;
do not run it to upgrade an existing graph. Local paths can be targets directly;
remote endpoints need a named target. Folder context no longer selects a target.

Keep your `cluster.yaml` source bundle. `--config` selects its directory and
`--target` selects the destination. For the server target above:

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --config ./company-brain --target prod
omnigraph cluster apply --config ./company-brain --target prod
```

For local bootstrap or managed deployments, follow the
[cluster guide](../clusters/index.md#bootstrap-a-cluster) or
[managed deployment guide](../cli/managed-lifecycle.md#prepare-and-apply-configuration).

## Validate before resuming traffic

```bash
omnigraph config validate
omnigraph target inspect prod --resolved
omnigraph query people --target prod --graph knowledge
```

Use an existing read query for the last command. Check scripts and aliases for
retired flags, positional alias arguments and changed JSON/exit codes; review
[client changes](../../releases/v0.14.0-overview.md#3-update-http-mcp-and-script-consumers).
Check [affected schemas and embeddings](../../releases/v0.14.0-overview.md#4-check-affected-schemas-and-application-data)
before deploying. Resume traffic only after representative reads and controlled
writes pass.
