# Upgrade from 0.13 to 0.14

**Existing 0.13 graphs do not need rebuilding for 0.14.** The main work is
updating configuration, commands and clients.

## Before upgrading

Finish pending deployments and resolve uncertain writes with the old CLI.
Stop application traffic, servers and writers. Preserve the old executable,
operator home and journals, and verify a backup of the **whole graph or cluster
storage root**. Upgrade the CLI, server and SDK together; do not mix versions
against one root. Rollback after new writes means restoring that backup.

Preserve each deployment folder's `.omnigraph` directory as well as the operator
home. Do not discard or rewrite unfinished-operation records during configuration
migration; follow the [recovery guidance](../cli/targets.md#upgrade-from-the-old-format).

## Check graph and data compatibility

| What you have | What to do |
|---|---|
| A graph written by released 0.13 | Keep its root and history. 0.14 reads its format 14; the next ordinary publication advances that branch to format 16 |
| A standalone graph from an older release | Check the [supported in-place upgrades](upgrade.md#storage-upgrade); unsupported formats require export/import |
| A cluster graph below format 14 | Follow the [rebuild and cluster cutover procedure](upgrade.md#cluster-cutover) using an export from its original binary |

Do not run `init` on an existing graph or rebuild it just to upgrade the CLI.
When a rebuild is necessary, export with the old binary and load into a **new**
root. Export/import preserves entities but does not preserve commit history or
branch ancestry; retain the old root until cutover is verified.

For a standalone local graph, this offline check reports its upgrade status
without changing data:

```bash
omnigraph upgrade --target ./graph.omni --check --json
```

The explicit `upgrade` command does not support cluster graphs. Normal 0.13
graphs already open in 0.14; see [storage upgrade](upgrade.md#storage-upgrade)
only if you need explicit stamping before using unpublished commits or unattached
branches, or are converting an older standalone graph.

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

### Local graphs and defaults

One-off local commands need no configuration: pass `--target ./graph.omni`.
To keep a named local destination, add it under the same `targets` mapping:

```yaml
  local:
    kind: direct
    scope: graph
    uri: file:///srv/graphs/dev.omni
```

Use an absolute `file://` URI in the file. Set `defaults.target: local` to use it
when `--target` is omitted; an explicit target overrides that default. A direct
graph target already identifies the graph and must not include `default_graph`.
Old `operator.actor` defaults are gone: pass `--as ACTOR` explicitly on supported
direct writes.

### CI credentials

For CI, replace the server target's `auth` value with an explicit environment
source and inject that variable through your secret manager:

```yaml
auth: {provider: token, source: {env: OG_SERVICE_TOKEN}}
```

This replaces the stored-token source; it does not need `credentials set`.
`OMNIGRAPH_BEARER_TOKEN` and per-name token variables are no longer read implicitly.
Set `OMNIGRAPH_HOME` for the job, or select its YAML with
`omnigraph --operator-config /path/to/config.yaml ...`. Keep tokens out of YAML.

## Replace CLI invocations

Prefix these commands with `omnigraph`:

| Before | After |
|---|---|
| `query people --store ./graph.omni --query queries.gq` | `query people --target ./graph.omni --query queries.gq` |
| `load ./graph.omni --data batch.jsonl --mode merge` | `load --target ./graph.omni --data batch.jsonl --mode merge` |
| `query people --server prod --graph knowledge` | `query people --target prod --graph knowledge` |
| `query people --profile NAME` | `query people --target NAME` after recreating that binding as a target |
| `alias experts databases` | `alias experts --params '{"topic":"databases"}'` after updating the alias below |

`init ./graph.omni --schema schema.pg` is unchanged and creates a **new** graph;
do not run it to upgrade an existing graph. Local paths can be targets directly;
remote endpoints need a named target. Folder context no longer selects a target.
Old direct `s3://` and `az://` CLI routes have no inline `--target` equivalent.
Use a configured server for supported graph operations; direct-only maintenance
does not gain a remote equivalent. See [capability boundaries](../cli/targets.md#transport-and-capability-boundaries).

Aliases now bind to a target. Replace their `server`/resource binding with
`target`, remove `args` and `format`, and pass parameters by name:

```yaml
aliases:
  experts:
    target: prod
    graph: knowledge
    query: find_experts
    params: {limit: 20}
```

Use your actual stored read query and its parameters. An alias needs its own
`graph` even when the target has `default_graph`; aliases never run mutations.
Choose output with `--format` or `defaults.output`.

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

## Update SDKs and scripts

Install the matching TypeScript SDK alongside the server:

```bash
npm install @modernrelay/omnigraph@0.14.0
```

If you use the npm MCP package, upgrade it to `0.14.0` too. Custom HTTP clients
must send `Omnigraph-Http-Api: 0.14` and update their generated bindings;
changing only the header is insufficient. Refresh cached MCP tool schemas and
regenerate saved query plans.

| Consumer | Required change |
|---|---|
| Actor fields | Read `actor_id` instead of `actor` |
| `load --json` | Stop reading `uri`; use the invocation's target. Receipts include `base_branch` |
| Write failures | Read `code` and `disposition`, replacing `command_outcome` |
| Commit-list requests | Use `listing=` instead of the old `unpublished` / `unattached` booleans |

Use explicit `--json` for machine-readable write receipts. For classified data
commands, exit 2 means partial or unknown effects, 4 a failed condition, 64 a
parser refusal and 75 busy; exit 1 means no effects. Other command families have
their own exits. Inspect the reported disposition before retrying a write; a
lost response does not prove failure. See [failure handling](troubleshooting.md#failed-data-write-commands).

## Check schemas, search and embeddings

- String properties used by full-text calls now need `@index`, for example
  `text: String @index`. Update affected declarations and validate stored queries
  before deployment. Compatible full-text indexes stay usable; if search reports
  `FullTextIndexRebuildRequired`, follow the [index rebuild procedure](upgrade.md#full-text-index-upgrade).
- Embeddings created by the older offline concatenation pipeline need regeneration
  and reloading. Each spec must name the single source field declared by `@embed`:

  ```bash
  omnigraph embed --input raw.jsonl --output embedded.jsonl \
    --spec embeddings.json --reembed-all
  ```

  This changes a file, not the graph. Review the output and reload it using the
  intended load mode and preserved entity IDs; see the [offline pipeline](../search/embeddings.md#offline-file-pipeline).
- Review `@range` constraints before reapplying schemas: upper-only bounds are
  inclusive, list bounds apply to each element, and out-of-range writes now refuse.
  See [constraints](../schema/index.md#constraints).
- Send branch names exactly: surrounding whitespace and control characters now
  refuse instead of being trimmed.

## Validate before resuming traffic

```bash
omnigraph version --json
omnigraph config validate
omnigraph target inspect prod --resolved
omnigraph query people --target prod --graph knowledge
```

Use an existing read query for the last command. Configuration validation and
target inspection are offline; the query checks actual access. Verify a controlled
write in staging, representative searches, and scripts that parse results and
errors before restoring traffic. Keep the old installation and verified backup
until the new fleet is proven.
