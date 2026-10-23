# Upgrade from 0.13 to 0.14

Use this checklist for the released 0.13 CLI and server. For intermediate development
formats, see [operator configuration](../cli/targets.md#upgrade-from-the-old-format).

Work through these steps in order:

- [ ] [Settle old work, stop writers and preserve backups](#preserve-the-old-installation).
- [ ] [Check graph compatibility and affected features](#check-graph-compatibility).
- [ ] [Replace operator YAML](#rebuild-operator-configuration).
- [ ] [Restore credentials and register managed targets](#restore-credentials-and-discover-managed-targets).
- [ ] [Replace CLI commands and deployment invocations](#replace-cli-invocations).
- [ ] [Validate clients, scripts and the new fleet before resuming traffic](#validate-before-resuming-traffic).

Keep these three things separate:

| Item | Upgrade action |
|---|---|
| Operator `config.yaml` | Replace the old routing configuration with profiles and targets |
| Deployment `cluster.yaml` and referenced files | Keep the bundle; validate it and select its destination separately |
| Stored graphs | Preserve their roots and history; check storage compatibility before opening |

## Preserve the old installation

1. With the old binary, settle accepted deployments and uncertain writes. Keep
   operation IDs, idempotency keys, responses and the original identity. Finish
   interrupted configuration renames with the CLI that wrote their journals.
2. Preserve the old executable, operator home and each deployment folder's
   `.omnigraph` directory. Do not delete or rewrite recovery evidence to make
   a new configuration pass validation.
3. Stop application traffic, serving, writers and maintenance. Verify a backup
   of each complete graph/cluster storage root. Update the CLI, server and
   integrations together; do not run a mixed old/new fleet against those roots.

The new CLI does not import a normal 0.13 version-1 folder context. In particular,
`config recover-operations` supports later **version-2** contexts carrying an
organization ID; it is not a general 0.13 journal converter. If old work cannot
be settled, keep the originating installation and evidence and resolve it before
cutover. Never invent an organization ID or change a journal's version.
See [recovery limits](../cli/targets.md#upgrade-from-the-old-format).

## Check graph compatibility

Released 0.13 writes storage format 14; 0.14 opens formats 14–16. Existing
format-14 graphs need no rebuild: an ordinary publication advances that branch
to format 16. The [storage upgrade guide](upgrade.md#storage-upgrade) explains
the optional stamp-only upgrade and features that require it before use.
For older roots, follow its conversion or [rebuild procedure](upgrade.md#rebuild);
cluster-managed graphs below format 14 require rebuild, not direct `upgrade`.

If cluster status reports `ledger_upgrade_required`, follow the separate
[stopped-ledger conversion](../clusters/index.md#direct-deployments-and-conversion).
This conditional conversion already existed in 0.13; it is not a required step
for every cluster. Rollback after new writes means restoring the verified
pre-upgrade backup with the old installation.

Before validating deployment source, check affected features: String full-text
queries require `@index`; incompatible full-text indexes require an explicit
index rebuild, not a graph rebuild. Old offline embeddings need `--reembed-all`
and reloading. Review numeric range assumptions too. Follow the
[schema, search and data actions](../../releases/v0.14.0-overview.md#4-check-affected-schemas-and-application-data).

## Rebuild operator configuration

There is no automatic importer. Prepare a separate operator home so the old
configuration and credential state remain available:

```bash
export OMNIGRAPH_HOME="$HOME/.omnigraph-0.14"
mkdir -m 700 "$OMNIGRAPH_HOME"
```

Use a new directory; do not overwrite an earlier migration attempt. Write its
`config.yaml` using the new shape below, replacing example paths and endpoints
with absolute local `file://` URIs and registered HTTPS endpoints.

**Before: 0.13 operator configuration**

```yaml
operator: {actor: act-alice}
defaults:
  output: table
  server: prod
  default_graph: knowledge
servers:
  prod: {url: https://graph.example.com}
clusters:
  company: {root: file:///srv/company-data}
profiles:
  prod-knowledge: {server: prod, default_graph: knowledge}
  company-admin: {cluster: company, default_graph: knowledge}
  local-dev: {store: file:///srv/graphs/dev.omni}
aliases:
  experts:
    server: prod
    graph: knowledge
    query: find_experts
    args: [topic]
    params: {limit: 20}
    format: table
```

**After: 0.14 operator configuration**

```yaml
version: 1
defaults:
  output: table
  target: prod-knowledge
targets:
  prod-knowledge:
    kind: server
    endpoint: https://graph.example.com
    default_graph: knowledge
    auth: {provider: token, source: {store: {}}}
  company-admin:
    kind: direct
    scope: root
    uri: file:///srv/company-data
    default_graph: knowledge
  local-dev:
    kind: direct
    scope: graph
    uri: file:///srv/graphs/dev.omni
aliases:
  experts:
    target: prod-knowledge
    graph: knowledge
    query: find_experts
    params: {limit: 20}
```

The old **resource profiles become targets**. New `profiles` identify a control-plane
API, organization and identity; create them through login below. Old
`defaults.server`/`defaults.store` become a registered `defaults.target`; move global
`default_graph` into each applicable target. Direct graphs have no `default_graph`.
Replace `operator.actor` with explicit `--as act-alice` on direct operations.

Local paths may be inline targets, such as `--target ./graph.omni`; HTTPS
endpoints must be registered. The old S3/Azure direct CLI routes have no
`--target s3://…` or `--target az://…` replacement. Use a configured served
destination for supported graph operations; direct-only maintenance and upgrade
do not gain a remote equivalent. See [capability boundaries](../cli/targets.md#transport-and-capability-boundaries).

## Restore credentials and discover managed targets

For the standalone target above, install a token from its trusted issuer:

```bash
printf '%s\n' "$SERVER_TOKEN" | omnigraph credentials set --target prod-knowledge --stdin
omnigraph credentials status --target prod-knowledge --json
```

`login prod` is gone; credentials always require an explicit owner. For environment
or mounted tokens, declare the exact [auth source](../cli/targets.md#authentication-providers)
instead of `store`. `OMNIGRAPH_BEARER_TOKEN` and per-name token variables no longer
override credentials. Never copy token values into YAML.

For managed service access, let login discover the organization and registration
discover the cluster incarnation and endpoint:

```bash
omnigraph login --api https://control.example.com --create-profile work
omnigraph cluster list --profile work --json
# Choose CLUSTER_ID from that listing; this registers an existing cluster.
omnigraph target add cloud --profile work --cluster CLUSTER_ID
omnigraph target inspect cloud --resolved --json
omnigraph graphs list --target cloud --discovery
```

The resulting entries have this shape. IDs and endpoints here are illustrative;
keep the actual values written by login and discovery, rather than inventing them:

```yaml
profiles:
  work:
    api: https://control.example.com
    organization_id: org_example
    auth: {provider: omnigraph}
targets:
  cloud:
    kind: managed
    profile: work
    cluster_id: cluster_example
    cluster_incarnation: incarnation_example
    endpoint: https://cluster.example.com
```

These entries join the existing `version: 1` document; do not duplicate its
`targets` mapping. Neither command changes defaults. To choose defaults, edit
the existing `defaults` mapping:

```yaml
defaults:
  output: table
  profile: work
  target: prod-knowledge
```

Account commands now default to `work`, resources to the standalone server.
`--target cloud` uses its pinned profile; `--profile` cannot override its identity.
Plain `login` uses `defaults.profile`; `login --profile work` is explicit.
Folder context and `OMNIGRAPH_PROFILE` no longer select destinations.

## Replace CLI invocations

These examples use the registrations above; keep your actual query names and inputs.

| Task | Before: 0.13 | After: 0.14 |
|---|---|---|
| Create a new local graph | `init ./graph.omni --schema schema.pg` | Same; never run init on an existing graph to upgrade it |
| Load a local graph | `load ./graph.omni --data batch.jsonl --mode merge` | `load --target ./graph.omni --data batch.jsonl --mode merge` |
| Query local source | `query people --query queries.gq --store ./graph.omni` | `query people --query queries.gq --target ./graph.omni` |
| Use a resource profile | `query people --profile prod-knowledge` | `query people --target prod-knowledge` |
| Query a server | `query people --server prod --graph knowledge` | `query people --target prod-knowledge --graph knowledge` |
| Inspect a root's graph | `snapshot --cluster company --graph knowledge` | `snapshot --target company-admin --graph knowledge` |
| Query a managed graph | `query people --graph knowledge` from its bound folder, with no competing operator/environment target | `query people --target cloud --graph knowledge` |
| Call an alias | `alias experts databases` | `alias experts --params '{"topic":"databases"}'` |

Prefix table commands with `omnigraph`. Aliases lose `args`/`format`; use named
JSON parameters and `--format` or `defaults.output`.

For a direct cluster, the old `cluster plan/apply --config ./company-brain`
now needs a root destination as well as the source directory:

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan --target company-admin --config ./company-brain
omnigraph cluster apply --target company-admin --config ./company-brain --as act-alice
```

Direct apply requires stopped serving. For a running standalone server, replace
old `--server prod` with `--target prod-knowledge` on plan/apply and omit the
direct actor label; the bearer token supplies identity. The deployment's
optional `storage:` must agree with the selected destination. See
[deployment ownership](../clusters/index.md#bootstrap-a-cluster) before restarting serving.

For managed deployment, replace `use` and `--managed --config DIR` routing with
the registered target. Keep your existing source checkout, or recover its
managed source into a **new** directory:

```bash
omnigraph cluster pull --target cloud --out ./managed-config --json
# Edit the downloaded files; BASE_REV is data.revision from pull.
omnigraph cluster push --target cloud --config ./managed-config \
  --expected-revision BASE_REV --message "Update configuration" --json
# REV is data.revision from push; PREVIEW_ID is data.preview_id from plan.
omnigraph cluster plan --target cloud --rev REV --json
omnigraph cluster apply --target cloud --plan PREVIEW_ID --json
omnigraph cluster status --target cloud --json
```

Use exact returned IDs. Managed plan/apply refuse local `--config` and consume
pushed source. An externally bound repository uses its Git workflow instead of
pull/push. See [managed deployment](../cli/managed-lifecycle.md#prepare-and-apply-configuration).

## Validate before resuming traffic

```bash
omnigraph version --json
omnigraph config validate --json
omnigraph target list --json
omnigraph target inspect prod-knowledge --resolved --json
omnigraph access --target prod-knowledge --graph knowledge --json
omnigraph query people --target prod-knowledge --graph knowledge --json
```

Use an existing read query for the final check. Validation and inspection are
offline; credential status proves availability, not server acceptance. Managed
targets use discovery and a permitted read instead of `access`.

Update wrappers and CI to pass the new home or `--operator-config FILE` explicitly.
Keep stdout and stderr separate: write destinations appear on stderr and the old
`--quiet` flag is gone. Update JSON consumers for HTTP `0.14`, target/credential/
access protocol `2`, `actor_id`, write `disposition`, and the other
[client and script changes](../../releases/v0.14.0-overview.md#3-update-http-mcp-and-script-consumers).
Regenerate saved plans and refresh MCP tool schemas. Only resume traffic after
the new fleet passes application reads and controlled writes; preserve the old
installation and verified backup for rollback.
