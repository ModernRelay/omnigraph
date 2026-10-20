# Operator targets

The operator file is `~/.omnigraph/config.yaml`, or `config.yaml` under
`OMNIGRAPH_HOME`. It selects destinations independently from `cluster.yaml`:

```yaml
target_config_version: 1
defaults:
  target: local
operator:
  actor: act-alice
targets:
  local:
    kind: direct
    scope: graph
    uri: /data/graph.omni
  root:
    kind: direct
    scope: root
    uri: /data/cluster
    default_graph: knowledge
  prod:
    kind: server
    endpoint: https://graph.example.com
    credential: prod-auth
    default_graph: knowledge
  account:
    kind: managed
    scope: account
    api: https://control.example.com
    organization_id: org_example
    control_auth: human-device
  cloud:
    kind: managed
    scope: cluster
    api: https://control.example.com
    organization_id: org_example
    cluster_id: cluster_example
    cluster_incarnation: incarnation_example
    control_auth: human-device
    endpoint: https://data.example.com
    default_graph: knowledge
```

Target names contain ASCII letters, digits, `-`, `_` and `.`, up to 64 bytes.
`.` and `..` are path spellings; `og_catalog` is reserved for the built-in
catalogue. A registered direct catalogue uses `kind: direct, scope: catalogue`
with no URI. Defaults and read aliases take registered names, never addresses.
Stored relative `uri` paths resolve against the operator file's directory;
inline paths resolve against the invocation directory. Prefer absolute stored
paths for clarity.

A server entry declares exactly one of `credential: OWNER` or
`auth: anonymous`. Each credential owner belongs to one endpoint scope. A token
cannot move to another endpoint when its target is edited. See
[server authentication](index.md#use-a-server).

Managed targets separate account administration from a cluster's exact
identity. A cluster control target does not need a ready data endpoint; graph
commands require `endpoint` and an explicit graph or `default_graph`.
`control_auth` is `human-device` for interactive sign-in or `automation-env`
for the declared automation token environment. Authentication never fills in a
missing destination.

For first sign-in, create a human-device account entry with `api` and omit
`organization_id`. Run `omnigraph login --target account --json`, then record
the returned organization ID in the entry. Only account login permits this
incomplete form; subsequent operations require the organization. Register
cluster IDs and incarnations from verified create/list responses. Login never
silently rewrites the target or selects a cluster.

Cloud direct targets also need explicit non-secret provider destination
settings. These examples belong inside their target entry:

```yaml
# With uri: s3://bucket/cluster and kind: direct, scope: root
provider:
  kind: s3
  region: us-east-1
  endpoint: https://s3.us-east-1.amazonaws.com
  virtual_hosted_style: false
```

```yaml
# With uri: az://graphs/cluster and kind: direct, scope: root
provider:
  kind: azure
  account_name: companybrainprod
  endpoint: https://companybrainprod.blob.core.windows.net
  emulator: false
```

S3's endpoint is optional and defaults to the service endpoint for its explicit
region, including the bucket when `virtual_hosted_style: true`. A custom virtual-host
endpoint must already include the bucket. Azure's endpoint is optional and defaults to its explicit account's
Blob endpoint. For Azurite, set `emulator: true` and optionally its HTTP service
endpoint. Local targets may omit `provider`, or use `provider: {kind: local}`.
Native provider credentials remain outside the operator file.

Named targets never inherit ambient account, endpoint or region selection.
An inline cloud target captures these settings from the provider environment
for that invocation. Inline S3 requires an explicit region; with `AWS_PROFILE`,
it also requires an explicit service endpoint. The CLI passes the same captured
destination to admission, control objects and Lance data access.

Native authentication retains its provider's credential refresh behavior. Target
bindings fix request signing, certificate validation and conditional-write settings;
unsupported ambient proxy, encryption, checksum or transport overrides refuse.
Nonempty `HTTP_PROXY`, `HTTPS_PROXY` and `ALL_PROXY` variables, including lowercase
spellings, also refuse for cloud targets. Optional `OBJECT_STORE_CLIENT_MAX_RETRIES`
and `OBJECT_STORE_CLIENT_RETRY_TIMEOUT` settings are captured for the invocation,
limited to 0–3 retries and 1–180 seconds. Changing those settings after capture
refuses instead of changing the bound client's behavior.

Before a backend mutation, stderr identifies the resolved destination and the
branch when it is known from the command. Ordinary mutations and loads default
to `main`; branch statements report the branch they change, and commit operations
report their commit ID. Source-dependent scope is printed after source preparation
and before dispatch. Stored mutations or backend-interpreted source without an
explicit branch report `branch=backend-resolved` rather than guessing a branch.
A failed diagnostic write or flush prevents dispatch; stdout keeps its existing
result format.

```bash
omnigraph target list --json
omnigraph target show prod --json
printf '%s\n' '{"kind":"direct","scope":"graph","uri":"/data/scratch.omni"}' \
  | omnigraph target add scratch
omnigraph target remove scratch
```

These commands perform no backend requests. Add reads one complete JSON entry
from stdin, up to 64 KiB. Add/remove preserve unrelated configuration and never
change `defaults.target`; removing the default refuses. Removing a target keeps
its credential, so log out before removing the last target that owns it.
`credential get --target prod` exports a server credential only to a pipe or
file; it refuses terminal output. `--json` includes the owning endpoint.
Target administration, credential and access JSON reports include `protocol: 1`;
`version --json` reports their versions as
`protocols: {"target": 1, "credential": 1, "access": 1}`.

The operator config and credentials files are each limited to 1 MiB.
Unknown fields, duplicate YAML keys, unsupported format versions and legacy
routing sections are errors whenever the operator file is read. Incomplete
unused entries and unused invalid default references do not block an explicit
valid target. The selected entry must be complete for its operation.

## What you may do at a target

```bash
omnigraph access --target staging --branch main --json
```

```json
{"protocol":1,"target":"staging","source":"server","actor":"act-me","graph_id":"knowledge",
 "graph_wide":["read","export"],"branch":"main","target_branch":null,"actions":["read","export","change"]}
```

`access` names the actor and the actions the graph's policy grants it:
`graph_wide`, then with `--branch` the actions on that branch (`read`, `export`
and `change` there, and the branch-targeted ones with it as the target), and
with `--target-branch` too `branch_create` and `branch_merge` from one into the
other. `target` is the target's name or the address given inline.

- A server target asks the server's
  [access route](../operations/policy.md#learn-your-access) (`source: server`)
  with the target's token, for `--graph` or the target's `default_graph`.
  `--as` is sent as the `Omnigraph-If-Actor` assertion.
- For direct storage the CLI decides itself (`source: direct`) for the actor
  `--as` or `operator.actor` names. A cluster graph answers by the bundle its
  cluster's applied ledger binds to it, whether a root target with `--graph`
  or the graph's store (`<root>/graphs/<id>.omni`) is addressed; a
  `storage:` in `cluster.yaml` moves neither. With a bundle, nothing is
  allowed without an actor; with none, every action is allowed. A graph the
  cluster does not apply refuses with `not_found`, and one it quarantined, or
  that two bundles bind, with `graph_unavailable`, as a server refuses them.
- A standalone graph carries no policy, so once its store opens every action
  is allowed, as direct writes do; its `graph_id` is `default`. A store that
  holds no graph refuses with `not_found`. A graph that fails to open for
  another reason, such as a manifest that names a missing file, keeps that
  failure's own code.
- A catalogue or managed target refuses with `target_incompatible` before any
  request.

An action listed for a branch is one the policy allows there, not one the
branch supports: `schema_apply` on a branch other than `main` does not make
schema apply possible there. A failure carries a `disposition` with
`effects: none`, as a read's does
([failed commands](../operations/troubleshooting.md#failed-data-write-commands)).

## Migrate an older operator file

`omnigraph target import` explicitly converts legacy server, cluster and profile
entries. It preserves credential ownership and records saved tokens' endpoint
bindings. It keeps a non-secret `config.legacy.yaml` backup. Ambiguous names,
conflicting bindings and relative legacy destinations refuse for manual review;
the importer never guesses from the current directory.

Managed folder context needs manual completion because it does not establish
a complete current cluster incarnation. First convert the operator file by
running `target import` from a directory without legacy lifecycle records.
Then register the verified managed target and run `target import` again from
each original folder holding pending lifecycle work. Import copies verified
pending and accepted recovery evidence to the
operator home before retiring the original records. Preserve unresolved
records. An explicit target works independently of old routing context;
default selection refuses old `.omnigraph/context` and nonempty
`OMNIGRAPH_PROFILE` until those obsolete selections are deliberately retired.

There is no legacy flag or config adapter. Upgrade scripts and operator config
together, and use a separate `OMNIGRAPH_HOME` for older binaries. The target
migration changes neither graph data nor the deployment ledger.
