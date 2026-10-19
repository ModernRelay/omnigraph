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
region. Azure's endpoint is optional and defaults to its explicit account's
Blob endpoint. For Azurite, set `emulator: true` and optionally its HTTP service
endpoint. Local targets may omit `provider`, or use `provider: {kind: local}`.
Native provider credentials remain outside the operator file.

Named targets never inherit ambient account, endpoint or region selection.
An inline cloud target captures these settings from the provider environment
for that invocation. Inline S3 requires an explicit region; with `AWS_PROFILE`,
it also requires an explicit service endpoint. The CLI passes the same captured
destination to admission, control objects and Lance data access.

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
Target administration and credential JSON reports include `protocol: 1`;
`version --json` reports that value as `target_protocol`.

The operator config and credentials files are each limited to 1 MiB.
Unknown fields, duplicate YAML keys, unsupported format versions and legacy
routing sections are errors whenever the operator file is read. Incomplete
unused entries and unused invalid default references do not block an explicit
valid target. The selected entry must be complete for its operation.

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
