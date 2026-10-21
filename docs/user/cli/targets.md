# Operator configuration

Use profiles for service/organization access and targets for resource access.
The operator file is `~/.omnigraph/config.yaml`, or `config.yaml` under
`OMNIGRAPH_HOME`. `--operator-config FILE` replaces it for one invocation;
files are never merged or discovered by searching parent directories.
`--config DIR` remains deployment source, not operator configuration.

## Configure destinations

```yaml
version: 1
profiles:
  work:
    api: https://platform.omnigraph.dev
    organization_id: org_example
    auth:
      provider: omnigraph
targets:
  production:
    kind: managed
    profile: work
    cluster_id: cluster_example
    cluster_incarnation: incarnation_example
    endpoint: https://cluster.example.com
    default_graph: knowledge
  standalone:
    kind: server
    endpoint: https://graph.example.com
    default_auth: operator
    auth:
      operator:
        provider: token
        source:
          store: {}
  scratch:
    kind: direct
    scope: graph
    uri: file:///srv/graphs/scratch.omni
defaults:
  output: table
```

A profile requires `api`, `organization_id` and one `auth` provider object. Each profile represents one
identity; use separate profiles for different people or automation identities, even within the same
organization. A managed target references that profile and pins `cluster_id` and `cluster_incarnation`. Its
optional `endpoint` is required for data operations; without it the registration is control-only. Server
targets own `endpoint` and `auth` directly. Direct targets have `scope: graph` or `scope: root` and an
absolute local `file://` URI without remote authority, query or fragment. A root may declare `default_graph`;
a direct graph may not.

Names match `[A-Za-z][A-Za-z0-9_.-]*` and are case-sensitive. The
[JSON Schema](operator-config.schema.json) describes the fields;
`omnigraph config validate` also checks references and cross-field constraints.
The whole document must be valid, including unselected entries. Unknown fields,
versions, duplicate keys, YAML tags, anchors, aliases, merge keys and implicit null defaults refuse.

Organization IDs contain 1–128 ASCII letters, digits, underscores or hyphens;
cluster IDs and incarnations use the same characters with a 256-character limit.
Graph IDs use the server's rule: 1–64 ASCII letters, digits or hyphens, excluding
`policies`, `healthz`, `openapi` and `graphs`. Served targets additionally accept
the system scope `og_catalog`; direct-root targets do not. Validation checks these
rules for defaults and alias graph selectors before any network or credential access.
There is no interpolation, include or inheritance. The input limit is 1 MiB.
Relative token and CA paths resolve beside the selected operator file.
Credential, helper and CA availability are checked only for selected operations.

## Selection and defaults

| Selection | Precedence and boundary |
|---|---|
| Account operations | `--profile NAME`, then `defaults.profile` |
| Resource operations | `--target NAME`, then `defaults.target` |
| Managed authentication | The selected profile's single `auth` provider; `--auth` is rejected |
| Standalone authentication | `--auth PROFILE`, then server's `default_auth`; otherwise refuse, even with one entry |
| Graph | `--graph ID`, then target `default_graph`; otherwise graph-scoped operations refuse |
| Read output | Explicit `--format`, then `defaults.output`, then `table`; `--json` selects JSON where offered |

Profile and target selectors are mutually exclusive; their defaults never infer each other. Managed targets
always use their pinned `profile`; neither `defaults.profile` nor a global `--profile` can override that
binding. Only standalone servers own named auth entries and `default_auth`. There is no last-used identity or
implicit single-profile selection. A direct graph rejects a separate graph selector. Discovery never selects
the first graph. Graph IDs are exact API identifiers, not display-name lookups; cluster incarnation pinning
does not protect against recreation of a graph with the same ID.

Inline targets are local paths or `file://` URIs only, such as
`--target ./graph.omni`. Bare names select registered targets. Remote endpoints
must be registered. `init ./graph.omni --schema schema.pg` retains its positional
local creation shorthand. Defaults and aliases require registered names.
There are no environment overrides for destination fields.

`defaults.output` accepts only `table`, `json`, `jsonl`, `csv` or `kv` and
applies to supported read-result formatting. Action receipts retain their own
contracts. Unsupported formats refuse. Diagnostics go to stderr, results to stdout.

## Authentication providers

Managed profiles contain one provider object, such as `auth: {provider: omnigraph}`.
Standalone servers retain a named `auth` map with these alternatives:

```yaml
auth:
  saved: {provider: token, source: {store: {}}}
  injected: {provider: token, source: {env: OG_SERVICE_TOKEN}}
  mounted: {provider: token, source: {file: /var/run/omnigraph/token}}
  helper:
    provider: exec
    command: [/usr/local/bin/credential-helper, get]
    timeout: 30s
  public: {provider: anonymous}
```

These are alternatives, not a valid set for every owner. `omnigraph` is a
managed-profile device-login provider. `anonymous` is standalone-server only
and must be selected explicitly or by `default_auth`. Profile names grant no
privileges. Token/exec sources do not qualify arbitrary workload federation.

Token profiles declare exactly one source. `store` uses secure local custody;
`env` reads only the named variable; `file` reads one bounded regular-file
snapshot, allowing symlink-based atomic rotation. Missing or empty sources fail
without fallback. Environment names match `[A-Za-z_][A-Za-z0-9_]*`; there is no
shell expansion. OmniGraph never writes or removes externally owned token files.
Tokens are bounded to 64 KiB and must be valid bearer tokens. Stdin/file input
may remove one terminal LF or CRLF; environment values must contain only the
token. Installation does not verify server acceptance or establish the principal
of an opaque standalone token. A new invocation can observe source rotation.

Exec uses an absolute executable path and argv, never a shell. Timeout is
`1s`–`120s`, default `30s`; stdout and stderr are each bounded to 64 KiB.
The helper receives one version-1 JSON request on stdin with purpose, endpoint,
`interactive: false` and applicable identity constraints. It returns one object
with `version: 1`, `credential: {type: bearer, token: ...}` and optional RFC 3339
`expires_at`. Unknown fields, expired results and unsupported types refuse.
Without expiry the result is not cached across invocations. Timeout terminates
the helper process group; stderr is not echoed in routine errors.
Output cannot change routing or prove identity. Helpers inherit the process
environment and are trusted programs, not sandboxed plugins. Inspection and
validation never execute them.

The exec adapter currently requires Unix process-group support; other platforms
refuse it rather than launch a helper without bounded cancellation.

Never put tokens, refresh tokens or private keys in YAML. Persistent login fails
if secure custody is unavailable; there is no plaintext fallback. Resource
commands never start interactive login. Managed access exchanges the selected
profile credential for a cluster-bound data credential; control and refresh
tokens never go to the graph endpoint. Service and graph policy still authorize
operations independently of credential acquisition.

For managed resource commands and managed-target credential export, env/file/exec
profiles keep exchanged credentials only in invocation memory, without requiring
an OS store. This is explicit source-based custody, not a store-failure fallback.
Human profiles, stored tokens and explicit `cluster token` administration retain
secure-store requirements.

## Login and registration

```bash
omnigraph login --register work
omnigraph login --api https://control.example.com --register private
omnigraph login --profile work
omnigraph cluster list --profile work --json
omnigraph target add production --profile work --cluster CLUSTER_ID
omnigraph target inspect production --resolved --json
omnigraph target refresh production
```

Bare `login` is interactive onboarding, proposing name `omnigraph` and API
`https://platform.omnigraph.dev`. Noninteractive onboarding requires `--register NAME`;
`--organization ID` explicitly selects an organization. Verified selection precedes
saving. Registration refuses existing names and does not change routing defaults.
`login --profile NAME` signs in to an existing profile; login rejects `--auth`.
Login/logout require `provider: omnigraph`, directly or via a managed target's pinned
profile. Static tokens use `credentials`.

Target registration uses authenticated discovery to save exact cluster identity
and its service-attested endpoint, if available. It creates no remote cluster.
Refresh verifies the same incarnation before changing the route pin; missing
discovery endpoints preserve the old pin and refuse refresh. Normal requests
never adopt changed endpoints or follow redirects.

`target list`, `target inspect NAME [--resolved]` and `config validate` are
offline, with optional `--json`. `target add NAME` without `--cluster` reads a
complete target JSON object from stdin (64 KiB limit). `target remove NAME`
preserves credentials and journals; references must remain valid. Add/remove
never switch defaults. `target register` is an alias for `target add`.
Discovery-backed add and refresh contact the service. Resolved inspection expands
the profile and its provider but reports identity as unknown; it does
not verify the principal or probe runtime transport availability.

Logout clears the selected profile's local session and derived credentials and
reports whether remote revocation was confirmed. It cannot promise instant
server revocation of independently valid data tokens. Preserve unresolved
operation evidence across logout and configuration changes.

To deliberately replace a profile's principal, confirm the **new** principal ID:

```bash
omnigraph login --profile work --replace-principal PRINCIPAL_ID
```

This requires an existing profile or managed target;
`--register`, `--api` and `--organization` cannot be combined with it. The newly
verified principal must match the supplied ID. A different principal without
confirmation refuses. Replacement invalidates the profile's credential generation
and clears its derived data cache; it does not reroute existing operations.

## What you may do at a target

```bash
omnigraph access --target standalone --auth operator --branch main --json
```

`access` reports your actor and permissions; there is no separate `whoami` command.
Offline inspection and credential status do not verify identity. Servers use the
selected auth profile and `--graph` or `default_graph`; `--as` asserts the actor.
Direct targets use `--as` or no actor; v1 config has no actor field. Without policy,
direct graphs allow all actions; standalone graphs must open. Managed/catalogue targets refuse.
Use `--branch` for branch permissions, plus `--target-branch` for create/merge permissions.
JSON includes `protocol: 1`, `actor`, `graph_wide` and `actions`; see [access semantics](../operations/policy.md#learn-your-access). Authorization guarantees neither operation support nor storage health.

## Rename registrations and profiles

```bash
omnigraph config rename-profile work company
omnigraph config rename-target production live
omnigraph config rename-auth server standalone operator administrator
# After an interrupted rename, select the same operator file:
omnigraph config recover-rename --operator-config /path/to/config.yaml
```

These local commands preserve credential binding IDs. Profile renames update managed references and the
profile default; target renames update aliases and the target default; auth renames apply only to servers and
update their auth default. Destination-name or retained-custody collisions refuse. Use these commands instead
of manually renaming YAML keys, which does not move custody.

An interrupted rename blocks normal config use until `config recover-rename` finishes it. Conflicting
subsequent edits refuse recovery and preserve the journal for review. Renames and rename recovery have human
output only, with no `--json`.

## Manage credentials

```bash
printf '%s\n' "$SERVER_TOKEN" | omnigraph credentials set --target standalone --auth operator --stdin
omnigraph credentials status --target standalone --auth operator --json
omnigraph credentials remove --target standalone --auth operator
# Pipe export directly to the intended consumer, never to a file.
omnigraph credentials export --target standalone --auth operator | consumer
```

Credentials require one explicit owner selector; defaults do not apply.
Managed credentials use `--profile NAME` without `--auth`:

```bash
omnigraph credentials status --profile work --json
printf '%s\n' "$CONTROL_TOKEN" | omnigraph credentials set --profile automation --stdin
```

Standalone credentials use `--target NAME --auth PROFILE`; managed-target export
uses its pinned profile without `--auth`. Managed set/status/remove require `--profile`.
Direct targets have no HTTP credentials. Set/remove require `token` with
`source: {store: {}}`; set requires `--stdin`, plus `--replace` for existing entries.
Neither edits YAML. Remove is local and idempotent, not remote revocation.

Status performs no network request, refresh, login or helper execution. It reports source availability and
locally known metadata, never values. Secure-store availability and identity are reported as unknown without
prompting or reading secrets. File status does not read contents. Availability is not validity.

Export currently requires a Unix FIFO pipe, refusing other platforms, terminals and regular files before
acquisition. Success writes only a bearer access token and newline, ignoring
output defaults. It may refresh or invoke the selected helper but never starts
interactive login. `--profile` exports control access; a managed `--target`
exports only exchanged cluster data access. Anonymous and refresh credentials
cannot be exported. The receiving process can copy or disclose the token.

## Transport and capability boundaries

Remote endpoints require HTTPS. The only HTTP exception is an explicitly
anonymous loopback standalone server. Optional `transport` belongs independently
to a profile's API or target's data endpoint; it is not inherited.
API/endpoint URLs may include a base-path prefix, which stays inside the
credential scope. Userinfo, queries, fragments, traversal and ambiguous encoded
separators refuse. TLS verifies certificates and hostnames with system roots by default.
`tls.ca: {mode: append, file: company-ca.pem}` adds a PEM CA bundle;
`mode: replace` replaces destination roots. There is no insecure mode.

Proxy defaults to `{mode: direct}`, ignoring ambient proxy variables.
`{mode: url, url: http://proxy.example.com:8080}` selects a proxy explicitly;
`{mode: environment}` opts into captured variables. HTTPS uses
`https_proxy`/`HTTPS_PROXY`; loopback HTTP uses lowercase `http_proxy` only.
Conflicting spellings refuse. `no_proxy`/`NO_PROXY` supports `*`, exact hosts/IPs
and leading-dot DNS suffixes with optional ports; CIDR is unsupported.
Empty proxy values mean direct; bracket IPv6 literals in bypass entries.
`ALL_PROXY` is unsupported. Proxy URLs accept only HTTP/HTTPS and forbid credentials, non-root paths,
queries and fragments. Version 1 rejects custom destination CA plus HTTPS proxy.
HTTP CONNECT proxies support custom destination CAs. Identity-provider
traffic does not inherit API transport settings.

Standalone requests stay within the selected endpoint prefix. A more-specific
registered server endpoint remains a credential boundary: a composed graph or
discovery request cannot borrow its credentials or carry the parent's token into
it. Registrations at the same root do not override the explicitly selected owner
and profile. These boundaries are captured for the invocation.

Version 1 does not support cloud-direct targets, catalogue targets, Unix sockets,
mTLS, tunnels or arbitrary private identity-provider transport. Server storage
and deployment retain separate contracts. This rewrite does not qualify BYOC
private routes or workload federation. Managed and standalone targets share
served graph operations subject to capabilities and policy; maintenance, schema
changes and direct-root administration keep their storage restrictions. Remote
failures never trigger direct storage fallback.
The old standalone `--target og_catalog` shorthand is not a built-in target;
catalogue reads through a registered server still use `--graph og_catalog`.

## Read aliases

An alias has `target`, `query`, optional `params`, and required explicit `graph`
for multi-graph targets. A direct-graph alias must omit `graph`, but execution
still refuses because the stored-query registry is served-only. Aliases never
inherit `default_graph`, contain source code or mutate data. Caller target/graph
overrides refuse; `--auth` may select a named auth entry only on a bound standalone
server. Managed aliases keep the target's pinned profile. `--params`
and `--params-file` must contain a JSON object. Its fields shallowly replace
defaults as typed values, never source interpolation. Arrays, scalars and null
are refused. Alias `args` and `format` fields and positional arguments are unsupported.

## Upgrade from the old format

There is no backward compatibility or importer. Replace `connections` with `profiles`, flatten managed `auth`
to one provider object and remove managed `default_auth`. Change target `connection` to `profile` and
`defaults.connection` to `defaults.profile`. Split identities into separate profiles and pin each target to
its intended profile. Old fields, `--connection`, managed `--auth` and login `--auth` are rejected. For the
older target-only format, replace account targets with profiles and static login with `credentials set
--stdin`. Remove alias `args`/`format`, use `--as` for direct actors, and register remote endpoints. Never put
plaintext credentials in YAML. Preserve old operator homes and unresolved journals; config replacement changes
no graph data.

Legacy folder lifecycle evidence has a separate, explicit recovery command:

```bash
omnigraph config recover-operations --source /path/to/old-workspace --profile work --json
```

It checks the selected profile's API and organization against evidence in the
source's `.omnigraph` directory, durably copies matching pending/accepted records
to the operator home, then retires the originals. Conflicts refuse; preserve both
copies. This is local evidence preservation, not config conversion or operation
submission. Resume with the original verified principal and explicit original
idempotency key; observe accepted operations by their IDs. Keep older executables
excluded after migration. It does not import arbitrary operator-home histories.
