---
rfc: "0059"
title: "Named targets and CLI coherency"
track: maintainer
status: accepted
implementation: complete
authors:
  - andrew
created: 2026-09-06
updated: 2026-10-29
discussion: https://github.com/ModernRelay/omnigraph/pull/675
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0059: Named targets and CLI coherency

**Historical implementation record.** This RFC records the target resolver
introduced by PR #1090. Its operator YAML, authentication, bootstrap and
migration examples were subsequently revised before 0.14 shipped. Use the
[0.13 → 0.14 checklist](../user/operations/upgrade-0.14.md) and
[operator configuration guide](../user/cli/targets.md) for released commands
and configuration; the examples below preserve the decision's implementation
history.

## Accepted amendment: organization administration (2026-10-27)

This amendment uses the released profile/target model. Account operations
select `--profile NAME`, then `defaults.profile`; resource operations select
`--target NAME`, then `defaults.target`. A managed target pins its profile.
The selectors are mutually exclusive and their defaults never infer one
another. The earlier target-only examples below remain historical; the
[operator configuration guide](../user/cli/targets.md) owns current syntax.

Keep the existing `organization` command family and its `org` alias:

```text
organization members list
organization members set EMAIL|USER_ID --role ROLE [--role ROLE ...]
                         --idempotency-key KEY
organization members offboard EMAIL|USER_ID [--expected-membership ID|absent]
                              --idempotency-key KEY
organization invitations list [--after CURSOR]
organization invitations send EMAIL --role ROLE --idempotency-key KEY
organization invitations resend EMAIL|INVITATION_ID --idempotency-key KEY
organization invitations revoke EMAIL|INVITATION_ID --idempotency-key KEY
```

`members set` replaces the service roles of one existing active organization
membership. It does not invite, enroll, create a role or edit graph policy.
The identity provider owns role definitions and organization service
permissions; applied cluster configuration owns native graph and deployment
permissions. Keep retired local grant commands removed. Do not add a roles
catalog, a new administration hierarchy or managed `access` reporting in this
amendment.

The accepted role-selection extension (2026-10-29) keeps these command families.
Member role flags select a nonempty set of at most 256 existing role slugs;
reject duplicate flags and sort the set before retaining the exact request.
Changing only flag order therefore reuses the same request and idempotency key.
The service receives canonical `roles`, and its receipt must bind the exact
`requested_roles` and verified resulting roles. No implicit default or additive
role assignment is inferred.

Every new invitation explicitly selects one service role with `--role`.
Its receipt exposes the provider-observed role; a successful new send must match
the selected role. Previously saved no-role invitation requests retain their
original arguments, body and key for archived result or uncertainty lookup only.
The CLI may parse an omitted role for that lookup, but a fresh no-role request
refuses before submission. It never inserts a default role into old custody or
reinterprets an uncertain invitation as a new send.

Email is a subject selector inside the selected organization, never a
destination or an authority. Compare email using ASCII case-insensitive equality;
reject whitespace instead of trimming it. Resolve a fresh member selector from the complete
bounded organization directory. A role change binds the exact active membership;
offboarding binds the current membership unless an explicit assertion is given.
`--expected-membership absent` remains an explicit user-ID operation for session
cleanup after membership removal. A failed or ambiguous lookup refuses before
the write; never select the first match. Invitation email selectors resolve
exactly one pending invitation from complete pagination. Bound the scan to
1,000 invitations, ten pages and a 35-second total deadline; refuse truncation,
cursor loops and ambiguity. Exact invitation IDs retain their existing path.

Before any possible write, persist the resolved request in private operator-home
custody: original operation arguments, exact API/organization/principal/profile
binding, path, body and explicit idempotency key. A retry with the same key and
arguments reuses this request without resolving the email or membership again.
Changed arguments or an identity/custody mismatch refuse. Persistence must
finish atomically before submission; neither timeout nor output failure clears
the record. Keep at most 128 records and 1 MiB of encoded custody per stable
profile identity, with no eviction. Renaming a profile preserves this identity;
changing its API, organization or authenticated principal does not permit a key
to select a different request. Independent profiles have separate custody.
Reaching a bound refuses new requests while
preserving exact retries. Reuse the existing atomic file and locking primitives;
do not add a second recovery command or an automatic write replay.

Human member output leads with name/email, membership state, current service
roles and service permissions. Show the stable, complete `principal:ID` needed
for Cedar when available. Provider membership IDs and observation details remain
available in the validated JSON response. Role names are service roles, not
claims of effective graph access, and displayed assignments are not a catalog of
all configured roles.

This amendment is accepted. Implementation completion requires parser/help,
bounded selector resolution, durable same-key retry tests, validated receipts
and the linked user workflow to agree. The earlier completed implementation
status does not qualify this new behavior.

## Summary

Use the existing operator configuration to define named **targets** and a default
in `defaults.target`. A target is the destination of a backend operation, including
its access kind, scope, address and authentication binding. Every invocation
selects its destination the same way:

```text
explicit --target NAME|ADDRESS > defaults.target > missing-target error
```

`--target` takes a target name or an inline address: a storage path or URI, or a
server endpoint, spelled on the command line and needing no operator config
entry. Existing `init URI` is the same inline binding. `--config DIR` selects
input files containing `cluster.yaml`; it does not select a destination. Keep
existing command families, especially `cluster`, and existing operation flags.
Add one `target` group that lists and edits complete entries in operator config;
add no selection command or second current-target state. Editing operator config
changes the default; `--target` overrides it for one invocation. Resolve once,
refuse invalid selections without fallback, and print the resolved destination
before backend mutations.

The change is breaking. One release ships the target contract and removes every
other destination selector, the legacy routing sections and their commands, with
no compatibility adapters; an operator file is either the new format or refused.

Keep declarative plan/apply and existing deployment, identity and publication
authority. This is a targeting change, not a new resource model or deployment
protocol. [PR #1090](https://github.com/ModernRelay/omnigraph/pull/1090)
implements this contract for the 0.14.0 breaking release. The
[CLI reference](../user/cli/reference.md) owns supported behavior and migration
instructions; implementation completion does not itself publish a release.
Access reporting remains separate work under
[the graph-access RFC proposal](https://github.com/ModernRelay/omnigraph/pull/1017).
The proposed second serving model is discontinued; the existing cluster-backed
server remains the sole serving model. Any later CLI MCP bridge must connect to
an existing server and is not a prerequisite for this migration.

## Motivation

The current CLI distributes destination selection among `--server`, `--store`,
`--cluster`, `--profile`, `--managed`, `--direct`, backend URI arguments, operator
defaults, environment profiles, managed folder context and bundle `storage:`
(or its source-directory fallback). Their applicability differs by command.
Some supported data commands use folder context implicitly,
while managed controls require explicit mode selection. Profiles and root
nicknames also have different applicability across data and cluster controls.
The existing ambiguity checks prevent several wrong-destination cases, but do
not provide one selection contract.

Source was reviewed at `864495a863bf92a7f4084dce6b0d47743b112b2a` on 2026-10-16:
75 explicit command leaves, plus argument-dependent modes and aliases. This
includes organization administration from [#1004](https://github.com/ModernRelay/omnigraph/pull/1004)
and local skill commands from [#995](https://github.com/ModernRelay/omnigraph/pull/995).
Owners include [scope](../../crates/omnigraph-cli/src/scope.rs),
[applicability](../../crates/omnigraph-cli/src/planes.rs),
[managed data](../../crates/omnigraph-cli/src/managed/data.rs) and
[application dispatch](../../crates/omnigraph-cli/src/app.rs). This is source inspection,
not runtime qualification of the target resolver.

The common workflow is authoring locally and applying to a chosen cluster.
Bootstrap, authentication and recovery must also work without a source directory
or a ready data endpoint. Cover those cases through complete bindings and existing
operations, without adding commands for each combination. One-off commands and
scratch graphs must not need registration, so a complete binding can also be
written inline.

## User and operational behavior

### Ordinary use

Keep targets and defaults in the existing `~/.omnigraph/config.yaml`, or the
operator home selected by `OMNIGRAPH_HOME`. A minimal schema sketch is:

```yaml
target_config_version: 1
defaults:
  target: staging
targets:
  staging:
    kind: server
    endpoint: https://staging.example
    credential: staging-auth
    default_graph: knowledge
  production:
    kind: server
    endpoint: https://production.example
    credential: production-auth
    default_graph: knowledge
```

With this configuration, plan/apply use `staging` unless overridden. The directory
supplies desired configuration; the named target supplies the backend. Neither
replaces the other. `--config` retains its `.` source default where applicable.
An omitted `--target` uses `defaults.target`, except for the recovery effects
below that require explicit destination selection.

An empty, unknown, malformed or unsupported explicit target errors; it never
falls through to the default. Validate a default reference only when selected:
an unused dangling default cannot block an explicit target, bound alias or local
command. YAML, duplicate-name, format-version, unknown-key and mixed legacy/target
format errors refuse whenever the operator file is read. Variant completeness
and endpoint semantics are checked for the selected entry; unrelated incomplete
entries do not block it. Environment profiles, folder context, cached credentials
and discovery results cannot select or repair a destination. Default selection
refuses a nonempty `OMNIGRAPH_PROFILE` or folder routing evidence that formerly
applied to that command; it never silently substitutes the new default. Explicit
targets, named or inline, remain independent of retired routing evidence. Lifecycle
recovery evidence has the separate mandatory preservation rule below, even with
an explicit target. Automation can use isolated operator homes. Local modes ignore
the default and reject an irrelevant explicit target.

Keep `cluster` for configuration, deployment and managed lifecycle operations.
A configuration bundle is simply the input files, not another registered resource.
No `workspace`, `service` or `deployment` command group is introduced.

### Before and after

The following comparisons record the target-resolver design at this
implementation stage. The [0.13 → 0.14 checklist](../user/operations/upgrade-0.14.md)
gives the final released equivalents. Assume `defaults.target: staging`, with `staging` and `production` as
server targets and `cloud` as a managed cluster target. For the direct-access
example, the working directory is `/data/project` and `local` has
`uri: file:///data/project/graph.omni`, so it names the same graph as `./graph.omni`.
Named entries must be configured first; names do not infer endpoints or paths.
An inline address, including the literal `init URI` form below, needs no
registration.

**Query a server**

```sh
# Before
omnigraph query list_people --server production --graph knowledge

# After
omnigraph query list_people --target production --graph knowledge
```

**Plan and apply to the default cluster**

```sh
# Before
omnigraph cluster plan --config ./app --server staging
omnigraph cluster apply --config ./app --server staging

# After — defaults.target selects staging
omnigraph cluster plan --config ./app
omnigraph cluster apply --config ./app
```

**Apply to a different cluster for this invocation**

```sh
# Before
omnigraph cluster apply --config ./prod --server production

# After
omnigraph cluster apply --config ./prod --target production
```

`--config` selects the files; `--target` selects the destination. The bundle's
declared root must match that destination. This invocation leaves the default
unchanged.

**Plan and apply a managed revision**

```sh
# Before — ./app supplies the managed folder binding
omnigraph cluster plan --managed --config ./app --rev REVISION
omnigraph cluster apply --managed --config ./app --plan PREVIEW_ID

# After
omnigraph cluster plan --target cloud --rev REVISION
omnigraph cluster apply --target cloud --plan PREVIEW_ID
```

`REVISION` is the full pushed commit ID and `PREVIEW_ID` comes from planning.
These commands use pushed source; local edits still require `cluster push`.

**Check a managed deployment**

```sh
# Before
omnigraph cluster status DEPLOYMENT_ID --managed --config ./app --wait

# After
omnigraph cluster status DEPLOYMENT_ID --target cloud --wait
```

**Query a local graph using a query file**

```sh
# Before
omnigraph query list_people --query queries.gq --store ./graph.omni

# After — named, or inline
omnigraph query list_people --query queries.gq --target local
omnigraph query list_people --query queries.gq --target ./graph.omni
```

**Query a server without registering it**

```sh
# Before
omnigraph query list_people --server https://staging.example --graph knowledge

# After
omnigraph query list_people --target https://staging.example --graph knowledge
```

**Register a target**

```sh
# Before — edit servers: and profiles: by hand
# After — edit targets: by hand, or add one entry
echo '{"kind":"server","endpoint":"https://staging.example","credential":"staging-auth"}' \
  | omnigraph target add staging
```

**Validate local configuration — unchanged**

```sh
# Before and after — no destination needed
omnigraph cluster validate --config ./app
```

**Create a scratch graph — unchanged**

```sh
# Before and after — no target registration needed
omnigraph init ./scratch.omni --schema schema.pg
```

An explicit initialization URI is an inline target: it conflicts with `--target`
and ignores `defaults.target`. Without a URI, `init` uses ordinary selection.
Neither form registers a target or changes the default.

## Design

### Complete target bindings

Kind determines access and authentication; scope identifies what is addressed.
A nickname is not a storage, cluster or credential identity.

| Kind/scope | Required destination binding |
| --- | --- |
| Direct graph | One storage URI and any nonsecret provider account/endpoint settings needed to identify that storage. |
| Direct root | One deployment storage root with the same provider requirements. Source and storage are distinct roles but may occupy the same directory. |
| Direct catalogue | The current binary's built-in `og_catalog`; no URI, provider, credential or graph-default fields. |
| Server | One HTTP(S) endpoint or supported Unix socket, with an explicit authentication binding. |
| Managed account | Control API origin, organization ID and declared control authentication mechanism. Organization may be omitted only for first-login discovery described below. No cluster, data endpoint or source directory required. |
| Managed cluster | Control API origin, organization ID, cluster ID/incarnation and declared control authentication mechanism. A configured data endpoint is required for data requests; data credentials keep their existing identity-derived namespace. |

Additional schema sketches:

```yaml
targets:
  scratch:
    kind: direct
    scope: graph
    uri: file:///data/scratch
  local-cluster:
    kind: direct
    scope: root
    uri: file:///data/cluster
  provisioning:
    kind: managed
    scope: account
    api: https://control.example
    organization_id: ORGANIZATION_ID
    control_auth: human-device
  managed-production:
    kind: managed
    scope: cluster
    api: https://control.example
    organization_id: ORGANIZATION_ID
    cluster_id: CLUSTER_ID
    cluster_incarnation: INCARNATION
    control_auth: human-device
    endpoint: https://production.graphs.example
    default_graph: knowledge
```

The version 1 operator format uses these fields. Local direct entries may omit
`provider`; cloud entries declare `provider.kind` (`s3` or `azure`) and all
destination settings. S3 requires `region`, with optional `endpoint` and
`virtual_hosted_style` (false by default). Azure requires `account_name`, with
optional `endpoint` and `emulator` (false by default). Managed entries declare
`human-device` or `automation-env`, not a new credential nickname namespace.
Control lookup uses canonical API/organization; data lookup additionally uses
cluster identity and validates incarnation/principal. Human-device selection and
acquisition never consult automation environment credentials; automation uses the existing
origin-bound environment pair and verifies organization without falling back to
device custody. Valid data credentials retain their independent lifetime. Missing
credentials cannot cause another target, organization or mechanism to be used.

Server authentication explicitly names its existing token owner or selects
anonymous access. Bind each named owner to an allowed endpoint scope and check
that scope before reading or sending its token. Preserve URL base-path boundaries
and canonical Unix-socket identity; a socket's synthetic HTTP origin is insufficient.
A second target cannot reuse that reference at a different scope. Resource requests
with a missing required token refuse; login retains the named credential owner.
Named targets never fall through to `OMNIGRAPH_BEARER_TOKEN`; migration
preserves the keyed owner and records its original endpoint scope. No new secret
store is introduced.

Register targets by editing operator config; entries may exist before login.
For first login, an account entry may omit `organization_id` while declaring its
API origin and device-login mechanism. `login --target provisioning` then starts
a fresh hosted sign-in without selecting or renewing a cached organization's
session. Preserve strict cache-format, session-collision and identity verification
checks; this is a narrow new entry rule around the existing fresh-device protocol.
Save the verified session in the existing API/organization credential namespace,
not under a nickname or an API-only wildcard. Return the verified organization ID
for the operator to add to config; do not modify the entry or default automatically.
A configured organization remains an equality constraint. Resource operations and
logout reject incomplete entries before acquisition or network access; an incomplete
entry cannot use automation credentials to infer its organization. Fresh login
retains the existing shared API/organization session replacement behavior; report
the verified principal and test replacement under the same and a different
principal. Already-running commands keep their captured identity and refuse a
replacement that would change it. Automation entries reject login/logout rather
than altering human-device custody.

Create returns cluster and provisioning-operation identities. The operator adds
the returned cluster binding to config without changing the default implicitly.
A cluster entry may initially omit its data endpoint: control status, deletion,
pull and recovery must still work. Data commands refuse until that binding is
complete. `login --target NAME` and `logout --target NAME` retain the existing
server credential lifecycle or managed control credential bound to API/organization;
direct targets reject these commands. `cluster token` manages the selected cluster's
data credential/cache. Its existing endpoint metadata supplies the value for manual
target completion. Token acquisition can run before the endpoint is configured,
but sends no data request and never edits config. Once configured, compare origins
using the existing `canonical_origin` rules: mismatching issuance refuses cache
replacement, preserves the prior cache and reports safe endpoint metadata. This
deliberately amends RFC 0053's issuer-selected data routing: rotation requires an
explicit rebind and matching credentials, with the resulting availability cost.
Preserve logout's existing control-session and shared-reference
effects; managed data credentials retain independent expiry and explicit clearing.
`cluster token --clear` only forgets the selected local data cache, requires no
login/refresh, and does not revoke a token or delete the backend.

Resolve relative stored paths against the operator file, not the working
directory. A direct root path always means storage, regardless of whether it is
written as a path or `file://` URI. Pin destination-affecting cloud configuration,
including Azure account and endpoint or S3 endpoint overrides, in the registered
target and its resolved binding. Preserve supported provider credential chains
for authentication only.
The same captured storage binding must reach catalog/admission and Lance dataset
access; changing ambient endpoint variables cannot redirect either path. Refuse
an incomplete cloud destination rather than advertise a stable target while
still obtaining its address from the environment.
Classify every provider option read by either storage path as destination,
authentication or transport configuration before implementation acceptance. An
omitted destination option must have a fixed provider default or refuse; it cannot
inherit an environment or AWS-profile endpoint. Cover S3 endpoint/region/addressing
style and Azure account/endpoint/emulator selection, including aliases, on both
paths. Credential refresh cannot change those captured destination settings.
Do not copy every recognized provider environment key into a binding. Signing,
TLS validation, proxy routing and conditional-write settings also require explicit
classification and supported values; an unclassified ambient setting cannot weaken
request authentication or the write protocol. Use fixed safe defaults where this
format exposes no override. This boundary must also hold when the native provider
fills absent options from environment or profile configuration: filtering the
initial map alone is insufficient. Test both conflicting configuration at capture
and changes after capture, through control-object and Lance clients.

Preserve native authentication semantics independently of destination capture.
Refreshable credential providers must remain refreshable; explicitly supplied
static keys, including an environment session-token triple, retain their native
static lifetime. Do not promise refresh for static credentials or replace the
native provider chain with a CLI credential cache.

An inline target, below, is the only exception to registered destination
settings.

Strictly validate duplicate names, variant fields, endpoints, routing/authentication
keys and format versions. Preserve server Unix-socket support and platform refusals;
its synthetic HTTP origin is not the destination. Do not merge operator files or
splice fields from multiple targets. Unknown names in a selected default or alias
refuse. Config edits do not rename credential custody, erase pending recovery
records, log out sessions or change an invocation already in progress.

### Inline targets

`--target` also takes a literal address. Target names are one ASCII word of
letters, digits, `-`, `_` and `.`, so a value containing `/` or `:` is never a
name, and a bare word is always one. An unknown name that is also a relative
path refuses with guidance to write `./NAME`. The reserved name `og_catalog`
names the direct catalogue and cannot be registered. A literal address never
consults `targets:`.

An inline value is a complete binding written on the command line, not a
nickname: nothing maps it to another destination, so the case RFC 0011 rejected,
one nickname switching access path by its URI's scheme, does not arise. Each
supported spelling names exactly one access authority:

| Inline value | Binding |
| --- | --- |
| `http://…`, `https://…`, `unix:///…` | Server at that endpoint. |
| A path, `file://…`, `s3://…`, `az://…` | Direct storage: a graph, or a root when the invocation selects a graph within it with `--graph` or the command applies only to a root. |

The root-only commands are `queries`, `policy`, `cluster plan`, `cluster apply`,
`cluster status`, `cluster observe` and root recovery; a graph-scoped operation
given a root without
`--graph` refuses. The command line alone decides the scope: nothing probes
storage for a cluster's state to classify an address.

An inline server sends a token only when its endpoint lies under a configured
server target's endpoint scope, using the owner of the deepest such scope, which
is the endpoint-bound rule above; otherwise its requests carry none. An inline
value never names or creates a credential owner. Managed destinations have no
inline form: their API, organization and cluster identity need a named target.

Parse a relative path against the invocation directory. For an inline cloud
URI, capture the provider account/endpoint configuration once into an
invocation-local binding, print it with selection source `inline` before
effects, and pass that same binding to catalog/admission and Lance access.
Named targets never inherit these ambient destination settings. The positional
`init URI` is the same inline direct-graph binding; keep its ownership, `--force`
and external-admission restrictions.

An inline target has no `default_graph`, so a multi-graph scope needs `--graph`.
`defaults.target` and aliases take names only, so operator config holds only
registered bindings.

### Target administration

`target list|show|add|remove` read and edit complete entries in operator
config. `add` reads one entry as JSON on stdin and refuses an incomplete or
conflicting one with its code; `remove` refuses the default target. Each edit
changes only that entry's lines and keeps the rest of the file's text, holds an
exclusive lock beside the file, checks that the edited file loads with exactly
the expected targets and every other key unchanged, and replaces it by rename.
A file it cannot edit that way refuses; edit it by hand. No command stores a
secret, sets `defaults.target` or selects a target.

`target import` converts a legacy-format file in one edit: each server, root and
store entry and each complete profile becomes a complete target, keeping its
credential owner name and the endpoint scope that owner was bound to. It may
propose a default and records one only when the legacy file selected exactly
one; it never infers one from a folder, token or environment profile.
`credential get --target NAME` hands a server target's stored token to a pipe,
never a terminal, for a same-account client that attaches to that server.
Target administration and credential JSON reports carry `protocol: 1`.
`version --json` advertises them independently as
`"protocols": {"target": 1, "credential": 1}`, alongside its existing version,
HTTP-contract and storage-format fields. Do not publish a separate scalar
`target_protocol`. Later command families add their own protocol entries only
when implemented.

The nonsecret `binding` digest uses one canonical endpoint identity shared with
credential scope checks, including managed data endpoints. HTTP identity includes
scheme, the URL parser's host, effective port and base-path segments; an empty
trailing segment does not distinguish an endpoint. Thus `https://h.example`,
`https://H.example:443` and `https://h.example/` have the same identity, while
another port or base path differs. Unix identity uses the decoded socket path's
bytes, never lossy display text or its synthetic HTTP origin. Equivalent encoded
spellings compare equally; distinct non-UTF-8 socket paths remain distinct.
The digest includes the binding's kind/scope, destination and declared
authentication binding, but excludes target nicknames, default graph, token values
and token expiry. It identifies a binding, not permission or a database snapshot.
Changing these identity rules after protocol 1 is consumed requires compatibility
review.

Preserve stable refusal codes in the existing error envelope: `target_unknown`,
`target_invalid`, `target_exists`, `target_is_default`, `config_not_editable`,
`target_kind_unsupported`, `credential_terminal`, `credential_anonymous` and
`credential_missing`. Integration into the shared typed error registry follows
its owner; this RFC introduces no second error envelope or exit-status table.

### Command applicability

The following groups map the 75-leaf baseline and the new target/credential
commands into the accepted post-migration surface. The embedded command-index
test pins every current Clap leaf. Applicability is per mode,
not merely the top-level verb. Existing ownership, authorization and transport
checks remain additional requirements; this table grants no permissions.

| Command or mode | Eligible targets / behavior |
| --- | --- |
| `query`/`mutate` with inline or file source; `load`; `commit list/show` | Direct graph or root with a selected graph, server or managed cluster with data binding. A local input file still executes against the selected backend. |
| `query`/`mutate` with a stored query name and no source | Server or qualified managed data path; never infer a local filename from the name. |
| `branch create/list/delete/merge/diff/merge-base`; `snapshot`; `export`; `blob get/stat/put/clear`; `diff`; `commit publish/drop/changes`; `changes poll/baseline`; `schema show` | Direct graph, direct root with a selected graph, or server. Managed wrappers remain unsupported until individually qualified. |
| `graphs list` | Server or qualified managed data path; registry scope. |
| `alias` | Existing server read binding with its own target, graph, query and parameter/output defaults. Managed aliases need separate qualification. |
| `init` | Direct graph target, named or inline; `init URI` is the inline form. May address a nonexistent store. |
| Built-in catalogue read query/explain or `schema show` | Direct catalogue, or server with `--graph og_catalog`; no mutation, branch or snapshot. Managed catalogue access remains unqualified. |
| `upgrade`; `schema apply/upgrade-system-columns` | Direct graph, subject to existing standalone ownership restrictions. |
| `optimize`; `rebuild-full-text-indexes`; `repair`; `cleanup`; `schema plan`; live-schema `lint` | Direct graph, or direct root plus selected graph. Make root `schema plan` independent of which old selector supplied the root. |
| `queries list/validate`; `policy validate/test/explain` | Direct root, retaining existing optional explicit graph filtering and applied-state semantics. Served catalog support remains separate work. |
| `cluster validate`; `lint --schema`; `embed`; `version`; `skill export/show` | No backend target. Embedding retains its separately configured provider/input behavior; skill export retains local file writes. |
| `target list/show/add/remove/import`; `credential get` | Operator config and the credentials file only; no backend request. |
| `cluster plan/apply` | Direct root or server with source input; managed cluster with the revision/preview forms below. |
| `cluster status` | Direct root, server or managed cluster, with existing backend-specific exact-ID forms. |
| `cluster observe` | Direct root with local source input to select declared graphs and compare observed state. |
| `cluster force-unlock/upgrade-ledger`; writer-attested source-free `cluster apply` | Direct root with explicit `--target`; retain exact identities and existing quiescence/admission requirements. |
| `cluster create/list` | Managed account; organization is fixed before authentication. |
| `cluster members list`; `cluster pull/push/delete/undo-delete/token/history/cancel` | Managed cluster control binding; data endpoint readiness is not required. |
| `cluster operation` | Managed account or matching managed cluster; exact provisioning-operation identity. |
| `organization members list/apply/offboard`; `organization invitations list/send/resend/revoke` | Managed account; preserve the `org` alias and existing operation inputs. |
| `login/logout` | Named-token server or human-device managed account/cluster; automation and anonymous entries refuse. Only account login permits missing organization. |
| Existing `use`; `profile list/show` | Removed with the legacy formats; migration is described below. No selection command replaces them. |

Graph-scoped server/managed operations require `--graph` or target `default_graph`.
Direct root graph tools use the same rule; a direct graph target already identifies
its graph and rejects another selector. No sole-graph discovery supplies a missing
graph. `graphs list`, whole-cluster and organization operations ignore graph
defaults and reject explicit graphs. Root `queries`/`policy` tools retain explicit
graph filters, ignore `default_graph`, and preserve their existing all-graph or
ambiguity behavior.

The catalogue row overrides ordinary graph applicability. A direct catalogue
target identifies the existing store-free `og_catalog`, never a relative path;
served access keeps its reserved graph name and existing read-only restrictions.
Retain ordinary query source/name modes, catalogue versus graph scope, root graph
filters, sole/multi-graph refusals and temporal-selector conflicts in the mode
audit. Leaf count alone is not evidence of coverage.

Organization commands use the account target's API/organization and control
credential. Remove their `--managed`, `--api`, `--organization` and context-only
`--config`; retain member-patch files, versions, IDs, cursors and required
idempotency keys. A patch's organization must equal the target, not override it.
Keep enrollment, invitations, offboarding and graph-policy effects distinct under
the existing owners. Target migration does not add automatic request replay.

Preserve existing query/mutation effect doors, source/name/parameter combinations,
branch/snapshot support and conflicts, pagination and conditional-write semantics.
Do not make operation-specific selectors global. Apply actor rules per operation:
served identity is server-derived, while policy-bearing direct writes (including
commit publish/drop) retain their actor path. An unsupported kind, mode or flag
refuses before credential acquisition or backend effects; it never reroutes.

Read aliases name a complete target independently of `defaults.target`. Conflicting
target or graph overrides refuse. They do not introduce another resolver or inherit
credentials from ambient config. Help remains local and uses existing Clap and
applicability declarations; no second exhaustive command registry or backend probe
is needed. No completion command or generator exists in the inspected CLI; adding
one, including local target completion, is outside this decision.

### Configuration input and deployment

`--config DIR` means source files for `cluster validate`, `cluster observe`,
managed `cluster push`, and fresh direct/server `cluster plan/apply`. Keep the
existing `.` default only for those source modes. Bundle `storage:` becomes an
optional equality assertion, never a destination selector. If absent, use the
selected root; do not derive storage from the source directory. Co-located source
and state remain supported by a target pointing to that directory.

For a direct local root, resolve stored target paths against the operator file
and bundle-relative assertions against the source directory, then compare using
the existing deployment/storage canonicalization, including symlinks. Preserve
the deployment owner's existing-root requirement; `init` alone can create a graph
at a nonexistent address. For a server, retain comparison with its advertised
canonical root: do not resolve the server's paths or symlinks on the CLI host.
Relative server-root assertions refuse. For direct cloud roots, compare normalized
URI plus the selected account/endpoint binding;
interpret the bundle's URI assertion within that same binding, never a second
environment-derived one. Refuse mismatches or unprovable equality before submission.
Keep `cluster.yaml`, storage formats and wire names unchanged.

Remove `--config` wherever it only supplied routing context, including managed
create/list/members/pull/plan/apply/status/history/cancel/token/delete/undo-delete/
operation, all organization operations, and root-only status/force-unlock/recovery.
Distinguish an explicit argument from an old parser-default `.`: only an explicit
irrelevant flag is an input error. These modes read no implicit source or routing
context; the legacy recovery-evidence checks below remain mandatory.

| Operation | Input, mode and outcome |
| --- | --- |
| Managed `cluster push` | Capture/upload referenced files with existing bounds, expected revision and replay contract. |
| Managed `cluster pull --out NEW_DIR [--rev REVISION]` | Fetch an exact revision, or capture the source head. Preserve new-path and atomic-publication protections; do not install folder routing or change the config default. |
| Direct/server `cluster plan` | Preview local source against the selected root/server. |
| Managed `cluster plan [--rev REVISION]` | Prepare an immutable preview of a full lowercase Git commit ID or captured source head. Return `PREVIEW_ID`; no change lease or implicit local upload. |
| Direct-root `cluster apply` without an ID | Source apply; `--config` retains its `.` default. Capture/revalidate source through the existing owner; serving must be stopped. |
| Direct-root or server `cluster apply --config DIR --deployment-id ID` | Explicit source submission or observation with a caller-supplied full ID satisfying the existing ledger/sequence/nonce rules. Only a command-line `--config` selects this form; a parser default does not count. |
| Direct-root `cluster apply --deployment-id ID [--writers-stopped]` without explicit `--config` | Source-free lookup/reconciliation of the original invocation. Never read default `.` or resume schema execution. Terminal/not-recorded lookup needs no attestation; outstanding reconciliation requires `--writers-stopped`. |
| Server `cluster apply` without an ID | Source apply with the existing `--config` default and live activation. Reject `--writers-stopped`. |
| Server `cluster apply --deployment-id ID` without explicit `--config` | Refuse without reading source or submitting; direct the caller to existing `cluster status --deployment-id ID`. |
| Managed `cluster apply --plan PREVIEW_ID` | Create a delivery for that exact preview; never silently replan or push. Preserve product and native deployment IDs. `--no-wait` acknowledges durable delivery reservation, not native acceptance. |
| `cluster status --deployment-id ID [--wait]` | Existing server form; direct root supports the ID form without wait. |
| Managed `cluster status [DEPLOYMENT_ID] [--wait]` | Current cluster projections without an ID, or exact delivery observation. Wait requires the ID; it cannot be replaced by self-hosted `--deployment-id`. |
| `cluster operation OPERATION_ID [--wait]` | Provisioning recovery, distinct from preview and deployment delivery IDs. |
| Managed `cluster cancel DEPLOYMENT_ID` | Cancel the caller's queued delivery before first dispatch; no native cancellation or plan abandonment is implied. |

Today `--cluster` selects direct reconciliation and `--writers-stopped` only
attests safety after selection. After its retirement, an ID with no explicit source
can only select source-free lookup/reconciliation or refuse; changing target kind
cannot turn it into source submission. A safety attestation never changes intent. Reject
`--writers-stopped` with explicit `--config` or without a deployment ID. Omitting
attestation cannot turn a missing receipt into a fresh deployment. Direct
writer-attested apply, force-unlock and upgrade-ledger require explicit `--target`
before storage access, even if the eventual receipt is terminal. Unattested
source-free lookup may use the default. Existing exact identities, actor checks
and quiescence requirements remain additional conditions; no new confirmation
flag is introduced.

For direct and server source submissions with an existing deployment ID, matching
immutable input only observes the original invocation; changed input refuses.
Neither deliberate resubmission nor receipt eviction permits that ID to execute
again. Source mode may capture files to verify equality; recovery mode never does.
Caller preallocation remains supported; computing an ID does not reserve its
sequence. For a direct root, `cluster status --target ROOT --json` exposes
`ledger_id` and `next_sequence`; a caller can form
`<ledger_id>:<next_sequence>:<fresh ULID>` and submit it with explicit `--config`.
A concurrent submission can make that sequence stale, which refuses.
Do not replace these existing rules with arbitrary idempotency strings or require
that the server previously issued the ID.

Managed apply rejects `--deployment-id` and `--writers-stopped`; server apply
rejects the writer attestation; direct recovery rejects managed `--plan` and run
options that do not apply. Keep delete/undo-delete's explicit `--incarnation` as
an equality assertion against the target, never an identity override. Their
retention and deletion-ID semantics are unchanged. `cluster members list` reports
explicit enrollment, not effective graph-policy permissions.

The [control-plane guide](../dev/control-plane.md) and
[server deployment RFC](2026-09-29-server-runtime-and-online-deployment.md)
continue to own durable authority and activation. Targeting adds no ledger writer,
per-graph apply loop, last-applied cache or field-ownership database. Removed
import, refresh and approve commands stay removed.

### Resolution, credentials and outcomes

Parse the operation, resolve its named or inline target, and validate
applicability before acquisition or backend effects. The login-only
organization-discovery path freezes the API/mechanism first and verifies the
provider-selected organization before saving its credential; it performs no
resource operation. Carry that immutable binding and provenance through
submission, response handling and observation. A later config/default edit, folder
change or login cannot redirect an in-flight command. Operation retries may capture
fresh authorized database state under their existing protocol, but cannot select
another destination. Renewal remains bound to the same identity.

The [identity and applied-policy RFC](2026-09-09-identity-credentials-and-applied-policy.md)
owns authentication. Keep strict identity-only v2 data credentials and isolated
storage, server, managed control and managed data namespaces. Validate API,
organization, cluster/incarnation, endpoint and principal agreement before sending
a managed data credential. An existing cache cannot supply a missing destination.
Malformed/unsupported caches refuse before issuer access; missing/expired or
different-principal credentials follow the existing bounded self-only acquisition
and lock/revalidation protocol. Endpoint rotation requires an explicit binding
update and matching credentials. Permission refusal never changes identity or
mode. Current applied policy authorizes operations; candidate policy cannot
authorize its own deployment. Direct engine policy checks remain mandatory.

Print the resolved target kind, address, graph/branch where applicable and selection
source to stderr before backend mutations, including data, branch, Blob, load,
deployment, lifecycle and organization effects. Include organization and cluster
identity where needed to distinguish managed destinations, the operator-file
path for configured selections and the selection source `inline` for an inline
address. Use `redacted_storage_uri` for storage labels;
never expose userinfo, bearer values, signed URLs or query-string credentials.
Remove `--quiet`: its only current effect is suppressing the target line,
which conflicts with this contract, so the breaking release refuses it.
Destination and unknown-outcome evidence always remain visible. Do not invent a new progress flag.
Report a known branch from command arguments or the already prepared source;
ordinary mutations and loads use their existing `main` default. GQ branch
statements report the branch they change. Commit-addressed operations report the
commit ID, while opaque stored/backend-interpreted source without an explicit
branch reports `branch=backend-resolved`. Do not perform an extra authoritative
backend read merely to label a diagnostic or guess `main` for such operations.
Source-dependent scope may follow the initial destination line, but both must be
delivered and flushed before writable open or dispatch.
Existing read-shaped operations can have recovery effects: classify the actual
operation, including equivalent GQ statements, rather than trusting its verb.

Apart from retiring `--quiet`, keep current output flags, query formats, binary
streams, error/outcome envelopes and file-output meanings. Resolve the effective
output mode once for success and failure, including alias/operator defaults. Human diagnostics must not contaminate
stdout. No generic `--output` or wait redesign is part of this RFC; `embed --output`
remains a file path. Preserve existing `--yes`, `--confirm`, `--force` and
operation-specific timeout/no-wait behavior, including their validation and bounds.

Server submission retains its original deployment ID; an uncertain response leads
to exact-ID observation, not another write. Managed delivery and lifecycle requests
retain their existing exact-key/body replay scope; this does not generalize to data
writes or token issuance. Move existing pending lifecycle recovery custody from the
source folder to operator-owned storage. Persist API, organization, principal,
request identity/digest and key before sending. Repeating the same existing command
resumes its matching pending request; conflicting reuse refuses under the existing
pending-request rules. An unresolved pending request with a different key or
request body refuses with `pending_lifecycle_conflict`; no new key is sent.
Mutable target names, changed folders or config edits cannot
erase that record or redirect its replay. This is recovery evidence, not another
remote state authority; its implementation must preserve the accepted identity and
bounded-retention rules below.

Migration must inspect the invocation/source folder for legacy pending, last-operation
and bound-context evidence before any lifecycle submission, even with an explicit
target. Preserve/import verified evidence durably or refuse with recovery guidance;
never ignore it and mint a new create key. Import pending records without changing
their API, organization, principal, digest or key. Legacy last-operation/context
records lack some of those fields: retain their known identities as observation
evidence or a refusal guard, never fabricate a replayable request. Do not retire old
custody until the new record is durable and conflicting legacy writers are excluded.
An interrupted migration must recover or refuse, never appear as empty history.

Serialize custody read/check/persist under one lock for each API/organization
custody domain within an operator home. This is local coordination, not cross-home
deduplication: moving recovery to another home
requires transferring verified custody or using the original operation ID/key
within its owner's replay contract. Independent homes do not acquire a distributed
lock or infer common intent from equal target names.

Persist accepted cluster/incarnation/operation identity before retiring a pending
create. Match automatic recovery by API, organization, verified principal, operation
and canonical request digest, never nickname or folder. An explicit key must also
match its saved request. Multiple matches refuse and identify the existing operation
IDs; do not choose one. A retained accepted match observes its original operation.
A different explicit `--idempotency-key` denotes a deliberate new create only after
prior submission uncertainty is resolved by exact-key recovery or authoritative
operation observation; absence of a local record is not proof of non-acceptance.

Version 1 permits one pending request, at most 128 accepted request records and
at most 128 imported accepted-evidence records per API/organization custody
domain in an operator home. Each serialized custody file is limited to 1 MiB.
At capacity, refuse new lifecycle submissions requiring a new record; existing
record lookup and replay remain available. A full accepted-record list reports
`lifecycle_custody_full`; unreadable, oversized or invalid custody fails closed. Do not evict uncertain records by age or LRU. Accepted
payloads may retire only after verified terminal observation and durable installation
of a guard for that custody domain against automatic key generation on a subsequent
lookup miss.
After retirement, an unrecognized no-key create refuses: use a known operation ID
for recovery or an explicit key for a deliberate new create. The guard precedes
deletion and survives config edits. This permits bounded custody without promising
unlimited historical lookup or extending server replay retention. No discard/admin
command is added. Preserve acceptance evidence on persistence failure and retain
the old bound-folder protection until its equivalent record or guard is durable.

Pin observation to the original target/incarnation, operation ID and source
revision/digest where available. Managed success requires fresh native convergence
and active runtime plus archived evidence, not archived bytes alone. A newer healthy
rollout cannot satisfy an earlier wait. Keep accepted, applied, active and ready
distinct. Preserve finite connection/request/body limits and per-operation wait
budgets. Timeout stops local observation, not accepted work, and retains original
IDs and recovery instructions. Nonzero exit does not establish absence of effects.

### CLI implementation boundaries

Implement the targeting contract through one preparation path inside the existing
CLI crate. The module review at `4d7536d68b1db6da34bf9cff22a90d9fdd77bed4`
on 2026-10-17 found repeated configuration interpretation in scope resolution,
client construction and rendering. This is source evidence for the boundaries
below, not a new command inventory or runtime qualification.

```text
parse -> prepare operation and binding -> acquire credentials -> execute -> render
```

1. **Prepare a typed operation.** Capture argument provenance and only the bounded
   configuration, environment, path and applicable source/alias inputs needed by
   that mode. Separate input loading from a pure preparation function over captured
   values. Extend the existing exhaustive applicability owner; do not add a second command registry.
   Distinguish source/name queries, offline/live lint, catalogue access and source
   apply/recovery explicitly. Preserve Clap's command-line-versus-default distinction
   for `--config`. Preparation rejects invalid combinations before credential
   acquisition or backend effects; remote identity verification remains with its
   existing owner. Local modes keep their current independence from target config.
2. **Pass one binding down.** Replace the optional-string scope translation with
   typed target variants carrying scope, selection provenance and destination
   settings. Client constructors and handlers receive the prepared binding; they
   cannot reload routing config, select another authentication mechanism or infer
   a destination from the working directory. Keep the specified first-login and
   inline-target exceptions. Required legacy recovery-evidence inspection and
   custody validation remain explicit stages; they cannot select a new destination.
   Inject narrow credential stores, clock inputs and writers where needed, rather
   than a context object that gives every handler access to every service.
3. **Give command families their own modules.** Co-locate each family's argument
   definitions and handlers with explicit imports, following the existing managed
   modules. Keep shared preparation, target resolution, transport and rendering
   owners separate. A small `main.rs` owns process startup and final exit; `lib.rs`
   makes internal preparation and handlers testable without subprocesses. Keep
   these modules in the existing crate; no public SDK or command framework is
   introduced. Preserve separate graph, managed-control and OAuth transport owners,
   including destination checks, deadlines, redirect prohibitions and
   operation-specific replay rules.
4. **Capture presentation and return outcomes.** Resolve applicable alias/operator
   output defaults once and carry the resulting preferences to successful output
   and execution failures. Parse/config failures use only the output mode safely
   established so far. Renderers receive preferences and writers, not config
   loaders. Handlers return typed completion information; the outer boundary owns
   flushing and exit codes. Preserve existing execution/effects evidence, deployment
   outcomes and unknown-write classification. Streaming JSON and Arrow stay streaming;
   this does not require buffering all results or a universal JSON result envelope.

Keep Clap, Serde, `reqwest`, existing authentication libraries and `assert_cmd`.
Use Clap's reusable argument groups and
[`value_source`](https://docs.rs/clap/latest/clap/struct.ArgMatches.html#method.value_source)
for explicit argument provenance. Reuse workspace
[`thiserror`](https://docs.rs/thiserror/latest/thiserror/) for typed preparation and
acquisition errors while preserving domain errors and public envelopes.
[`serde_path_to_error`](https://docs.rs/serde_path_to_error/latest/serde_path_to_error/)
is a small candidate for nested configuration diagnostics such as
`targets.production.endpoint`; it does not replace strict schema validation or
secret redaction. Optional development-only
[`trycmd`](https://docs.rs/trycmd/latest/trycmd/) transcripts may check help and
executable examples when they reduce fixture duplication; they are not required
for the migration or a replacement for existing process/fault tests. No new
framework, automatic configuration merger or generic write-retry middleware is
needed.

## Invariants

The [architectural invariants](../dev/invariants.md) remain unchanged. A target
binding does not replace an accepted engine snapshot. Each attempt captures and
revalidates coherent authority; graph changes retain their publication boundary,
unpublished-result contracts and durable receipts. No shadow configuration ledger,
cloud-only correctness path, custom storage primitive or generic retry queue is
introduced. The current substrate is unmodified Lance 13.0.0; this CLI decision
changes no dependency or storage format. Its complete upstream
[object-store](https://lance.org/guide/object_store/) and
[observability](https://lance.org/guide/observability/) pages and native provider
option loading were reviewed. Any implementation changing that boundary must
repeat the relevant [Lance qualification](../dev/lance.md) against the dependency
version it will merge with.

## Compatibility and reversibility

This accepted decision replaces only the routing and custody-selection sections
listed below. Each earlier RFC carries a matching disposition; its other contracts
remain authoritative. These are design dispositions, not claims that a released
binary already uses the new syntax. Runtime cutover is the one breaking release
below.

| Accepted owner | Accepted disposition |
| --- | --- |
| [RFC 0007, D2–D6](0007-operator-config.md#design) | Replace routing schema/precedence, server-named login, legacy credential fallback and additive migration; preserve keyed credential custody, endpoint scope, actor/output preferences and atomic credential writes. |
| [RFC 0011, access path and resolution](0011-cli-addressing-and-config.md#access-path-is-derived-not-chosen) | Replace primitive/profile selectors with fixed declared kinds; preserve no scheme inference, distinct access authorities and refusal without fallback. |
| [RFC 0052, operational behavior](0052-managed-control-plane-cli.md#user-and-operational-behavior) | Replace folder/mode routing; preserve authorization, operation identities and outcomes. |
| [RFC 0053, managed CLI data access](0053-offline-data-token-verification.md#managed-cli-data-access) | Replace implicit folder routing and issuer-selected data endpoint with a configured endpoint assertion; preserve credential verification and bounded acquisition. |
| [RFC 0061, design](0061-managed-cluster-lifecycle.md#design) | Move folder recovery custody with verified migration and bounded retention; preserve exact-key/body replay and operation observation. |
| [Identity RFC, provider-native access](2026-09-09-identity-credentials-and-applied-policy.md#provider-native-access-and-standard-clients) | Replace folder-based organization selection with target identity and explicit mechanisms; preserve principal custody, renewal checks and current-policy authority. |
| [Server-runtime RFC, CLI and conversion](2026-09-29-server-runtime-and-online-deployment.md#cli-and-conversion) | Replace literal `--cluster` routing and configuration-discovery independence with explicit root targets and the isolated-home procedure below; preserve deployment/reconciliation authority and full-ID non-replay. |

RFC 0011 rejected the former `--target` because one graph nickname could switch
access path according to its URI scheme. This decision intentionally revisits that
decision: kind/scope are mandatory, cannot be overridden per command, and are
validated and announced with the complete binding. A path, nickname or credential
never infers another kind. The spelling is shared; storage and server authority
are not interchangeable. Its no-auto-pick principle remains intact.

| Existing surface | Migration |
| --- | --- |
| Server/store/root entries, profiles and flat routing defaults | Complete entries in `targets`, converted by `target import`; deliberate `defaults.target` selection. Keep presentation preferences separate. |
| Bundle `storage:` or omitted-storage source-directory fallback | Optional root equality assertion. Register the actual old storage root, including the source directory for a co-located layout; omission thereafter uses the target root. |
| Sole-graph auto-selection | Set `default_graph` explicitly when migrating that binding, or refuse with guidance to choose a graph; do not discover one at execution time. |
| Local `--store og_catalog` | A direct catalogue target (`kind: direct`, `scope: catalogue`) with existing query/schema verbs; served access retains `--graph og_catalog`. |
| `--server`, `--store`, `--cluster`, `--profile`, named `--uri` and positional backend addresses | `--target NAME`, or `--target ADDRESS` inline; positional `init URI` remains as the inline form. Retain data/source URIs and operation IDs as inputs. |
| `--managed`, `--direct`, `OMNIGRAPH_PROFILE` and implicit folder context | Target kind/scope; retire mode and ambient routing. `--direct` cannot mechanically become storage because it currently also permits HTTP. |
| Authentication server name, `--api`, `--organization` on login/logout and managed cluster/organization controls | Target plus existing operation inputs; no independent API or organization override. Account login alone permits organization discovery for an incomplete entry. |
| Existing folder `use` | Retire it; migrate its verified binding to operator config explicitly. Do not introduce a replacement selection verb. |
| `profile list/show` | Removed; `target list/show` reads targets. |
| Context-only `--config` | Remove as enumerated above. Preserve actual source input and pull's separate `--out`. |
| Read aliases | Explicit target-bound read aliases, preserving query/parameter/output behavior and credential ownership. |
| `--quiet` | Removed. Destination and unknown-outcome diagnostics cannot be suppressed. |
| Folder pending/last lifecycle records and bound context | Verified custody migration or submission refusal, including incomplete accepted evidence; no fresh key on missing imported history. |
| Server global bearer fallback | Migrate to an explicitly endpoint-bound named token owner or anonymous access; never attach the global token implicitly. |

Migration is explicit: `target import` may propose a default but cannot silently
choose one from a folder, token, environment profile or colliding nickname. Import
complete bindings, preserving graph scope, organization, identity and credential
ownership.
Equal nicknames or URL prefixes are not proof of equivalence. A migrated default
must be deliberately recorded as `defaults.target`; without one, only explicit
destination selections (`--target`, named or inline, a bound alias or
`init URI`) work.
No separate selection state is written.
Before lifecycle cutover, account for the legacy folders used by that operator;
the invocation-directory guard cannot discover recovery records in other folders.
Keep unresolved work on its original custody or transfer its verified evidence.

There is no compatibility interval and no legacy adapter. One breaking release
ships the target contract and removes every retired destination spelling, flag,
command and routing section; afterwards each refuses before any backend access,
naming its target equivalent. A legacy-format operator file refuses whenever it
is read, naming `target import`, and a target-format file rejects legacy routing
sections. No file mixes the two formats, and no command reads a second file or
guesses a binding. A legacy `--cluster ROOT --deployment-id ID` invocation refuses
like every retired spelling; its replacement is an explicit root target under the
source-free lookup rules above, never a fresh source apply.

Older binaries ignore unknown operator keys; a format marker alone cannot make
them refuse safely. Use isolated operator homes, matched binary/config/script
upgrades, a nonsecret backup and explicit rollback instructions. Do not run old
binaries on migrated config expecting equivalent routing. This migration changes
no graph format or ledger; independent server/storage downgrade limits still apply.

Recovery from an absent or broken ordinary operator file uses an isolated, empty
`OMNIGRAPH_HOME` and an inline root, with the original ID and required
writer-quiescence evidence:

```sh
OMNIGRAPH_HOME=/tmp/og-recovery omnigraph cluster apply --target file:///srv/omnigraph \
  --deployment-id ID --writers-stopped --as ACTOR
```

An inline cloud root captures its account/endpoint settings once, as above, and
uses the existing authentication chain. This reads neither the broken ordinary
home nor a source bundle, without adding another address flag or weakening
admission checks.

## Alternatives

Keeping overlapping selectors or adding managed profiles alone leaves command
families with different selection rules. Requiring `--target` every time is
unnecessary when a deliberate config default exists; automation can still do so.
A `target` group limited to listing and editing operator config entries saves
hand-editing YAML and lets same-account tools register a target, and it adds no
selection state; resource or deployment groups would enlarge the surface without
improving the common plan/apply workflow. The chosen change unifies selection and
preserves operation-specific grammar.

Requiring registration for every destination adds friction to scratch graphs and
one-off commands without improving selection of an address the command line
already names, so an inline address keeps them without restoring a flag per
access kind. Using `--writers-stopped` as a mode selector conflates
safety with intent; use existing explicit source and identity arguments instead.
A new recovery flag or login command is unnecessary. Mixed old/new operator
formats and compatibility adapters are rejected: one breaking release is simpler
to reason about and test than an interval in which two resolvers coexist.
Privileged recovery requires the existing `--target` instead of adding confirmation
semantics to defaults. Its isolated-home procedure needs no separate literal-root
selector: the inline root is `--target` itself. The catalogue scope represents existing read-only behavior,
not a new command family or storage kind.

## Evidence and tests

Extend owners in [the test map](../dev/testing.md). Capture a focused baseline
before implementation, then assert destinations, credentials, requests and durable
effects independently of the routing declarations. Avoid a duplicate command
registry or tests that merely repeat the implementation.

| Gate | Required cases | Existing owners |
| --- | --- | --- |
| Selection and scope | Current Clap leaves and enumerated argument modes; direct graph/root/catalogue, server, managed account/cluster; explicit/default/missing/invalid target; file errors versus unrelated entry errors; graph defaults/root filters, catalogue and temporal refusals; unsupported modes; organization and local skill modes. The embedded command index is checked against Clap; retired spellings have parser refusal tests. | `operator.rs`, `scope.rs`, `planes.rs`, `cli_schema_config`, `cli_data`; `integration/parity_matrix.rs` for qualified direct/server parity only |
| Binding stability | Folder/config/default changes cannot redirect in-flight work. Decoy S3 endpoint/profile and Azure account/endpoint settings cannot redirect named targets on either storage path; an inline cloud address, `init URI` included, freezes one binding. Local path/file-URI equality, symlinks, remote advertised roots and Unix-socket identity. | Operator/scope tests, managed data tests, `cli_cluster`, `omnigraph-storage` and core Lance-access tests |
| Authentication | Server reference shared across different endpoints refuses before token access/transmission; no global bearer fallback; explicit anonymous access. Human/automation mechanism isolation; API/org/incarnation/principal checks; strict cache formats and bounded acquisition. Token bootstrap with absent/matching/mismatched endpoint; mismatch preserves cache. Org-less login with zero/one/multiple cached organizations and same/different-principal replacement; in-flight identity cannot switch. Logout and token clearing retain their distinct local/session effects. | Managed auth/data tests, server `auth_policy`, operator tests |
| Source modes | Same ID-only argv against root/server defaults never becomes source submission; explicit source is required with an ID. Matching/mismatched/evicted and valid preallocated IDs; missing attestation; default-selected recovery effects refuse. Bundle storage absent/matching/mismatched; co-located source/state; root/provider disagreement; preview/apply and pull. | `cli_cluster`, `cli_cluster_e2e`, `cli_schema_config`, cluster deployment tests |
| Bootstrap and uncertainty | Inline and `init URI` conflicts/default independence; account before cluster; control-only cluster; recovery with malformed ordinary home via isolated home. Lost response, persistence failure and exact request reuse/conflict. Legacy pending and accepted-only migration, crashes between copy/retirement, concurrent processes in one home and transfer across homes. Full capacity refuses; retired-history guard prevents no-key recreation. | Managed lifecycle tests, `cli_cluster` |
| Outcomes and output | Original-ID waits with concurrent successors; delivery reservation versus native acceptance; fresh convergence versus archives; cancel boundary; effective output on success/error; parseable streams; `--quiet` refuses as removed and nothing suppresses destination/outcome diagnostics. | `system_remote`, `cli_cluster`, `cli_data`, existing outcome tests |
| Compatibility | Complete selector inventory: every retired spelling refuses with its target equivalent before backend access. Legacy-format files refuse and `target import` converts them, keeping credential owners and endpoint scopes; name collisions, sole-graph migration and strict formats. Default selection refuses applicable retired environment/folder evidence; explicit targets cannot bypass lifecycle preservation. Old-binary/migrated-home protection is documented isolation plus a detectable marker, not an impossible old-binary refusal. Preserved source/parameter/pagination/safety contracts. | `operator.rs`, `planes.rs`, `cli_queries`, CLI integration suites |
| Inline targets | Name versus address, reserved `og_catalog`, relative-path guidance; direct scope by `--graph` and by root-only command, refusing a graph operation on a root without one; inline server credentials only under a configured owner's endpoint scope; invocation-local cloud capture reaching both storage paths; managed refuses inline; `init URI` equals the inline form. | `scope.rs`, `targets.rs`, `cli_data`, `cli_cluster` |
| Administration | Each edit keeps the rest of the file's text, refuses an incomplete, conflicting or uneditable entry and the default's removal, never stores a secret or sets the default, and serializes under the lock; `credential get` refuses a terminal. | `targets.rs`, `cli_data` |

Implementation evidence is recorded in
[PR #1090](https://github.com/ModernRelay/omnigraph/pull/1090), integrated with
Lance 13 and the current command tree. The CLI unit and process owners cover the
selection, refusal, recovery-custody and output matrix; direct/server parity also
asserts recorded actors. Focused regressions reproduce canonical endpoint hash,
protocol-discovery and pre-dispatch branch-diagnostic defects before their fixes.
The isolated native-provider matrix exercises control-object and Lance reads and
conditional creates against fake S3/Azure endpoints, including environment drift,
Azure's cached options and rotating native credentials. It does not claim live
cloud qualification.

An old/new binary probe writes and queries a graph with the predecessor CLI,
checks that the new CLI refuses its legacy operator file, imports that file
explicitly, and verifies the same rows through the migrated default. It checks
that the backup and isolated older home are preserved and that retired selectors
refuse. User docs, embedded help/skill and the breaking release note accompany the
implementation. Exact-head CI owns the full workspace, GQT, cloud-emulator and
storage-upgrade gates before merge. Repeat affected owners when the dependency
or command baseline changes; pre-Lance-13 checks do not qualify the merged code.

The served query-catalog gap [#653](https://github.com/ModernRelay/omnigraph/issues/653)
remains separate: reuse target resolution and current catalog authority, but do not
conflate catalog visibility with permission to invoke a query. New managed wrapper
support, live capability discovery, authorization preview, local/remote completion,
new recovery flags and generalized output/wait behavior are outside this change.

### CLI tests and GQT

Keep one owner per observable contract. The
[GQT runner](../../crates/omnigraph-gqt/README.md) already covers direct engine,
DST and declared served execution; its HTTP adapter sends requests directly and
its [served conformance suite](../../crates/omnigraph-gqt-served/tests/gqt_served_conformance.rs)
compares supported cases with in-process execution. It does not currently execute
the product `omnigraph` binary for each GQT step. The
[served smoke test](../../crates/omnigraph-cli/tests/integration/gqt_served_smoke.rs)
starts a real deployment and invokes the separate `omnigraph-gqt` runner. These
are complementary boundaries, not evidence that GQT covers product CLI routing.

| Contract | Test owner |
| --- | --- |
| Argument provenance, mode selection, target applicability and configuration refusals | CLI unit tests over captured inputs and prepared operations, extending existing parser/operator/scope/applicability owners. Expected destinations and refusals must be independently specified, not derived from the classifier under test. |
| Actual selected destination and credential, stdin/files, stdout/stderr, streaming, exit codes and submission/recovery behavior | Existing CLI subprocess and fault-proxy suites using hermetic `OMNIGRAPH_HOME` fixtures. Pure preparation tests do not replace these effects and process assertions. |
| Query rows, counts, result types and logical errors | Existing `.gqt` cases; direct/DST coverage retains its existing support boundaries. Do not copy the semantic corpus into CLI tests. |
| Supported GQ behavior through HTTP | Existing served GQT adapter/conformance suite, plus CLI parity tests for the product CLI's own encoding and decoding. Neither establishes support for unqualified managed wrappers. |

For example, a wrong aggregate result belongs in GQT. Correct rows returned from
the wrong destination belong in CLI tests with distinguishable intended and decoy
graphs. A command's invalid-target refusal must prove no credential acquisition or
backend request; a JSON-output test checks that destination diagnostics stay on
stderr and does not reproduce aggregation semantics. If a defect spans both the
engine and CLI, add assertions at both affected boundaries.

During migration, change CLI fixture construction in the existing support owner to
write target-format config into isolated operator homes, and pass explicit targets
in process journeys. Keep dedicated tests for configured defaults, inline
targets and retired-spelling refusals. Update actual product CLI setup calls used by served GQT smoke tests;
keep `.gqt` cases and the separate runner's execution-environment selectors
unchanged. Do not add a GQT CLI adapter or expand the GQT grammar for this RFC.
Run the affected CLI suites and served smoke/conformance owners; run the relevant
GQT cases whenever shared engine/query behavior changes, retaining the existing
separate CI gates. Optional command transcripts test help and interface examples,
not query semantics or durable recovery.

### Upstream design evidence

Kubernetes documentation and implementation were reviewed on 2026-10-14:

- [Contexts and kubeconfig](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/)
  inform complete bindings and an explicit per-command override. OmniGraph reuses
  its existing operator defaults instead of adding another selection command or
  current-context field. Multi-file merging and independent connection overrides
  are excluded because they complicate provenance and allow field splicing.
- [Shared client configuration](https://github.com/kubernetes/cli-runtime/blob/master/pkg/genericclioptions/config_flags.go)
  and [command construction](https://github.com/kubernetes/kubectl/blob/7795eab7acb6f8653133eb3f8b7743623ecc0b12/pkg/cmd/cmd.go)
  inform one resolver across commands. Freezing its complete result for an invocation
  is this decision's stronger requirement.
- [Usage conventions](https://kubernetes.io/docs/reference/kubectl/conventions)
  inform explicit selection in automation and parseable output; existing OmniGraph
  formats remain intact rather than copying every kubectl flag.
- [API dry-run](https://kubernetes.io/docs/reference/using-api/api-concepts/#dry-run)
  informs explicit validation/planning effects. Managed previews are durable
  operations, not local side-effect-free validation.
- [Rollout status](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_rollout/kubectl_rollout_status/)
  may follow a successor by default; OmniGraph retains exact-operation observation
  and finite operation-specific timeouts.
- [Apply's object loop](https://github.com/kubernetes/kubectl/blob/7795eab7acb6f8653133eb3f8b7743623ecc0b12/pkg/cmd/apply/apply.go#L530-L543)
  and [server-side apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
  are not adopted as graph publication or deployment mechanisms.

## Rollout

1. Land this accepted schema and the 0.14.0 breaking-release decision, with
   section-level dispositions on the earlier RFCs. This documentation-only stop
   leaves shipped behavior unchanged.
2. Build on the command/preparation boundaries already extracted in
   [PR #1047](https://github.com/ModernRelay/omnigraph/pull/1047). Complete the
   shared resolver, typed bindings and migration in
   [PR #1090](https://github.com/ModernRelay/omnigraph/pull/1090). Qualify all current
   commands, login/init bootstrap and same-ID behavior before enabling the new contract; unsupported combinations remain
   errors. Keep broad file moves separate from targeting changes, and move output
   handling incrementally while preserving streams and outcome contracts.
3. Ship one breaking release with the target contract, inline targets, the
   `target` group and `target import`. It removes the retired selectors, routing
   sections, `use`, `profile` and `--quiet`. Migrate operator homes, scripts,
   aliases and docs with it.
4. Mark implementation complete only after the command matrix, refusal/recovery
   evidence, user docs, help and release notes match the accepted contract.

## Unresolved questions

None for destination selection or the version 1 operator format. Live cloud and
identity-provider qualification retain their existing release requirements and
support boundaries; fake-provider tests do not replace them.

The designated breaking release is 0.14.0. Operators using older binaries must keep a separate operator home and must not share a migrated home with them.
The implementation's exact serialized fields are represented by
[`TargetEntry`](../../crates/omnigraph-cli/src/targets.rs). The only format marker
is `target_config_version: 1`. Managed account entries may omit `organization_id`
only for human-device login discovery. Saved server credentials carry their
endpoint scope. Managed pending/accepted records are kept in operator-owned
custody with a bounded record count and encoded size; reaching the limit refuses
new submissions while preserving observation and exact retries.

## Decision log

- **2026-10-27:** Accepted the organization-administration amendment using the
  released profile/target model. Add one `members set` operation, friendly
  organization-scoped subject selectors and durable exact request custody;
  retain existing invitation/offboarding verbs and explicit idempotency keys.
  No role catalog, local grant authority or new managed access-reporting surface.
- **2026-09-06:** Initial draft proposed managed variants in the profile/cluster
  resolver. It was not accepted or implemented.
- **2026-09-08:** The [historical deadline amendment](https://github.com/ModernRelay/omnigraph/commit/e8c616990eb96375554d52b20239dacdedada29c)
  proposed a 30-second managed query/mutation total deadline including the response
  body, retaining a 10-second connection deadline. It did not accept the wider
  resolver or credential proposal. The present revision preserves each current
  operation owner's bounds rather than reviving that historical transport change.
- **2026-10-14:** Replaced the September proposal after current CLI and kubectl
  review, preserving current identity and deployment authority. Earlier drafts
  remain in Git history.
- **2026-10-15:** Settled on `targets` plus `defaults.target` in existing operator
  config and a per-invocation `--target` override. Keep `cluster` and existing
  operation flags; `--config` supplies source only. Remove proposed resource/admin
  groups and separate selection state. Recheck all current command leaves and
  argument modes, including organization binding, cloud destinations, managed
  previews/deliveries and existing-flag recovery. This remains a draft decision.
- **2026-10-16:** Updated coverage to 75 leaves, including organization and local
  skill commands. Supersedes the earlier attestation-selected recovery mode with
  explicit source/ID mode selection and makes same-ID observation/nonexecution
  explicit. Allows first-login organization discovery and retains literal `init`
  bootstrap without additional commands or flags beyond `--target`. Retires the
  old quiet behavior, clarifies per-home legacy compatibility, corrects command
  grouping/path examples, defers
  completion and restores the standard RFC sections and linked deadline history.
- **2026-10-16, review follow-up:** Require explicit source for any ID-bearing
  submission and explicit targets for recovery effects. Define bundle-root
  assertions, endpoint-bound authentication, legacy custody preservation and bounded
  retirement. Add catalogue scope, isolated-home recovery and section-level RFC
  dispositions; keep existing command families and operation flags.
- **2026-10-17:** Specify one typed command-preparation path, immutable client
  bindings, family-owned modules and captured presentation with outer exit handling.
  Reuse existing libraries and test owners; keep optional diagnostic/transcript
  helpers small. Clarify CLI/GQT test ownership without adding commands or a second
  query-semantics corpus.
- **2026-10-18:** Make the change breaking: one release removes every retired
  selector, routing section and their commands, with no compatibility adapters
  and no mixed-format operator file; `target import` converts a legacy file and
  `--quiet` is removed rather than deprecated. Support inline targets: `--target`
  takes a storage path or URI or a server endpoint as a complete binding, scoped
  from the command line alone, with endpoint-scoped credentials and an
  invocation-local cloud capture; `init URI` is its positional form and managed
  destinations stay named. Add the `target` administration group and
  `credential get`, which edit and read operator config and the credentials file
  and never select a target or set the default. Pull request #1080 implements
  the schema, validation, endpoint-bound credentials and administration commands
  for direct and server targets.

- **2026-10-19:** Accepted the clean replacement for 0.14.0: version 1 targets,
  explicit server credential owners or anonymous access, and human-device or
  automation-env managed authentication. Remove the old command/parser/resolver
  implementations in the same release. Direct roots support graph operations
  through an explicit graph selection or target default, without discovery.
  Carry the selected provider binding into both control storage and Lance;
  preserve existing command, publication, admission and recovery owners.
  Reuse the schema and administration work from
  [PR #1080](https://github.com/ModernRelay/omnigraph/pull/1080).

- **2026-10-20:** Consolidated the accepted design into this canonical RFC before
  merging it independently of the runtime change. Replaces the remaining draft
  and conditional-acceptance wording with implementation qualification, records
  section-level dispositions on earlier RFCs, and updates the substrate baseline
  to Lance 13. Protocol discovery uses the `protocols` object; binding hashes use
  the same canonical endpoint identity as credential checks. Provider-option
  classification covers native environment refill and preserves native static
  versus refreshable authentication semantics. Specify per-API/organization custody
  limits and serialization within each operator home. [PR #1080](https://github.com/ModernRelay/omnigraph/pull/1080)
  is closed in favor of [PR #1090](https://github.com/ModernRelay/omnigraph/pull/1090),
  which owns the breaking implementation and its remaining qualification. Access
  reporting remains independent follow-up work. The second serving model is
  discontinued; useful MCP client logic may be proposed separately against the
  existing server.

- **2026-10-20, implementation:** Completed the target migration in PR #1090 on
  Lance 13, including canonical protocol-1 identities, separate target/credential
  protocol discovery, native provider-option confinement and truthful write-scope
  diagnostics. Incorporated the remaining actor-classification and parity-harness
  cleanup from PR #1059. Updated current command examples, migration guidance and
  the evidence above. Access reporting, optional error refinements and future MCP
  clients remain independent work.
