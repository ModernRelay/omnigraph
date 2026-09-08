# OmniGraph configuration and CLI framework

This reference explains what OmniGraph commands address, which configuration
and credentials they consume, what they may change, and how to keep that
surface coherent as it grows. It covers operation families and their important
options and omissions; the command declarations remain the exhaustive argument inventory.

The deployment unit is a **cluster**: one definition, one storage root, and
many graphs. An HTTP server is an access endpoint/process. A Kubernetes
cluster is a **cell**, not an OmniGraph cluster.

This document describes enduring configuration, operation and architecture
contracts. Implementation status is labeled separately from framework rules;
a proposed rule is not a claim of released support. The
[coherency proposal](../rfcs/0059-config-cli-coherency.md) owns the proposed
changes, compatibility analysis and rollout. The existing
[architectural invariants](invariants.md), accepted RFCs and implementation
remain authoritative; this guide does not accept the proposal.

The proposed framework at a glance:

```text
argv -> parse -> classify -> resolve -> credentials -> connect -> execute
                              |
  data:    address > --profile > env profile > defaults
  control: --config -> context or --direct
  local:   explicit command inputs

one owner/fact | no target fallback | explicit effects and outcomes
registry -> help/docs/coverage; independent fixtures -> behavior checks
```

## 1. Status and evidence

| Label | Meaning |
| --- | --- |
| Current | Inspected implementation at the stated commit; deployment and runtime qualification are separate. |
| Accepted work | Triaged and accepted feature scope; implementation and qualification remain separate. |
| Proposed | Intended behavior requiring acceptance, implementation and tests. |
| Deferred / Open | No implementation or decided behavior should be inferred. |

Implementation claims are scoped to the revisions in the
[source index](#14-source-index). A supported operation path does not imply
that every invocation or binary supports it; configuration variants, dispatch
rules and transport qualification are part of the support contract.

An open issue alone does not establish accepted scope. Untriaged proposals
must not become framework requirements by implication; an accepted issue
also does not establish released support or ratify a broader design.

A source reference establishes what was inspected. A test reference establishes
which executable assertion exists; a passing run additionally needs its
revision and result. Local, integration, adversarial and live evidence are
different scopes. Documentation and source inspection are not runtime proof.

Read [configuration](#3-configuration-contracts) for YAML,
[routing](#4-target-resolution) for target selection,
[commands](#6-command-and-operation-catalog) for capabilities, and
[invariants](#10-invariants) for the proposed enforcement contract.

## 2. Concepts, authority and independent axes

Hosting, execution mode, repository ownership, storage backend and transport
are independent. Direct Core can use a plain directory or a Git checkout,
with file or object storage. Managed execution can use an external Git
binding or service-managed Git. HTTP alone does not imply managed service
execution; S3 alone does not imply managed execution either.

| Fact | Authority / owner | Copies and consumers |
| --- | --- | --- |
| Requested cluster configuration | Team definition, or exact bound repository revision | Materialized files and exact config snapshots identify their source. |
| Effective cluster configuration | Core ledger and applied catalog | Status, planning and server boot read them. |
| Accepted graph schema and data | Graph schema/manifest and atomic content protocol | Cluster state carries the appropriate projection, including actor settings. |
| Managed cluster identity | Immutable root marker: cluster ID, incarnation, canonical root | Service DB and operator Cluster object are projections. |
| Retirement and execution evidence | Owning immutable guard/receipt protocol | Views retain provenance and do not manufacture evidence. |
| Control/data permissions | Service grants and the relevant applied server policy | Cached credentials are bounded authority, not a replacement policy. |
| Personal connection selection | Operator configuration and explicit invocation | No mutation of the deployment follows from editing a nickname. |
| Cluster-command binding | Exact selected directory's context | No parent search or implicit data default. |
| Credential material | Designated credential store/provider | A cache cannot select a different target. |
| Observed serving state | Replica boot witness and health/refresh observations | May lag the effective ledger and must identify that difference. |

Every fact has a named authority. Projections identify their source, while
immutable evidence retains the guarantees of its owning protocol.

A profile/alias name is mutable and can be persisted in personal settings.
Renaming it must preserve authoritative cluster, incarnation, graph and
principal identity. A matching URL or a similar storage path does not prove
those identities. Graph branches and Git configuration revisions are unrelated
selectors; an engine branch incarnation is not a cluster incarnation.

## 3. Configuration contracts

### 3.1 Surfaces and discovery

| Surface | Location / selection | Purpose and validation |
| --- | --- | --- |
| Operator YAML | `$OMNIGRAPH_HOME/config.yaml`, default `~/.omnigraph/config.yaml` | Connections, profiles, aliases, presentation and direct actor preference. Absent means empty layer; malformed errors when loaded. Current unknown keys warn. |
| Desired cluster | `DIR/cluster.yaml` and referenced files | Team configuration. Unknown authority keys refuse; absent desired files cannot be reconstructed from observation. |
| Folder context | `DIR/.omnigraph/context` | Versioned managed API/cluster binding for cluster commands; `DIR` defaults to `.`. |
| Legacy credentials | Operator credentials file and environment | Named-server chain, separate from YAML; file uses restricted permissions. |
| Control session | Separate OS-keychain namespace, bound to API origin | Intent API access; not graph-server or storage authority. |
| Data credential | Separate OS-keychain namespace, API + cluster key | Signed offline data access and endpoint metadata. |
| Applied deployment | Storage root, including `__cluster/state.json` and catalog | Core-owned achieved projection; not personal configuration. |
| Public server trust | Explicit boot trust file | Keys, issuer and cluster/root/incarnation binding; contains no private signing key. |
| Cell configuration | Terraform/platform configuration | Infrastructure; never a second graph-definition authority. |

No automatic parent-directory walk is implied for any surface.
`OMNIGRAPH_HOME` relocates a directory, not one arbitrary YAML file.
`--config` belongs to cluster operations and `use`; it is not a global
data-command configuration flag. There is no implemented top-level `config`
command, `profile use`, `current_profile`, or `OMNIGRAPH_CONFIG` override.

| Environment input | Scope |
| --- | --- |
| `OMNIGRAPH_HOME` | Operator-directory discovery. |
| `OMNIGRAPH_PROFILE` | Ambient data/tool profile, below explicit selection. |
| `OMNIGRAPH_TOKEN_<NAME>` | Legacy named bearer; uppercase name with hyphens replaced by underscores. |
| `OMNIGRAPH_BEARER_TOKEN` | Existing legacy bearer fallback. |
| `OMNIGRAPH_CONTROL_API` + `OMNIGRAPH_CONTROL_TOKEN` | Paired, explicitly origin-bound managed automation credentials. |

Backend and embedding-provider environment variables belong to their own
credential/provider contracts, not the CLI target-selection ladder.

### 3.2 Current operator YAML

Every top-level section below is optional. Illustrative URIs are not live
deployment instructions.

```yaml
operator:
  actor: act-andrew

defaults:
  output: table
  table_max_column_width: 80
  table_cell_layout: wrap
  server: staging
  default_graph: knowledge

servers:
  staging:
    url: https://staging.graphs.example

clusters:
  local:
    root: file:///Users/andrew/data/local-cluster

profiles:
  staging:
    server: staging
    default_graph: knowledge
  maintenance:
    cluster: local
    default_graph: knowledge
  scratch:
    store: file:///Users/andrew/data/scratch

aliases:
  person:
    server: staging
    graph: knowledge
    query: get_person
    args: [slug]
    params: {include_archived: false}
    format: json
```

A profile selects exactly one of `server`, `cluster`, or `store`.
A store already identifies one graph and cannot have `default_graph`.
Flat defaults select server or store, never both. There is no current
`defaults.cluster`. Output values are `table`, `kv`, `csv`, `jsonl`,
`json`, `arrow`; table layout is `truncate` or `wrap`. These preferences
do not become query/schema/policy content. Tokens never belong in this YAML.

### 3.3 Proposed cluster-first connections

A proposed discriminated connection variant supports cluster-first naming.
This example requires implementation support; it is **not accepted by the
source baseline listed in this reference**:

```yaml
clusters:
  pilot:
    managed: true
    id: CLUSTER_ID
    api: https://control.example
    endpoint: https://pilot.graphs.example
  local:
    managed: false
    root: file:///Users/andrew/data/local-cluster
  archive:
    # Omitting managed retains the existing storage-root form.
    root: s3://example-bucket/archive

profiles:
  pilot:
    cluster: pilot
    default_graph: knowledge

aliases:
  pilot-person:
    cluster: pilot
    graph: knowledge
    query: get_person
    args: [slug]
```

| Variant | Required | Rejected |
| --- | --- | --- |
| `managed: true` | `id`, `api`, `endpoint` | `root`, missing/invalid authority fields, plaintext credentials. |
| `managed: false` or omitted | `root` | Managed `id/api/endpoint` fields. |
| Legacy `servers.NAME` | `url` | No implicit managed authentication based on matching URL. |
| Managed alias | Managed `cluster`, explicit `graph`, stored `query` | Simultaneous `server`, root-backed cluster, global target overrides. |

The canonical examples use YAML booleans `true` and `false`; no alternate
`yes` spelling is promised. Recognized authority variants validate strictly.
Existing ordinary preference warnings remain a separate compatibility rule.

The boolean selects a connection/authentication contract. It does not create,
enroll, transfer, authorize or resize a deployment. Existing `servers:`
remain for compatibility; an endpoint is still necessary even when a cluster
is the name the user selects. A future self-hosted HTTP cluster association
is not part of the proposed connection model.

### 3.4 Desired files and managed context

The current desired definition has top-level `version`, `metadata`,
`storage`, `state`, `providers`, `graphs`, and `policies`. Referenced
schemas, queries and policy remain team-owned files. Omitting `storage`
uses the config directory as the local storage root. Do not put operator
connections or tokens into this definition.

Context is a distinct, unchanged shared binding:

```yaml
version: 1
cluster: CLUSTER_ID
api: https://control.example
```

Managed external Git execution uses a pushed immutable revision. Local edits
are not implicitly uploaded. Service-managed repository upload creates an
immutable Git revision before planning. Exactly one config binding is
authoritative; applied state and cached snapshots are not replacement desired
definitions.

## 4. Target resolution

### 4.1 Target classes and data pipeline

| Class | Address | Capability boundary |
| --- | --- | --- |
| H | Legacy HTTP endpoint | Server route and ordinary credential contract. |
| G | One graph storage URI | Embedded operations with storage authority. |
| R | Cluster root/config-root locator | Existing direct maintenance/catalog tooling. |
| M | Proposed named managed data connection | Qualified HTTP operations with a separately bound data credential. |
| C | Desired config directory/context | Direct Core or managed cluster-command dispatch. |
| L | Local/session operation | Explicit operation arguments; no ambient graph target. |

For data/storage commands, the intended normal order is:

```text
supported explicit address primitive
  -> explicit --profile
  -> OMNIGRAPH_PROFILE
  -> flat operator defaults
  -> missing-target refusal
```

Pure syntactic conflicts and command applicability are checked first. A
selected invalid or unsupported target **refuses**; it does not disappear
from the ladder. Explicit selection shadows unused ambient inputs. Local
commands and aliases have their own input contracts.

`--server NAME` resolves a legacy name; a literal HTTP URL remains legacy.
`--store URI` identifies one graph. `--cluster VALUE` resolves a named root
or an existing literal path/root; the proposal additionally recognizes a
named managed variant. A cluster-root data query is not added as a side
effect. Two explicit primitive addresses conflict.

An explicit graph overrides the selected profile's graph default. An unused
profile cannot lend its graph default to an explicit primitive. Profiles do
not inherit unrelated flat graph defaults.

### 4.2 Graph omission

| Scope | Omitted graph |
| --- | --- |
| G | Already one graph; extra `--graph` refuses. |
| H, graph operation | Selected profile/default graph, otherwise existing best-effort registry probe. |
| H, `graphs list` | Registry operation; rejects explicit graph and does not use a profile graph default. |
| R, maintenance | Consumed profile graph, otherwise sole applied graph; zero/multiple candidates refuse. |
| R, policy/queries | Own catalog/bundle helper; currently ignores profile graph defaults. |
| M, proposed graph operation | Selected profile graph or explicit graph required; never choose from grants. |
| M, future registry operation | No graph selector; separately qualify registry scope and result filtering. |
| C | Definition/context identifies cluster; token issuance explicitly selects a graph. |

The current H probe rejects **any nonempty** registry, including exactly one
graph. If the probe fails, is denied/unsupported, or returns no graphs, legacy
behavior proceeds to the bare endpoint. This is not sole-graph selection and
is not a new managed fallback rule.

### 4.3 Cluster/config-directory pipeline

```text
cluster COMMAND --config DIR
  -> --direct: bypass context, use supported Core path
  -> valid DIR/.omnigraph/context: managed dispatcher
  -> absent context: supported Core path
  -> malformed context: refusal
```

Managed-only commands still refuse without their managed preconditions;
`--direct` cannot convert them into Core commands. Unmerged lifecycle extensions have separate requirements in the
[coherency proposal](../rfcs/0059-config-cli-coherency.md).

A data profile never redirects `cluster plan --config DIR`. Conversely,
context never supplies the proposed normal data destination. No merge of
operator and context API/ID fields takes place. `--direct` plus a proposed
managed data entry conflicts; it cannot downgrade credentials or discover
storage.

### 4.4 Access forms and support

The source baseline's managed query/mutate path uses the exact working
directory's context, an explicit graph and a separately cached data credential.
Named managed connections and context-independent data resolution are
Proposed. Direct/legacy examples use explicit `--direct` to select their
existing addressing contract.

Keep support for an access form separate from the enduring routing rules:
select one target, validate its capability, bind the appropriate credentials,
and execute only through that target's protocol. Dispatch exceptions and transition procedures belong in
[RFC 0059](../rfcs/0059-config-cli-coherency.md).

## 5. Credentials, policy and provenance

| Route | Binding and lookup | Missing credential / prohibited fallback |
| --- | --- | --- |
| Direct | Filesystem/backend authority; direct actor from `--as` then operator preference | A control session or data token provides no storage authority. |
| Legacy HTTP | Named keyed environment, then credentials file, then legacy bearer environment | Server's existing authenticated/unauthenticated behavior remains; no switch to managed credentials. |
| Managed control | API-origin-bound session or explicitly bound automation credential | Refuse; never send this credential to a graph server. |
| Managed data | Selected managed connection matched to separate API+cluster cache | Refuse absent/expired/mismatched/insufficient credentials; never use legacy bearer or storage fallback. |

The authentication variant determines the namespace; flag/profile/environment
is selection provenance. Legacy `OMNIGRAPH_TOKEN_INTEL_DEV` and
`OMNIGRAPH_BEARER_TOKEN` remain valid within the legacy chain, including the
existing matching-name lookup for literal URLs.

For proposed named managed connections, require exact canonical
API/cluster/endpoint agreement before sending a credential. The source
baseline's context-based credential path compares API/cluster identity and
validates endpoint syntax, then uses the cached endpoint; it does not yet
compare against an independently selected operator endpoint.

Managed API/endpoint origins require HTTPS, except exact supported loopback
hosts (`localhost`, `127.0.0.1`, `::1`) may use HTTP. Reject userinfo,
non-root paths, query and fragment; compare canonical origins, not prefixes.
Managed requests reject redirects. Existing finite request/response bounds
remain; the current 8 MiB JSON limit is not an export-streaming design.

For proposed managed query/mutation connections,
[CC-11](../rfcs/0059-config-cli-coherency.md#8-credentials-actor-and-transport)
sets a 10-second connection deadline within a 30-second total request deadline,
including the complete response body. The inspected baseline instead has a
10-second total deadline. The proposal retains the 8 MiB response bound and
prohibits automatic retry, credential fallback, and data requests to the Intent
API. A submitted mutation's timeout leaves its outcome unknown. These proposed
bounds do not qualify other managed operations or change legacy transport.

The server verifies signed data credentials offline: signature, issuer,
audience, expiry and cluster/root/incarnation binding, then action grants
intersect applied Cedar permissions. Control permission does not imply data
permission. A data grant alone does not enroll a principal in Cedar policy.

| Data action | Required grant in addition to policy |
| --- | --- |
| Ad-hoc read / ordinary metadata read | `read` |
| Stored read | `read` + `invoke_query` |
| Ad-hoc mutation/load | `change` |
| Stored mutation | `change` + `invoke_query` |
| Load creating a branch | `change` + `branch_create` |
| Export / change baseline | `export` |
| Branch create/delete/merge | Respective branch action; optional deletion is separate. |
| Registry listing | `graph_list`, with authorized result filtering. |
| Schema apply | Refused by this signed-token profile. |

Permissions do not establish CLI route support. Managed connection eligibility
and qualification are declared separately for each command in the catalog.

Direct writes retain explicit/default attribution. Authenticated HTTP writes
use the server-resolved principal and reject explicit actor override where
required. Actor nodes record provenance, never permission. Actor
materialization defaults on for new graphs; existing graphs need explicit
planned enablement. Disabling retains existing actors and commit attribution.

## 6. Command and operation catalog

### 6.1 Graph and storage operations

H/G/R below describe existing operation paths. Access-form and dispatch
support are separate constraints, as described in section 4.4. The managed
column records proposed eligibility, not released support.

| Command | Current target | Important contract | Managed connection proposal |
| --- | --- | --- | --- |
| `query` / `read` | H; G for ad-hoc source | Stored name requires H; `--query FILE` or `-e GQ`; branch/snapshot conflict. | Proposed named-connection support. |
| `mutate` / `change` | H; G for ad-hoc source | Write query; optional `--if-commit` exact head precondition. | Proposed named-connection support. |
| `load` | H/G | `--data FILE --mode merge\|append\|overwrite`; optional `--branch`, explicit `--from` to create missing branch. | Later qualification. |
| Hidden `ingest` | H/G | Separate legacy command with permissive merge/from-main defaults. | Requires independent qualification. |
| `snapshot` | H/G | Read graph snapshot; optional branch. | Later qualification. |
| `export` | H/G | JSONL snapshot, optional branch/type filters; output effects. | Streaming qualification. |
| `blob get`, `blob stat` | H/G | Logical Blob cell selection and bounded/streaming output. | Streaming qualification. |
| `branch list` | H/G | Read branch inventory. | Later qualification. |
| `branch create/delete/merge` | H/G | Create `--from` and merge `--into` default main; deletion and merge separate effects. | Later qualification. |
| `commit list/show/changes` | H/G | History and changes at explicit positions. | Later qualification. |
| `changes poll` | H/G | `--start` defaults now; conflicts with `--cursor`; preserve cursor/pagination contract. | Later qualification. |
| `changes baseline` | H/G | Required `--out`; existing POSIX durable-output restrictions. | Streaming/platform qualification. |
| `schema show` / `schema get` | H/G | Read accepted schema. | Later qualification. |
| `schema apply` | H/G | Existing HTTP policy rules; direct cluster-owned graph refuses. | Unsupported signed-token action. |
| `graphs list` | H | Registry scoped; rejects graph/store/actor selectors. | Later registry qualification. |
| `init` | Positional graph URI | `--schema FILE URI`; no `--json`; `--force` is not overwrite of an existing graph. | Unsupported. |
| `schema plan` | G; R through profile exception | `--schema FILE`, optional `--allow-data-loss`; explicit cluster/graph rejected today. | Unsupported. |
| `optimize` | G/R | Physical maintenance, not a data read or deployment. | Unsupported. |
| `rebuild-full-text-indexes` | G/R | Explicit analyzer/index rebuild; branch-scoped, custom analysis implications. | Unsupported. |
| `repair` | G/R | Preview without `--confirm`; `--force` requires confirm. | Unsupported. |
| `cleanup` | G/R | At least one retention selector (`--keep`, `--older-than`); confirmation controls execution. | Unsupported. |
| Graph-backed `lint` | G/R | Validates query against graph schema. | Unsupported. |
| `policy validate/test/explain` | R | Applied policy source; tests/actor/action selectors are command-specific. | Unsupported tooling API. |
| `queries list` | R; served CLI mapping is accepted work, not implemented | Direct listing reads the applied catalog; served listing uses the existing active-catalog HTTP endpoint. | Separate managed connection qualification required. |
| `queries validate` | R | Validate the applied stored-query catalog through the existing direct helper. | Unsupported tooling API. |
| `alias NAME [args]` | Own H binding | Stored read only; binding rejects global target overrides. | Additive managed alias, separately qualified. |

**Accepted work: served catalog listing.** The accepted
[CLI listing issue](https://github.com/ModernRelay/omnigraph/issues/653)
adds `queries list --server NAME --graph ID --json`, using the existing
`GET /graphs/{graph}/queries` endpoint and normal server/profile/default/token
resolution. Preserve direct root listing and keep `queries validate` separate.
The accepted mapping is not present in the inspected CLI source.

Output must have a stable documented JSON shape and reflect the server's
active catalog, including operation names/kinds and metadata the endpoint
actually supplies. Do not invent richer descriptors or another registry.
Preserve the endpoint's read authorization; listing an operation does not
grant permission to invoke it. Managed named-connection support requires its
own credential/route qualification, independently of this accepted HTTP CLI
mapping.

`--params` and `--params-file` are exclusive. Parameterize GQ instead of
interpolating user values. A named query is a served catalog invocation, not
an implicit local filename.

### 6.2 Cluster operations

Every row uses `--config DIR` (default `.`) unless otherwise stated.

| Command | Direct Core | Managed context |
| --- | --- | --- |
| `validate` | Validate desired files. | Currently unsupported; `--direct` is explicit local validation. |
| `plan` | Plan against ledger; takes lock unless `--observe`. | Creates saved plan run; optional pushed `--rev`, otherwise bound head; no upload; observe refuses. |
| `apply` | Apply desired configuration through Core. | Exact `--plan RUN_ID` required; permission-based execution. |
| `status [RUN_ID]` | Reads ledger (no managed run argument). | Cluster projection or exact run. |
| `observe` | Read-only observations, no sweep or ledger write. | Unsupported. |
| `refresh` | Recovery/observation update of existing ledger. | Unsupported CLI route. |
| `import` | Initialize missing ledger from declared existing graphs. | Unsupported CLI route. |
| `approve RESOURCE` | Existing actor-bound destructive-resource artifact. | Unsupported; no separate managed approval. |
| `force-unlock LOCK_ID` | Exact lock recovery, existing safeguards. | Unsupported; never a writer fence. |
| `history` | Managed-only refusal. | Limit default 100, 1–1000; optional RFC 3339 since. |
| `cancel RUN_ID` | Managed-only refusal. | Cancel pending run or abandon unused saved plan. |
| `token` | Managed-only refusal. | Explicit graph/actions; TTL default 3600 seconds, range 60–86400; clear is separate. |

Managed run waits default to 300 seconds, timeout range 1–3600;
`--no-wait` returns after acceptance. Reuse `--idempotency-key` and the
same request for supported managed run replays. Wait timeout is not
cancellation and does not establish that submission failed.

Managed authorization is the caller's current scoped action permission for
the exact plan. Direct `approve` artifacts and `--yes` prompt behavior are
existing compatibility surfaces, not a second managed approver or per-action
step-up.

### 6.3 Lifecycle support boundary

Self-service lifecycle extensions are outside the merged CLI described here.
Do not infer support for `cluster create`, `push`, `delete`, `undo-delete`,
`list`, `pull`, `bind`, `restore`, `eject`, operation recovery or a standalone
purge verb from the control-command catalog. The
[coherency RFC](../rfcs/0059-config-cli-coherency.md) records the compatibility
requirements for lifecycle work.

Current serving refuses an empty applied cluster. Lifecycle status,
provisioning completion and a usable graph endpoint are distinct facts.

### 6.4 Local/session operations

| Command | Read/write contract |
| --- | --- |
| `version`, `--version` | Binary information, no target. |
| `profile list` / `show [NAME]` | Read profiles; show omission uses environment profile then defaults, no sticky selection. |
| `login SERVER` / `logout SERVER` | Legacy local credential entry; logout cannot unset another process's environment. |
| `login --api ORIGIN` / `logout --api ORIGIN` | Native device authorization / control-session revocation and local removal. |
| `use CLUSTER_ID --api ORIGIN --config DIR` | Validate and write exactly that directory's managed context; no operator-default mutation. |
| `embed` | Explicit seed/input/output/spec pipeline; no graph target. |
| `lint --schema FILE --query FILE` | Offline validation; current accepted-but-unused scope flags are a known inconsistency. |

### 6.5 Argument details and output

Global flags are `--direct`, `--as`, `--server`, `--graph`, `--profile`,
`--store`, `--cluster`, `--yes`, and `--quiet`. Global parsing does not make
every flag applicable to every command. **`--json` is command-local**;
`init` and `policy` do not expose it. Information flags include lowercase
`-v` / `--version` and the standard help surface.

| Operation | Additional exact argument shape / omission |
| --- | --- |
| `query` | `[NAME] [--query FILE \| -e/--query-string GQ] [--params JSON \| --params-file FILE] [--branch B \| --snapshot ID] [--format FORMAT \| --json]`. |
| `mutate` | Same source/params; `--branch`, `--if-commit`, `--json`; no snapshot or format flag. |
| `branch` | `create NAME`, `list`, `delete NAME`, `merge SOURCE`; address override is `--uri URI`, not positional URI. Merge has `--into TARGET` and `--delete-branch`. |
| `blob get/stat` | `node\|edge TYPE ID PROPERTY [--branch B \| --snapshot ID]`; get adds offset, positive length and out; stat adds JSON. |
| `commit` | `list [URI] [--branch B]`; `show ID [--uri URI]`; `changes ID [--uri URI] [--limit N] [--page-token TOKEN]`. |
| History/feed filters | Repeated `--kind node\|edge`, `--type T`, `--op insert\|update\|delete` where declared. Commit changes with page-token requests one page; otherwise auto-paginates. Feed polling consumes pages automatically. |
| `changes poll` | `--uri`, `--branch`, `--cursor` or `--start now\|beginning\|after:ID`, limit and filters. |
| `schema plan/apply` | Optional positional URI, required schema file, optional allow-data-loss and `--actor-provenance true\|false`; omission preserves accepted actor setting. |
| `init` | Required positional URI/schema; omitted actor-provenance defaults on. |
| `policy` | `test --tests FILE`; `explain --actor ACTOR --action ACTION [--branch B] [--target-branch B]`. Policy-test actors are inputs to evaluation, not authenticated actor overrides. |
| `embed` | `--seed FILE` or `--input FILE --output FILE --spec FILE`; clean/reembed-all conflict; repeatable type/select; JSON available. |
| `cluster plan` | `--rev` has alias `--revision`; omission uses bound head only on the managed route. |
| Legacy spellings | `read`, `change`, hidden `schema get`; separate hidden `ingest`; argv rewrites `check`, `query lint`, `query check`; hidden accepted `export --jsonl`. |

Read-format priority is explicit format/JSON, applicable alias format,
operator output, then table. Table width defaults to 80 and layout to
truncate. Fixed or streaming command outputs retain their own contracts.
`--quiet` suppresses the existing write-target diagnostic, not failures.
`--yes` bypasses supported destructive-write prompts; it grants no permission.

## 7. Operational workflows

Direct/legacy examples explicitly select their addressing contract with
`--direct`. Managed and proposed named-connection examples are labeled
separately. Check the deployed binary's support and operation-specific help
before using an example.

### 7.1 Current direct cluster and legacy data work

```sh
omnigraph --direct cluster validate --config ./cluster
omnigraph --direct cluster plan --config ./cluster --json
# After reviewing the plan and satisfying the existing direct requirements:
omnigraph --direct cluster apply --config ./cluster --json
omnigraph --direct cluster status --config ./cluster --json

omnigraph --direct query get_person --server staging --graph knowledge \
  --params '{"slug":"alice"}' --json
```

An apply may be partially converged; inspect its outcome and remaining plan,
rather than treating exit/wait completion as deployment readiness. Restarting
serving follows the existing exclusion/drain protocol independently of client
routing.

### 7.2 Current managed control and data; proposed named data selection

```sh
omnigraph login --api https://control.example
omnigraph use CLUSTER_ID --api https://control.example --config ./cluster
omnigraph cluster plan --config ./cluster --rev PUSHED_COMMIT --json
omnigraph cluster apply --config ./cluster --plan PLAN_RUN_ID --json
omnigraph cluster status --config ./cluster --json
omnigraph cluster token --config ./cluster --graph knowledge \
  --actions read,invoke_query
```

Current managed query/mutate use cwd context; `--config` is not their flag.
For this access form, run from the bound directory and specify graph:

```sh
cd ./cluster
omnigraph query get_person --graph knowledge --params '{"slug":"alice"}'
```

The proposed named-connection form uses the YAML above to select a data target
explicitly from any directory:

```sh
# Proposed; requires named managed-connection support.
omnigraph query get_person --profile pilot --params '{"slug":"alice"}'
omnigraph query get_person --cluster pilot --graph knowledge \
  --params '{"slug":"alice"}'
```

Plan/apply do not mint a data token. Logout or token-clear does not revoke an
already issued offline credential at the server. No implicit token refresh or
control API dependency is part of ordinary data requests.

### 7.3 Current data review branch

```sh
omnigraph --direct load --store file:///Users/andrew/data/scratch \
  --data data.jsonl --mode merge --branch review --from main --json
omnigraph --direct query --store file:///Users/andrew/data/scratch \
  --query queries/review.gq --branch review --json
```

Verify the intended branch/content before an explicit merge. If a write
response is lost, inspect branch head or intended effects first. An optional
`--if-commit` is an optimistic precondition, not a generic replay key.
Merge followed by refused deletion is a partial outcome with two effects.

### 7.4 Failure and recovery decisions

| Observation | Next action / boundary |
| --- | --- |
| Unknown/malformed selected connection | Correct that selection; never substitute another target. |
| Missing/expired managed credential | Explicitly mint matching access through the control flow. |
| Endpoint metadata mismatch | Explicitly update/remint matching metadata; never let cache choose. |
| Permission denial | Inspect relevant control grants or data grants and applied policy; actor rows grant nothing. |
| Quarantined/missing graph | Use authorized status/recovery for the addressed resource; don't route around it. |
| Control API down, valid data token | Data may continue within its offline lifetime; control actions fail independently. |
| Submitted managed run, wait timeout | Retain run/key, query status, replay same request only where supported. |
| Data write timeout | Unknown outcome; reconcile head/effects under that operation's contract before retry. |
| Held/stale-looking lock | Inspect owner and existing recovery protocol; age or force-unlock is not writer exclusion. |
| Effective/booted revisions differ | Report lag and follow activation protocol; do not rewrite the witness to hide it. |

## 8. Effects, recovery and serving

### 8.1 Effect ownership

| Operation | Owned effects |
| --- | --- |
| Operator edit / use / login | Respective operator file, context, or credential store; never each other's authority. |
| Read/observe/plan | No intended data mutation; declare locking, local output and any underlying recovery effects separately. |
| Data mutation/load/branch operation | Graph publication, commit and provenance through the existing atomic protocol. |
| Cluster apply | Successful resource outcomes, statuses, ledger CAS/revision and declared artifacts; partial progress is durable. |
| Refresh/import | Existing recovery sweep and declared-graph observations, including inserted/changed schema and graph digests. |
| Managed plan/apply | Exact source capture, run/lease/offer/receipt and authorized execution effects. |
| Managed lifecycle extension (outside merged CLI; section 6.3) | Provisioning/retention/retirement checkpoints and exact authorized resources, by the lifecycle protocol. |
| Maintenance | Explicit physical/index/retention effects under its existing storage/exclusion requirements. |
| Server startup | Applied snapshot read; graph opening may complete graph-level recovery, not rewrite cluster ledger. |

Open mode is part of **operation × transport**, not derivable from a read
label. At the inspected baseline, direct branch listing, snapshot, commit
reads, baseline, export and `queries validate` use read-write engine opening
and may perform recovery. Schema show and graph-backed lint explicitly use
read-only opening. Blob output, embedding and baseline can write local files;
baseline has a durable output/locking protocol. A future stronger read-only
guarantee needs an explicit implementation change and qualification.

Only convergence advances apply's requested `config_digest`; changed achieved
resources and `state_revision` can advance earlier. Import can initialize a
missing ledger from existing declared graphs, even if no prior cluster apply
created them. Refresh can record changed observed schema and graph digests.
These Core transitions retain their operation-specific authority and effects
independently of connection selection.

### 8.2 Server surface

`omnigraph-server` is a separate binary; it does not load personal profiles.

| Input / omission | Current behavior |
| --- | --- |
| `--cluster DIR` | Read definition to locate storage, then boot applied ledger/catalog. |
| `--cluster file://...`, S3 or Azure root | Direct applied snapshot from root; no desired-file reconstruction. |
| No cluster argument | Refuse; current server is cluster-only. |
| `--cluster pilot` | Directory name, not an operator connection lookup. |
| `--bind` omitted | `127.0.0.1:8080`. |
| `--data-token-trust FILE` | Opt-in public signed-token trust bound to actual root. |
| Trust omitted | Existing static auth/policy behavior; no automatic managed token support. |
| No configured protection or explicit unauthenticated mode | Refuse unprotected startup. |
| `--require-all-graphs` omitted | Healthy graphs may serve alongside graph-local failures; empty/all-unservable still refuses. |
| Shutdown grace omitted | Environment override then 25 seconds; orchestration grace must be longer. |

The process boot witness is fixed; current ledger status can be newer.
Restart activates a new cluster catalog revision under current behavior.
Graph-level data publication/recovery is a separate protocol. A lock, lease,
heartbeat, placement decision or bounded shutdown does not establish a
distributed graph-writer fence. This framework claims no new G9/G11 proof.

## 9. Proposed coherence framework

### 9.1 Layers and their owners

| Layer | Input → output | Boundary |
| --- | --- | --- |
| Parse | argv → typed command/arguments | Syntax and syntactic conflicts only. |
| Classify | command → capability and option applicability | Pure; no context/keychain interception. |
| Resolve | applicable addressing → one typed target + selector provenance | Explicit/ambient precedence; no credential-presence or target-substitution logic. |
| Bind authority | resolved authentication variant → exact allowed credential | Separate namespaces; selected identity validated before credential-bearing request. |
| Connect | target + authority → bounded transport/client | Central policy for origins, redirects, deadlines and response contracts. |
| Execute/present | command + client → effects, outcome and output | Per-operation semantics; do not rebuild addressing here. |

Cluster/config-directory commands have a distinct declared dispatch pipeline.
Local/file/session commands likewise have explicit inputs. Shared error and
inventory machinery does not imply one global target ladder.

Graph derivation/probing is command-specific and declared: legacy registry
probing needs a request, maintenance sole-graph selection needs applied state,
and managed graph selection must not inspect grants. Do not place these under
a blanket assertion that every selector is resolved without I/O.

### 9.2 Registry contract

A future registry lives with the owning CLI code, not in another handwritten
YAML authority. It declares:

- Commands/subcommands and compatibility spellings, capability/target variants,
  supported status and owner evidence.
- Options, positional forms and environment inputs; applicability, conflicts,
  precedence, intentional shadowing, defaults and omission behavior.
- Configuration surfaces, variants, discovery, validation and authority owner.
- Credential namespace/binding, permission actions, transport contract and
  provenance source.
- Intended writes, lock/recovery/output effects, failure classes, uncertain
  outcomes and replay/precondition contract.
- Output formats and compatibility/removal boundaries.

The registry generates help/reference coverage and test-case enumeration.
**Expected routing and safety outcomes remain independently authored contract
fixtures.** Generating both implementation and expected answers from the same
precedence list would let a wrong rank validate itself.

Undeclared commands/options should fail coverage checks after adoption.
Current capability/help tests are partial coverage, not this full registry.
A feature may need a new architecture boundary; it needs a reviewed design
rather than an unregistered pre-dispatch special case.

## 10. Invariants

These are framework requirements, not a claim every current command already
enforces them. F1–F13 are stable identifiers for review and verification.

1. **F1 — Configuration changes use the owning protocol.** Cluster apply owns
   planned configuration changes; partial outcomes, recovery, constrained
   refresh/import and graph data writes retain their distinct contracts.
   Credentials and routing edits cannot change cluster configuration.
2. **F2 — State is observable through authorized projections.** Declare a
   permitted read path for operationally relevant state and its provenance.
   Requested/effective/observed may differ; disclose lag, not all state on
   every API. Readiness stays minimal.
3. **F3 — The surface is registered.** Each command, argument, compatibility
   rewrite and config surface has one registry declaration after adoption.
   Generated coverage and independent expected outcomes detect drift.
4. **F4 — One authority owns each fact.** Name desired, effective, graph,
   identity, guard, policy, routing and credential domains explicitly.
   No projection silently becomes another domain's authority.
5. **F5 — Applicable explicit selection beats ambient selection.** Preserve
   explicit address/profile precedence within each pipeline. Unrelated
   context cannot gate or redirect explicit data commands under the framework.
6. **F6 — Addressing chooses the target.** Credentials/grants never select it.
   State-derived graph selection occurs only on declared operation paths and
   has explicit zero/one/multiple behavior.
7. **F7 — Credentials respect their authentication variant.** Managed
   credentials bind to exact selected identity/origin and never cross
   namespaces. Preserve legacy lookup and direct attribution contracts.
8. **F8 — Applicable inputs have declared treatment.** Consume, conflict,
   explicitly shadow, or report a documented compatibility exception.
   Locally decidable errors precede target requests and operation effects;
   reading local configuration is permitted. Remote authorization precedes
   protected effects. Unknown post-submission outcomes are not refusals.
9. **F9 — Unsupported selected targets refuse.** No fallback to lower-ranked
   targets, storage, different endpoints or another authentication mode.
   Older clients fail closed on new managed variants.
10. **F10 — Effects and retries are bounded by operation.** Declare lock,
    recovery, artifact and output writes. Lost responses are unknown.
    Reuse exact keys/requests only where that protocol supports replay;
    otherwise reconcile effects under its existing contract.
11. **F11 — Mutable names do not redefine durable identity.** Personal files
    may persist names. Alias renames preserve authoritative identities;
    similar URLs, paths or names are not identity proof.
12. **F12 — Serving resources come from applied state.** Directory boot may
    read desired config solely to locate storage. Report the fixed boot
    witness; preserve graph-opening recovery and current restart activation.
13. **F13 — Compatibility and evidence are explicit.** Parsing, routing,
    defaults, permissions, effects, wire format and Rust source compatibility
    are separate claims. Source inspection, existing tests and executed
    qualification are labeled separately.

## 11. Antipatterns

| Antipattern | Why it fails | Required alternative |
| --- | --- | --- |
| Cwd file globally gates commands | Unrelated folder state can block or redirect an explicit operation. | Separate declared pipelines and explicit precedence. |
| Unsupported selected source is skipped | Can execute against a different cluster/store. | Refuse the selected target. |
| Cached token selects endpoint | A credential cache becomes routing authority. | Selected connection plus exact metadata validation. |
| WorkOS login implies data/storage access | Collapses independent permissions. | Explicit credential/action contracts. |
| Managed error falls back to bearer env | Crosses authentication boundary. | Preserve error; explicit legacy selection is separate. |
| One token grant chooses graph | Permissions become addressing. | Explicit/profile graph, or declared non-token derivation. |
| Root entry silently enables query | Widens storage capabilities through a connection change. | Preserve R capability boundaries. |
| Every status API exposes entire ledger | Leaks fields and hides projection semantics. | Authorized views with provenance and lag. |
| Only converged applies may persist progress | Loses achieved partial-state truth. | Existing per-resource outcome and ledger protocol. |
| Refresh/import may only delete digests | Contradicts existing observation/adoption. | Preserve constrained Core transitions. |
| Alias rename must change no persisted bytes | Personal config necessarily stores aliases. | Preserve authoritative identity, not byte equality. |
| Every failure occurs before requests | Makes remote validation impossible. | Distinguish local refusal, remote refusal, partial/unknown outcome. |
| Retry every write with an idempotency key | Several protocols do not support one. | Operation-specific replay or effect reconciliation. |
| Read command implies zero physical activity | Opening/recovery/output/locks may have effects. | Declare effects and keep pure observation explicit. |
| Lease/lock/termination timeout called a fence | Overstates writer-exclusion evidence. | Existing proof requirements and named limitations. |
| Same generator supplies resolver and expected tests | A wrong rule passes its own test. | Independent contract assertions. |
| New registry silently cleans up old quirks | Creates unreviewed compatibility changes. | Named follow-up decision, release note and tests. |

## 12. Compatibility boundaries and known gaps

Assess compatibility independently for parsing, configuration shape, target
selection, credentials, defaults, operation effects, wire format and public
Rust APIs. Preserving one does not prove the others. Each change needs its own
consumer inventory, compatibility fixtures and documented adoption boundary.

Known source-level differences include schema-plan root profiles versus
rejected explicit root flags; policy/queries ignoring profile graph; offline
lint accepting unused flags; command-specific read `--as` handling; and legacy
graph omission probing. Keep them explicit until their owning contracts and
implementation change; a common framework does not erase differences.

Compatibility spellings include clap aliases `read`, `change`, `schema get`;
a separate hidden `ingest` command; and argv-level lint compatibility
rewrites. Parse preservation is not proof of identical defaults/routing.

Public Rust compatibility includes struct-literal construction and
destructuring, not just function calls. Boot compatibility includes accepted
root forms, validation failures and trust requirements. Accepted graph-schema
formats and provenance settings have their own upgrade/downgrade constraints;
client configuration changes cannot relax those constraints.

Broader managed command support, streaming bounds, cluster-name control
shortcuts, optional managed endpoints, `defaults.cluster`, self-hosted
HTTP cluster entries, empty-cluster serving, automatic failover and console/
public routing are not completed by this framework.

## 13. Verification and maintenance

Use independent contract cases across command, target, explicit/ambient
selection, context state, graph omission, credentials, policy, readiness and
output. Exhaust the crosses that can redirect a write or disclose a credential;
use pairwise coverage only for lower-risk presentation combinations.

Framework conformance assertions (acceptance criteria, not test results):

1. Explicit staging profile inside valid or malformed production context
   targets only staging; ambient production settings cannot override it.
2. Unsupported managed target refuses without touching any default store.
3. New managed selection + every API/cluster/endpoint mismatch sends no token.
4. Legacy literal server keeps its own chain even if a managed entry shares
   the URL; no automatic managed credential selection.
5. Credential presence, zero/one/many grants and unrelated context do not
   choose a managed graph or endpoint.
6. Explicit data selection does not read unrelated context; control commands
   read only their selected directory's binding.
7. Existing config fixtures remain valid; new mixed/missing authority fields
   refuse, and old clients reject rootless managed entries.
8. Local tools, init's positional URI, storage tools, aliases and registry
   operations receive their declared inputs without a blanket context gate.
9. Partial apply, observed digest refresh, initial import and graph recovery
   retain their existing owner tests and effects.
10. Status/boot tests verify each view's provenance and authorization, permit
    lag, and do not expand public readiness disclosure.
11. Exact managed-run replay and unknown data-write outcomes remain distinct;
    token issuance gains no unsupported idempotency header.
12. Runtime qualification is per command/transport: a server route, successful
    compile or generated table alone does not qualify a managed CLI command.
13. Accepted served query listing covers read and mutation entries, an empty
    catalog, unknown/unauthorized graphs, profiles/defaults, address conflicts
    and stable JSON. Results come from the active server catalog; existing
    direct listing and direct validation retain their separate behavior.
14. Proposed query/mutation deadline fixtures verify success after 10 seconds
    and before 30, the 10-second connection bound, and expiry of the complete
    request at 30 seconds even while receiving body chunks. A submitted
    mutation remains unknown after timeout; no automatic retry or authority
    fallback occurs.

Before accepting a new command: name its authority and target classes; define
omissions, credentials, effects, failure/retry and compatibility; register it;
supply independent positive/negative cases; update generated help/reference;
and record source/test/live evidence at its actual scope.

Contract changes follow the [RFC process](../rfcs/README.md) before
implementation. Changes that also affect the managed control plane require
its decision/spec synchronization. Update this reference as verified
support changes, retaining the distinction between rules and evidence.

## 14. Source index

The CLI, cluster and server implementation was checked against `500cdc29`
on 2026-09-06. Source inspection does not assert the binary running in a
deployment. Keep this reference aligned with the owning source and tests;
version-specific compatibility evidence belongs in the RFC. Managed
control-plane references retain their own draft status.

Implementation and contracts:

- [CLI declarations and defaults](../../crates/omnigraph-cli/src/cli.rs)
- [Operator schema](../../crates/omnigraph-cli/src/operator.rs)
- [Scope resolution](../../crates/omnigraph-cli/src/scope.rs)
- [Capability rules](../../crates/omnigraph-cli/src/planes.rs)
- [Credential and maintenance helpers](../../crates/omnigraph-cli/src/helpers.rs)
- [Managed data cache/validation](../../crates/omnigraph-cli/src/managed/data.rs)
- [Client transports](../../crates/omnigraph-cli/src/client.rs)
- [Core apply/refresh/import/status](../../crates/omnigraph-cluster/src/lib.rs)
- [Observed digest and import construction](../../crates/omnigraph-cluster/src/config.rs)
- [Serving snapshot](../../crates/omnigraph-cluster/src/serve.rs)
- [Server startup and boot witness](../../crates/omnigraph-server/src/lib.rs)
- [Engine invariants](invariants.md)
- [RFC 0011: addressing/config](../rfcs/0011-cli-addressing-and-config.md)
- [RFC 0052: managed CLI](../rfcs/0052-managed-control-plane-cli.md)
- [RFC 0053: offline data access](../rfcs/0053-offline-data-token-verification.md)

Existing executable assertions, inspected but not rerun for this document:

- [Core tests](../../crates/omnigraph-cluster/src/tests.rs):
  `import_missing_state_creates_state_with_graph_observation` and
  `refresh_records_live_schema_digest_and_graph_manifest_version`.
- [CLI capability/help tests](../../crates/omnigraph-cli/tests/cli_schema_config.rs):
  partial command-surface coverage, not a full coherence registry.
- [Server OpenAPI drift tests](../../crates/omnigraph-server/tests/openapi.rs):
  a precedent for artifact drift enforcement, not CLI routing proof.

CP normative homes: [RFC](https://github.com/ModernRelay/og-control-plane/blob/main/specs/00-RFC-Main.md),
[config supply](https://github.com/ModernRelay/og-control-plane/blob/main/specs/05-CFG-Config.md),
[identity](https://github.com/ModernRelay/og-control-plane/blob/main/specs/06-IDN-Identity.md#idn-04-cluster-identity-and-incarnation-i17),
[runs/projections](https://github.com/ModernRelay/og-control-plane/blob/main/specs/07-RUN-Runs.md#run-06-three-projections-t4),
[interfaces](https://github.com/ModernRelay/og-control-plane/blob/main/specs/11-UIS-Interfaces.md), and
[decisions](https://github.com/ModernRelay/og-control-plane/blob/main/specs/02-DEC-Decisions.md).
