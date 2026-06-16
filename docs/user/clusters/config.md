# Cluster Config

> New to the cluster tooling? Start with the operator how-to guide,
> [cluster.md](index.md) — this document is the reference.

Cluster config is the future control-plane configuration surface for a whole
OmniGraph deployment. In this stage, OmniGraph can validate a local
`cluster.yaml` folder, produce a deterministic read-only plan, inspect the
local JSON state ledger, explicitly refresh/import graph observations into
that ledger, manually remove a held local state lock by exact lock id, and
**apply the executable subset of the plan** — stored-query and policy-bundle
catalog writes, **graph creation** (a declared graph that does not exist yet
is initialized by apply at the derived root), **schema updates** (soft drops
only), and — behind an explicit, digest-bound **approval** — **graph
deletion**. It does not perform data-loss schema migrations, start servers,
or run data loads. A server can boot from the applied ledger with
`omnigraph-server --cluster <config-dir | storage-root>`.

## Commands

```bash
omnigraph cluster validate --config company-brain
omnigraph cluster plan     --config company-brain --json
omnigraph cluster apply    --config company-brain --json
omnigraph cluster approve  graph.<id> --config company-brain --as <actor>
omnigraph cluster status   --config company-brain --json
omnigraph cluster refresh  --config company-brain --json
omnigraph cluster import   --config company-brain --json
omnigraph cluster force-unlock <LOCK_ID> --config company-brain --json
```

`--config` points at a directory, not a file. The directory must contain
`cluster.yaml`. When omitted, it defaults to the current directory.

## Relationship to `~/.omnigraph/config.yaml`

`cluster.yaml` and the per-operator `~/.omnigraph/config.yaml` never describe
the same fact. The operator config is the permanent **per-operator** layer
(the operator's identity and credential references, named servers/clusters,
profiles, and CLI defaults); `cluster.yaml` is the shared desired state of a
whole deployment, read only by the `cluster` commands via `--config`.

The exact contract:

- **Cluster commands read the operator config for exactly one thing**: the
  `operator.actor` default used by `apply`/`approve` when `--as` is omitted —
  operator identity is a per-operator fact. With `--as` present, the operator
  config is not needed. Nothing else in it influences a cluster command.
- **No legacy `omnigraph.yaml`**: the CLI does not read `omnigraph.yaml` at
  all, and a `--cluster` server reads only the cluster catalog — boot is
  cluster-only.
- **The other direction is ergonomics, not coupling**: per-operator
  data-plane commands address a cluster graph by its derived storage root
  (`company-brain/graphs/knowledge.omni`) with `--store <uri>` — an ordinary
  local path, no special handling.

## Supported `cluster.yaml`

The current config surface accepts this resource subset:

```yaml
version: 1
metadata:
  name: company-brain

state:
  backend: cluster
  lock: true

providers:
  embedding:
    default:
      kind: openai-compatible
      base_url: https://openrouter.ai/api/v1
      model: openai/text-embedding-3-large
      api_key: ${OPENROUTER_API_KEY}

graphs:
  knowledge:
    schema: knowledge.pg
    embedding_provider: default
    queries: queries/          # discover every `query <name>` in queries/*.gq

policies:
  base:
    file: base.policy.yaml
    applies_to: [knowledge]
```

`queries` is Terraform-shaped — the `.gq` files are the declaration. Three
forms:

```yaml
queries: queries/                  # directory: top-level *.gq, sorted; every declaration registers
queries: [people.gq, extra/a.gq]   # explicit files; every declaration in each
queries:                           # fine-grained name -> file map
  find_experts:
    file: knowledge.gq
```

Discovery is loud: an unreadable or unparseable `.gq`, or the same query name
declared in two files, fails validation (`query_parse_error`,
`duplicate_query_name`). Each discovered query is still an individually
addressed resource (`query.<graph>.<name>`) with its own plan/apply lifecycle;
the digest is the containing file's hash, so editing a multi-query file
updates all of its queries together. Paths are relative to the config
directory — the cluster is one explicit folder, so no `./` prefixes are
needed.

`providers.embedding.<name>` defines a query-time embedding provider profile
for cluster-served graphs. A graph opts in with `embedding_provider: <name>`;
bare names normalize to `provider.embedding.<name>`. Supported provider
`kind` values are `openai-compatible` (default/OpenRouter-compatible),
`openai` (OpenAI's own host), `gemini`, and `mock`. Real providers require
`api_key: ${ENV_VAR}`; inline secrets are rejected. The env var is resolved
only when a `--cluster` server boots, so `cluster validate`, `plan`, and
`apply` do not need deployment secrets. `mock` is deterministic and does not
require `api_key`. Vector dimensions stay schema-driven by the target
`Vector(N)` column, not the provider profile.

`storage:` (optional) is the **storage root URI** for everything the cluster
stores — the state ledger, lock, content-addressed catalog, recovery
sidecars, approval artifacts, and the derived graph roots
(`<storage>/graphs/<id>.omni`). Absent, it defaults to the config directory
itself (the original layout, byte-compatible with pre-existing clusters).
`s3://bucket/prefix` puts the whole cluster on S3-compatible object storage:
the ledger CAS uses conditional writes (verified against AWS S3 semantics and
RustFS), the lock becomes genuinely cross-machine, and graph roots are
engine-native S3 URIs. Credentials are **never** in `cluster.yaml` — the
standard `AWS_*` environment contract applies, identical to graph storage.
Declared configuration (`cluster.yaml` and the schema/query/policy sources it
references) always stays in the working tree: config is versioned in git,
state lives in the store — the Terraform split.

`metadata.name` is a display label. `state.backend` may be omitted or set to
`cluster`; external state backends are reserved for a later stage. `state.lock`
defaults to `true`. When enabled, `cluster plan`, `cluster apply`,
`cluster refresh`, and `cluster import` briefly acquire
`<config-dir>/__cluster/lock.json`, then remove it before returning. `cluster status` never acquires the lock; it only reports
whether one is present. `cluster force-unlock` is the only lock-removal command;
it requires the exact lock id and should be run only after confirming no cluster
operation is active.

## Validation

`cluster validate` checks:

- `cluster.yaml` syntax and supported fields
- duplicate YAML keys
- schema, query, and policy file existence
- schema parsing and catalog construction
- stored-query parsing and query-name matching
- stored-query type-checking against the desired schema
- policy `applies_to` graph references
- embedding provider profiles and graph `embedding_provider` references

Fields reserved for later phases, such as `pipelines`, top-level
`embeddings`, `ui`, `aliases`, and `bindings`, fail with a typed diagnostic
instead of being silently ignored. Under `providers`, only `embedding` is
supported today; other provider namespaces fail as unsupported config.

## Planning

`cluster plan` first performs validation, then reads local JSON state from:

```text
<config-dir>/__cluster/state.json
```

If the file is missing, the state is treated as empty and every desired
resource is planned as a create. If present, the file must use this shape:

```json
{
  "version": 1,
  "state_revision": 0,
  "applied_revision": {
    "config_digest": "...",
    "resources": {
      "schema.knowledge": { "digest": "..." },
      "query.knowledge.find_experts": { "digest": "..." },
      "provider.embedding.default": {
        "digest": "...",
        "embedding_profile": {
          "kind": "openai-compatible",
          "base_url": "https://openrouter.ai/api/v1",
          "model": "openai/text-embedding-3-large",
          "api_key": "${OPENROUTER_API_KEY}"
        }
      },
      "graph.knowledge": {
        "digest": "...",
        "embedding_provider": "provider.embedding.default"
      },
      "policy.base": {
        "digest": "...",
        "applies_to": ["cluster", "graph.knowledge"]
      }
    }
  },
  "resource_statuses": {
    "graph.knowledge": {
      "status": "applied",
      "conditions": [],
      "message": "optional status detail"
    }
  },
  "approval_records": {},
  "recovery_records": {},
  "observations": {}
}
```

`state_revision`, `resource_statuses`, `approval_records`, `recovery_records`,
and `observations` are optional so older Stage 1 state fixtures keep working.
Missing `state_revision` is treated as `0`. Resource status values are
`pending`, `planned`, `applying`, `applied`, `drifted`, `blocked`, or `error`.

Plan output compares desired resource digests against state resource digests
and reports `create`, `update`, and `delete` changes. It also reports the state
CAS (`sha256:<digest>`) and state revision. `state_observations.locked` means an
existing lock file was observed, along with its metadata (`lock_id`,
`lock_operation`, `lock_created_at`, `lock_pid`, `lock_age_seconds`); a
successful `plan` instead reports `lock_acquired: true` and an
`acquired_lock_id`, then releases the lock before returning. The command never
writes `state.json` and does not scan live graphs. Use explicit
`cluster refresh` / `cluster import` when the state ledger should be updated
from live observations. Live drift scans during plan are later-stage work.

Policy entries additionally record their applied `applies_to` bindings as
normalized typed refs — the state ledger is serving-sufficient for the
future server-boot stage. A change to `applies_to` alone (the policy file
digest unchanged) appears in the plan as an Update marked `binding_change`
(human output: `[bindings]`), applies like any catalog change, and counts
toward convergence; ledgers written before this field existed are backfilled
by the next apply.

Each plan change carries a `disposition` field — an honest preview of what
`cluster apply` will do with it in this stage: `applied` (executes), `derived`
(a `graph.<id>` composite-digest update that converges automatically once its
query digests land), `deferred` (graph/schema change, later phase), or
`blocked` (query/policy gated by an unapplied or missing dependency, with the
condition in `reason`).

## Apply

`cluster apply` executes the executable subset of the plan — stored-query and
policy-bundle changes, graph creates, and schema updates. There is no confirm
flag: `cluster plan` is the preview,
and apply recomputes the same diff under the state lock before executing, so a
stale preview can never be applied. Apply requires an existing `state.json`
(`state_missing` directs you to `cluster import` first).

For each applied create/update, the resource payload is written
content-addressed into the local catalog:

```text
<config-dir>/__cluster/resources/query/<graph>/<name>/<digest>.gq
<config-dir>/__cluster/resources/policy/<name>/<digest>.yaml
```

Extensions are fixed per kind regardless of the source file's name. Payloads
are written before the state update because `state.json` is the publish point:
if the final CAS-checked state write fails, no success is reported and the
digest-named blobs already written are inert — re-running apply is the repair.
Deletes remove the resource from state; their old payload blobs stay on disk
(garbage collection is a later stage). Re-running a converged apply is a no-op:
no state write, no revision change (`state_written: false`).

**Applied means serving.** A server started with `--cluster <dir>` boots from
the applied revision (see
[Serving from the cluster](#serving-from-the-cluster-the-mode-switch)); it
picks up newly applied state on its next restart. Until that restart, applied
means recorded in the catalog, nothing more.

### Graph creation

A `graph.<id>` create (the graph is declared but no root exists) is executed
by apply: the graph is initialized at the derived root

```text
<config-dir>/graphs/<graph-id>.omni
```

with the declared schema, before any catalog writes, so queries and policies
that depend on the new graph apply **in the same run**. Each create is fenced
by a recovery sidecar under `__cluster/recoveries/{ulid}.json`, written before
the init and removed only after the state update lands. If apply crashes in
between, the next state-mutating command (`apply`, `refresh`, `import`) runs a
**recovery sweep** that classifies the survivor by observation: an absent root
removes the stale intent; a completed create rolls the cluster state forward
(recorded in the state's `recovery_records`); a partial root reports
`graph_create_incomplete` (status `error` — remove the root and re-run apply;
nothing is auto-deleted); unexpected graph content reports
`actual_applied_state_pending` (status `drifted` — run `cluster refresh` and
re-plan). While a kept sidecar is pending, that graph's create and its
dependents are blocked with `cluster_recovery_pending`. Read-only commands
(`status`, `plan`) warn about pending sidecars without acting on them.

**Re-creation is convergence.** If a graph root disappears out-of-band,
`refresh` records the drift and the next `plan` proposes a create — and apply
will execute it, producing an **empty** graph at the root. The data was
already lost when the root vanished; the create is visible in the plan
(disposition `applied`) before anything runs.

### Schema updates

A `schema.<id>` update (the declared schema differs from what state records)
is executed by apply via the engine's schema-apply, after graph creates and
before catalog writes — so a query change that depends on the new schema
applies in the same run. Each schema apply is sidecar-fenced like a create:
pre-operation manifest version recorded, post-operation version written back,
sidecar retired only after the state update lands; the recovery sweep
classifies survivors by schema digest (consistent ledger → retired; completed
on the graph → state rolled forward with an audit entry; anything else →
`drifted`/`actual_applied_state_pending`, kept).

Migrations run with **soft drops only** — a removed property disappears from
the current version while prior versions retain the data (reversible until
`cleanup`). Data-loss migrations (`allow_data_loss`) are not reachable from
cluster apply until the approval-artifact stage. Unsupported migrations
(e.g. changing a property's type), engine lock contention, or graphs with
user branches fail loudly as `schema_apply_failed` with the engine's message;
dependent changes are demoted to `blocked` and graph-moving work stops for
the run.

`cluster plan` previews schema updates with the engine's real migration plan:
each schema change carries a `migration` field (`supported` + typed steps),
and the human output prints the steps. If the live graph cannot be opened the
preview degrades to the digest diff with a `schema_preview_unavailable`
warning.

**Drift is converged, not just reported.** A schema changed out-of-band on
the live graph shows up as `drifted` after `refresh`, and the next plan
proposes migrating it back to the declared schema — apply executes that like
any other soft migration. Drift correction is gated by the same rules as any
change; nothing about it is hidden (the plan shows the steps, including soft
drops of out-of-band fields).

**Attribution.** `cluster apply --as <actor>` records the operator identity
in recovery sidecars and audit entries and threads it to the engine's
schema-apply (so commit attribution and Cedar enforcement — wherever a policy
checker is installed — work unchanged).

### Approvals and graph deletion

Deleting a graph is the irreversible tier: it requires a recorded human
decision. `cluster plan` lists the gate under `approvals_required` (one gate
per graph — the graph-level approval carries its schema and queries);
`cluster approve graph.<id> --as <actor>` writes a digest-bound artifact to

```text
<config-dir>/__cluster/approvals/<approval-id>.json
```

bound to the exact desired config digest and the change's state digest, so
**any config or state drift after approving invalidates the artifact**
automatically (`approval_stale` warning; it never authorizes a different
change). An unapproved delete blocks with `approval_required`.

An approved delete executes **last** in the apply run: the graph root is
removed recursively, the subtree (graph, schema, its queries) is tombstoned
out of the state ledger with a tombstone observation, and the approval is
consumed — recorded in the state's `approval_records` in the same state
update, and the artifact file rewritten with `consumed_at` (the file is never
deleted: the audit fact survives the loss of either store). A failed run
consumes nothing; the approval stays valid for the retry. Catalog blobs of
the deleted graph's queries stay on disk (GC is a later stage).

Crash recovery for deletes: a completed-but-unrecorded delete is rolled
forward by the sweep (tombstone + approval consumption + audit entry); an
incomplete delete (root still present) is retired with a
`graph_delete_incomplete` warning and simply **re-proposed** — prefix removal
is idempotent, so the still-approved retry is the repair.

Standalone schema deletes are never executed by this stage. They are
reported as `deferred` (warning `apply_unsupported_change`), and query/policy
changes that depend on them are `blocked` (warning `apply_dependency_blocked`, status
`blocked` in state). A partially-applicable plan still exits 0 with warnings;
the JSON `converged` field is the automation signal for "state now matches the
desired revision". The applied `config_digest` is only recorded when apply
fully converges. The `graph.<id>` composite digest is recomputed from state's
own schema/query digests after each apply, so applied query changes converge
without graph movement.

## Serving from the cluster (the mode switch)

```bash
omnigraph-server --cluster company-brain --bind 0.0.0.0:8080
```

`--cluster <dir>` is an **exclusive boot source** (axiom 15): it cannot
combine with a graph URI or `--config`, and in this mode
`omnigraph.yaml` is never read — not for graphs, not for queries, not for
policies. The server serves the **applied revision**: graph roots recorded in
`state.json`, stored-query and policy content from the content-addressed
catalog at the applied digests (re-verified at boot), and policy bundles
wired by their applied `applies_to` bindings — `cluster`-bound bundles become
the server-level Cedar engine, graph-bound bundles attach per graph.
Un-applied config drift never leaks into serving; `cluster plan` is where
drift is visible. Routing is always multi-graph (`/graphs/{id}/...`). Bearer
tokens and the bind address stay process-level (flags/env) — they are
per-replica facts, not cluster facts.

Boot is fail-fast: missing or unreadable state, pending recovery sidecars,
missing/tampered catalog blobs, policy entries without binding metadata
(pre-binding ledgers — re-run `cluster apply`), an empty graph set, more than
one policy bundle binding a single scope (split or merge bundles; stacked
scopes are a later stage), unopenable graph roots, and stored queries that no
longer type-check all refuse startup with a remedy. A held state lock is
*not* an error — boot reads the atomically-replaced state file without
locking.

Serving is static per process: the server reads the applied revision once at
startup, so picking up newly applied state means restarting it. Stored
queries are all listed in `GET /queries` in cluster mode (the cluster
registry has no expose flag; exposure becomes a policy decision in a later
phase).

## Status

`cluster status` reads the same local JSON state ledger and prints what the
ledger says is deployed. It does not validate referenced schema/query/policy
files and does not inspect live graphs. Missing `state.json` succeeds with a
warning; invalid state JSON or an unsupported state version fails. If a lock is
present, status reports its id, operation, creation time, pid, and age.

Status also verifies the catalog payloads read-only: every query/policy digest
recorded in state is checked against its content-addressed blob under
`__cluster/resources/` (existence and full digest re-hash). A missing or
mismatched blob is reported as a warning (`catalog_payload_missing` /
`catalog_payload_mismatch`); an unreadable blob is an error
(`catalog_payload_read_error`) because an unverifiable catalog must not report
healthy. Status never writes state — persisting the `drifted` condition is
refresh's job. The check runs without the state lock, so it is a point-in-time
report.

## Refresh And Import

`cluster refresh` updates an existing `state.json` from actual observations.
`cluster import` creates the first `state.json` when the ledger is missing.
Both commands open declared graphs read-only at:

```text
<config-dir>/graphs/<graph-id>.omni
```

They observe only branch `main`, recording graph existence, manifest version,
live schema digest, desired schema digest, and schema-match status under
`observations["graph.<id>"]`. Missing graph roots are recorded as drift and
remove the graph/schema digests from state so a later `plan` proposes creates.
Invalid graph roots are recorded as errors; `refresh` persists the error
observation and exits non-zero, while `import` exits non-zero without creating
initial state.

Refresh also verifies the catalog payloads of every query/policy digest
recorded in state (the same check `cluster status` reports read-only), and
closes the loop:

- a **missing** or **digest-mismatched** blob marks the resource `drifted`
  (condition `payload_missing` / `payload_mismatch`) and removes its digest
  from state — so the next `cluster plan` proposes a create and the next
  `cluster apply` republishes the blob (the self-heal loop, mirroring how a
  missing graph root is handled);
- an **unreadable** blob (IO error other than not-found) keeps the digest,
  marks the resource `error` (condition `payload_read_error`), and exits
  non-zero — transient IO must not trigger a spurious republish.

Upgrade note: a state ledger written before catalog publish existed records
query/policy digests with no blobs on disk; the first refresh after upgrading
flags them all `payload_missing`, and a single `cluster apply` republishes
everything and converges.

Refresh/import do not observe query or policy resources beyond their catalog
payloads yet. Existing query and policy state digests are preserved on refresh
(unless their payload drifted, above) and are not invented on import.

## Force Unlock

`cluster force-unlock <LOCK_ID>` removes `<config-dir>/__cluster/lock.json` only
when the file exists, is valid version-1 lock JSON, and its `lock_id` exactly
matches the argument. A wrong id, missing lock, invalid lock JSON, or unsupported
lock version exits non-zero and leaves the file untouched.

This is manual recovery for abandoned local locks. OmniGraph does not perform
PID-liveness checks, TTL expiry, stale-lock breaking, or automatic unlock in
Stage 2C.
