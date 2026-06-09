# Cluster Config

**Status:** Stage 2C state-lock recovery preview.

Cluster config is the future control-plane configuration surface for a whole
OmniGraph deployment. In this stage, OmniGraph can validate a local
`cluster.yaml` folder, produce a deterministic read-only plan, inspect the
local JSON state ledger, and explicitly refresh/import graph observations into
that ledger. It can also manually remove a held local state lock by exact lock
id. It does not apply desired changes, start servers, or write graph resources.

## Commands

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan     --config ./company-brain --json
omnigraph cluster status   --config ./company-brain --json
omnigraph cluster refresh  --config ./company-brain --json
omnigraph cluster import   --config ./company-brain --json
omnigraph cluster force-unlock <LOCK_ID> --config ./company-brain --json
```

`--config` points at a directory, not a file. The directory must contain
`cluster.yaml`. When omitted, it defaults to the current directory.

## Supported `cluster.yaml`

Stage 2C accepts only the read-only resource subset:

```yaml
version: 1
metadata:
  name: company-brain

state:
  backend: cluster
  lock: true

graphs:
  knowledge:
    schema: ./knowledge.pg
    queries:
      find_experts:
        file: ./knowledge.gq

policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge]
```

`metadata.name` is a display label. `state.backend` may be omitted or set to
`cluster`; external state backends are reserved for a later stage. `state.lock`
defaults to `true`. When enabled, `cluster plan`, `cluster refresh`, and
`cluster import` briefly acquire `<config-dir>/__cluster/lock.json`, then remove
it before returning. `cluster status` never acquires the lock; it only reports
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

Fields reserved for later phases, such as `pipelines`, `embeddings`, `ui`,
`aliases`, and `bindings`, fail with a typed diagnostic instead of being
silently ignored.

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
      "graph.knowledge": { "digest": "..." },
      "schema.knowledge": { "digest": "..." },
      "query.knowledge.find_experts": { "digest": "..." },
      "policy.base": { "digest": "..." }
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
from live observations. Apply and live drift scans during plan are later-stage
work.

## Status

`cluster status` reads the same local JSON state ledger and prints what the
ledger says is deployed. It does not validate referenced schema/query/policy
files and does not inspect live graphs. Missing `state.json` succeeds with a
warning; invalid state JSON or an unsupported state version fails. If a lock is
present, status reports its id, operation, creation time, pid, and age.

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

Refresh/import do not observe query or policy resources yet. Existing query and
policy state digests are preserved on refresh and are not invented on import.

## Force Unlock

`cluster force-unlock <LOCK_ID>` removes `<config-dir>/__cluster/lock.json` only
when the file exists, is valid version-1 lock JSON, and its `lock_id` exactly
matches the argument. A wrong id, missing lock, invalid lock JSON, or unsupported
lock version exits non-zero and leaves the file untouched.

This is manual recovery for abandoned local locks. OmniGraph does not perform
PID-liveness checks, TTL expiry, stale-lock breaking, or automatic unlock in
Stage 2C.
