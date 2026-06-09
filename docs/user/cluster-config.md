# Cluster Config

**Status:** Stage 2A read-only preview.

Cluster config is the future control-plane configuration surface for a whole
OmniGraph deployment. In this stage, OmniGraph can validate a local
`cluster.yaml` folder, produce a deterministic read-only plan, and inspect the
local JSON state ledger. It does not apply changes, open graph roots, scan live
cluster state, start servers, or write graph resources.

## Commands

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan     --config ./company-brain --json
omnigraph cluster status   --config ./company-brain --json
```

`--config` points at a directory, not a file. The directory must contain
`cluster.yaml`. When omitted, it defaults to the current directory.

## Supported `cluster.yaml`

Stage 2A accepts only the read-only resource subset:

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
defaults to `true`. When enabled, `cluster plan` briefly acquires
`<config-dir>/__cluster/lock.json` while it reads state, then removes it before
returning. `cluster status` never acquires the lock; it only reports whether one
is present.

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
CAS (`sha256:<digest>`), state revision, and lock id used for the read. The
command never writes `state.json`; apply, refresh, import, and live drift scans
are later-stage work.

## Status

`cluster status` reads the same local JSON state ledger and prints what the
ledger says is deployed. It does not validate referenced schema/query/policy
files and does not inspect live graphs. Missing `state.json` succeeds with a
warning; invalid state JSON or an unsupported state version fails.
