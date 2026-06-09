# Cluster Config

**Status:** Stage 1 read-only preview.

Cluster config is the future control-plane configuration surface for a whole
OmniGraph deployment. In this stage, OmniGraph can validate a local
`cluster.yaml` folder and produce a deterministic read-only plan. It does not
apply changes, acquire locks, open graph roots, start servers, or write state.

## Commands

```bash
omnigraph cluster validate --config ./company-brain
omnigraph cluster plan     --config ./company-brain --json
```

`--config` points at a directory, not a file. The directory must contain
`cluster.yaml`. When omitted, it defaults to the current directory.

## Supported `cluster.yaml`

Stage 1 accepts only the read-only resource subset:

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

`metadata.name` is a display label. `state.lock` is parsed for forward
compatibility, but no lock is acquired in this read-only stage. `state.backend`
may be omitted or set to `cluster`; external state backends are reserved for a
later stage.

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
  "applied_revision": {
    "config_digest": "...",
    "resources": {
      "graph.knowledge": { "digest": "..." },
      "schema.knowledge": { "digest": "..." },
      "query.knowledge.find_experts": { "digest": "..." },
      "policy.base": { "digest": "..." }
    }
  }
}
```

Plan output compares desired resource digests against state resource digests
and reports `create`, `update`, and `delete` changes. The command never writes
`state.json`; apply and locking are later-stage work.
