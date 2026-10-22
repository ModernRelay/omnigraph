# Aliases and agent automation

An operator alias binds a personal name to a stored read query on a registered
served target. The registry remains the team's deployed contract; see
[stored queries](stored-queries.md). The primary
[operator reference](../../../docs/user/cli/targets.md#read-aliases) owns the grammar.

Add this mapping to a complete `version: 1` operator document:

```yaml
aliases:
  triage:
    target: production
    graph: spike
    query: weekly_triage
    params: {limit: 20}
```

```bash
omnigraph alias triage --params '{"since":"2026-06-01"}' --format jsonl
omnigraph alias triage --params '{"limit":5}' --json
```

Aliases require an explicit target and, for multi-graph targets, an explicit
graph. They never inherit `default_graph` and reject target/graph overrides.
Managed aliases retain their target's pinned profile. Standalone aliases use
their target's single auth provider; neither accepts an authentication override.
`query` is a stored
identifier, never a filename or inline program. Values in `params` are typed
JSON-compatible defaults; explicit parameters shallowly replace matching names.
There is no positional `args` binding or per-alias `format` field.

Aliases are read-only: a stored mutation refuses. Invoke mutations directly with
`omnigraph mutate NAME --target production --graph spike --params '{...}'`.
A direct-graph alias must omit `graph` to pass validation, but cannot execute:
the stored-query registry is served-only. Validation does not add capabilities.

Use `--format jsonl` or `--json` for automation, or set `defaults.output` to
`jsonl`/`json`. The supported defaults are `table`, `json`, `jsonl`, `csv`, and
`kv`. Action receipts retain their own output contracts. Use JSON when the
consumer needs the complete column list or read `graph_commit_id` for a
conditional mutation; JSONL metadata does not carry those fields. See
[result values](queries.md#system-fields-and-result-values).

Never put credentials or helper definitions in aliases. Authentication comes
from the target's owner-local provider, with no source fallback. Validation and
inspection never run helpers; execution can acquire the selected credential.
See [credentials](../../../docs/user/cli/targets.md#manage-credentials).
