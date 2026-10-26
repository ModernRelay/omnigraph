- `optimize` now checks the installed graph policy's explicit `optimize` action
on target branch `main`, including direct engine and CLI access. Existing
policies must grant this action before those actors can optimize. Graphs
without an installed policy retain direct storage behavior.

- Cluster servers can opt into bounded, authenticated optimization with durable
original-ID receipts. It runs under the existing native owner, serializes with
configuration deployment and preserves unresolved work without replay.
Source inventory ceilings do not replace process CPU, memory or I/O limits.
The complete result envelope is reserved before effects. Original-ID recovery
records interrupted publication under stopped-writer exclusion without replay.
Root-bound signed-token/OIDC servers refuse new optimization until resource
admission is supported; direct HTTP cannot bypass that gate.

- Vector index reconciliation now propagates statistics, storage and builder
failures instead of reporting them as pending training data. An existing index
is deferred only when it has no non-null vectors; Lance chooses the supported
training layout for positive input.
Served source preflight also refuses unsupported index formats before rewriting.
