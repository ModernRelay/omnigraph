# Query, mutation, and load execution

**Audience:** compiler and engine contributors
**Authority:** current execution pipeline; public language syntax belongs in
[the user query guide](../user/queries/index.md)

## Read pipeline

A query runs against one resolved `ReadTarget`:

1. Resolve the branch or graph snapshot once.
2. Parse and type-check `.gq` source against that snapshot's accepted catalog.
3. Lower the checked query to typed IR.
4. Select any search mode and the edge types required by traversal or an
   anti-join.
5. Execute the IR against the same snapshot.
6. Serialize result batches at the calling boundary.

The executor never refreshes a mutable branch head midway through a query.
Historical reads build from the requested immutable table versions; current
branch reads may reuse version-keyed derived state.

Stable code owners:

| Concern | Owner |
|---|---|
| Parser, type checker, lowering | `crates/omnigraph-compiler/src/query/`, `src/ir/` |
| Query orchestration and IR execution | `crates/omnigraph/src/exec/query.rs` |
| Lance scan boundary | `crates/omnigraph/src/table_store.rs` |
| Topology build/cache | `crates/omnigraph/src/graph_index/`, `runtime_cache.rs` |
| Mutation orchestration | `crates/omnigraph/src/exec/mutation.rs` |
| Pending read-your-writes state | `crates/omnigraph/src/exec/staging.rs` |
| Loader | `crates/omnigraph/src/loader/mod.rs` |

Avoid line-number links in documentation; these modules are the stable owners.

## Traversal and joins

Unbound `Expand` and `AntiJoin` use a CSR/CSC `GraphIndex` scoped to the
edge types actually referenced by the query. The cache key includes each
covered edge table's physical identity and version, so unrelated edge types do
not force a graph-wide scan and a lazy branch may reuse an identical inherited
table view.

An explicitly bound edge must scan its edge table because topology alone does
not contain edge properties. It preserves incoming row/rank order and carries a
deterministic edge tie-break. `not { ... }` is an anti-join over its typed
inner pipeline.

Do not replace these shapes with eager cross-products. Keep intermediate rows
factorized and flatten only where the result contract needs it.

### Expand path selection

For an unbound `Expand`, the engine chooses between per-hop BTREE scans and the
in-memory CSR with a deterministic, I/O-free cost model. The inputs are the
current frontier size, effective hop count, manifest-resident edge and source
node counts, index coverage, and whether the CSR is already built. A degraded
BTREE is priced as a full edge scan per hop. `choose_expand_mode` and
`CSR_BUILD_FACTOR` in `crates/omnigraph/src/exec/query.rs` own the exact formula.

| Setting | Default | Effect |
|---|---:|---|
| `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` | `1024` | A larger input frontier always selects CSR before the cost comparison. |
| `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS` | `6` | A larger effective maximum hop count always selects CSR before the cost comparison. |
| `OMNIGRAPH_TRAVERSAL_MODE` | unset | `indexed` forces per-hop BTREE scans; `csr` forces the in-memory path. |

The two numeric settings are dispatch caps, not result limits or mid-traversal
cutoffs. A missing or nonnumeric value uses its default; a zero hop cap also
uses the default. Both execution paths have identical query semantics, so the
mode override is an operational escape hatch and test seam only.

## Filters and pushdown

The executor hoists a filter only when its bindings and operation make the move
semantically safe. Pushable scalar expressions use structured DataFusion/Lance
expressions with case-preserved column identities. Search prefilters remain on
the same scanner as the search operation. Multi-binding or unsupported
expressions stay in the engine at their lowered position.

String-built SQL is retained only at explicitly documented compatibility
seams. The camel-case regression and its two-parser boundary are recorded in
[the case study](case-studies/camel-case-filtering.md).

## Search and rank

`nearest`, text search/BM25, and reciprocal-rank fusion are first-class
execution concepts. Rank and score remain columns through downstream
operations; traversal or projection must not silently discard them. RRF
executes its sources independently against the same graph snapshot and fuses
their ordered results.

Lance 10 still loses final KNN ordering metadata in one late payload-hydration
shape, so OmniGraph requests one output partition for the affected nearest
path. The compatibility guard is owned by `lance_surface_guards.rs` and
`search.rs`; do not remove the fence based only on upstream release notes.

## Mutations

A named mutation is parsed, checked, and lowered against one captured
`WriteTxn`. Statement execution accumulates logical changes in
`MutationStaging`:

- insert and update append pending row batches;
- update reads the committed snapshot plus prior pending batches;
- delete records predicates and stages one deletion transaction at finalize;
- the D2 rule rejects a query that mixes constructive
  (insert/update) and destructive (delete) statements.

No statement advances a production table HEAD. After every statement and
cross-table validation succeeds, finalize stages one bounded transaction per
touched table and enters the shared write protocol. See
[writes.md](writes.md).

## Loads and graph batches

All load modes share the mutation publisher and recovery protocol:

| Mode | Current physical intent |
|---|---|
| `Overwrite` | One staged replacement per touched table. |
| `Append` | Strict insert by exact physical `id`; an existing ID is a typed conflict. The public mode name does not mean a bare Lance Append transaction. |
| `Merge` | Upsert by exact physical `id`; the last input occurrence wins. |

Mutation and keyed Load reject a table's accumulated input above 8,192 rows or
32 MiB before recovery is armed. Larger imports must be split into separately
atomic graph commits; Overwrite remains the initial bulk-replacement path.

`load_graph_batch_as` is the strict graph-level NDJSON boundary. Each nonblank
line is one logical node or edge envelope; duplicate members, physical fields,
unknown properties, invalid values, and noncanonical supplied node IDs are
rejected before effects. The older loader-compatible `load_as` parser and
deprecated `ingest*` SDK shims share the transaction machinery but are not a
second durability path. See [ingestion.md](ingestion.md).

## Validation

Mutation, Load, and branch merge use the catalog-derived validation owner in
`crates/omnigraph/src/validate.rs`. It covers value constraints, uniqueness,
edge referential integrity, and cardinality against the operation's pinned base
plus its complete delta. A physical index may accelerate a probe but may never
be a validation prerequisite.

Validation completes before recovery arm and table effects. For selected
external Blob inputs, policy and retained-metadata accounting complete before
target I/O; source probing, any required materialization, and payload-size
admission complete before durable graph movement. See [blob.md](blob.md).

## Embeddings

The provider-independent engine client handles query-string embedding and the
offline `omnigraph embed` workflow. Ordinary Load does not execute `@embed`
at ingestion time; callers supply vectors or precompute them. The annotation
records and validates embedding identity. Any future ingest-time reconciler is
a separate design, not a hidden loader behavior.

## Derived indexes

Schema apply, mutation, and Load publish logical data and index intent only.
`ensure_indices` materializes declared missing indexes through the shared
recovery protocol; `optimize` folds coverage as physical maintenance. Reads
remain correct through Lance's indexed-plus-unindexed scan behavior while
coverage converges.
