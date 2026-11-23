# Architectural invariants

**Audience:** anyone proposing, implementing, or reviewing a non-trivial change
**Authority:** standing design rules; implementation mechanics belong in the
area guides

## Governing principle

Accepted schema and manifest state define the logical graph. Lance files,
indexes, fragment layout, staged effects, and caches are physical state.
Physical state may lag or be rebuilt, but it may never silently weaken the
logical contract. A genuine logical conflict still fails loudly.

The default review question is: **what remains the single source of truth after
five more changes like this one?**

## Hard invariants

1. **Respect the substrate.** Lance owns per-dataset storage, versions,
   branches, transactions, indexes, compaction, and cleanup; DataFusion owns
   relational execution where it fits. Read the full matching pages in
   [lance.md](lance.md) before adding parallel machinery or relying on an
   undocumented behavior.

2. **There is one graph-content publication door.** A graph change becomes
   authoritative through one `__manifest` publication containing every
   visible table pointer, accepted schema contract and graph-lineage update.
   A write to an existing registered table never moves its linear HEAD: its effects are detached commits the `__manifest`
   pin `(published_dataset_version, staged_version, transaction_uuid)`
   names, every pin is a detached version that opens, and the pin is the
   table's version for its whole life. A graph table's linear HEAD stays at
   its creation version, recorded per registration as
   `omnigraph.last_linear_version`; the collector reads it to list foreign
   versions. Added-type
   admission may also inspect an unregistered deterministic path, but accepts
   only its qualified original empty version 1, never an advanced HEAD.
   Per-table publication is never graph publication.

3. **Every operation uses one coherent accepted view.** A read holds one
   immutable snapshot for its lifetime. A writer captures schema, catalog,
   branch, graph head, branch identity (the identifier of the branch's row in
   `main`'s catalog at or above format 18, of its native ref below), and table
   baselines from one attempt, then revalidates that complete authority before
   effects. A retry
   starts a new attempt; it never combines fresh and stale facts.

4. **A mutation publishes once.** `mutate`, `load`, schema apply, merge, and
   equivalent multi-table operations stage all participants and publish once.
   They do not acknowledge or publish per statement or per table. A mutation
   holds inserts and updates, or deletes, never both (`write:1`).

5. **Crash convergence is part of the commit protocol.** An effect that is
   durable before publication must be unreachable: a detached version or a
   staged file nothing references, which a retry ignores. `cleanup`'s
   collector reclaims such a version only when the publication authority
   its transaction properties record is provably gone; age does not prove
   abandonment. The two explicitly requested exceptions are the publishable
   proposal (`as commit`, [decision](../decisions/2026-10-10-unpublished-graph-commits.md))
   and the nonpublishable snapshot (`as unattached branch`,
   [decision](../decisions/2026-10-11-unattached-graph-branches.md)): each writes a
   detached `__manifest` version reachable by its graph commit id through a
   reserved tag, `unpublished_<id>` for the proposal and
   `unattached_<root>_<C>` for the snapshot, which acknowledges durable
   snapshot addressability with `published: false` and never branch
   visibility; the branch still changes only at the proposal's later ordinary
   publication, the snapshot never publishes, and both tags expire with the
   captured target authority. Published table effects and the accepted schema contract
   are complete in one manifest publication; no contract-file installation
   remains. There is no side record to classify. The collector holds two properties
   on every run: safety, that every pin of every retained `__manifest`
   version on every live branch still opens and every path its manifest
   lists still exists after the run; and progress, that a table version no
   retained `__manifest` version names is gone after a run that retains
   nothing else. Foreign movement fails closed: a linear commit above a
   table's last linear version is reported by the collector (`cleanup`'s
   `foreign_versions`), never deleted and never adopted.
   See [recovery.md](recovery.md).

6. **Stable identity survives renames, not lifetimes.** Accepted SchemaIR owns
   non-zero type, property, and table-incarnation identities. A rename
   preserves them and their table history; drop/re-add creates new identities.
   Never infer identity from an alias, path, Lance version, field ID, or branch
   name.

7. **Physical acceleration is derived state.** Secondary indexes, topology
   indexes, caches, fragment layout, and compaction output may be incomplete or
   stale. Missing coverage may change cost but not correctness. Expensive index
   work happens through explicit reconciliation, not inline on content writes.

8. **Integrity failures are loud.** Type, required-field, uniqueness,
   endpoint, cardinality, schema-lifetime, recovery, and mutation-mode
   violations fail with typed outcomes. The engine does not invent
   placeholders, silently weaken constraints, return plausible partial state,
   or reinterpret an unknown persisted format.

9. **Query semantics are typed structures.** Traversal, search modes, ranking,
   mutations, polymorphism, policy predicates, and future planner capabilities
   belong in AST, IR, and typed plan structures. Do not smuggle semantics
   through strings, transport flags, global state, or side tables.
   Expressions and parameter declarations retain the compiler's type.
   Numeric comparison conversions are explicit Cast nodes; execution and scan
   pushdown never select a domain from values. The internal exact integer
   carrier cannot appear in a declared property, parameter or public result.
   Mutation statements retain the compiler-selected node or edge target;
   execution does not choose again when both namespaces contain the same name.
   Binding validates every declared parameter, including unused declarations;
   replay validates used values against the retained leaves. Projection fields
   come from those declarations, including empty results. Opened Lance fields
   must match the accepted storage schema in type and nullability. Derived
   operator fields may widen nullable flags, but values may not violate a
   declared non-null constraint.

10. **Trust is established at the boundary and enforced at the engine.** HTTP
    resolves bearer tokens to actors; a client never supplies its trusted actor
    identity. Token plaintext is not retained. Server reads apply their policy,
    and every mutating engine `_as` entry point applies the action/scope/actor
    gate so embedded and CLI-direct writers do not bypass it.

11. **Failures and resource use are bounded and observable.** Conflict,
    timeout, OOM, backpressure, partial-effect, external dependency, and
    recovery paths have explicit outcomes. Retries are contract-defined and
    bounded. Hot-path work must scale with the working set, not accumulated
    history.

12. **One source of truth, cheaply derived.** Lance, `__manifest`, and the
    accepted schema are authoritative. `__manifest` is a table's only
    lineage; the table's Lance linear history is not a second, derived
    history. Immutable version-pinned state may be
    cached; mutable-tip caches are hints, never commit authority. Do not
    maintain a shadow copy that can drift or rebuild a warm projection from
    full history on every call.

13. **Evidence matches the boundary.** Test the layer whose contract changed.
    Irreversible storage, protocol, or substrate decisions require a
    [decision record](documentation.md#decisions) plus compatibility, refusal,
    crash, and rebuild evidence. Cost claims require a
    checked-in instrument. Extend the existing owner
    ([testing.md](testing.md), [test ownership](test-ownership.md)) before
    adding a parallel fixture.

## Deny-list

Treat these shapes as rejected until a
[decision record](documentation.md#decisions) argues why the case is
different:

- a custom WAL, transaction manager, buffer pool, or storage primitive already
  owned by Lance;
- a job queue for state derivable from accepted manifest state, where an
  idempotent reconciler suffices;
- synchronous vector or FTS rebuilds on a content-write path;
- a logical precondition based on physical index coverage, fragment count, a
  cache entry, or staged layout;
- raw Lance graph-table writers or public writable `Dataset` handles outside
  the sealed storage boundary;
- ad-hoc SQL or `IN (...)` string generation where structured expressions or
  SIP apply;
- side channels for query semantics or discarded retrieval rank;
- eager cross-product materialization in multi-hop execution;
- cost-blind plan choice or planner decisions based on hidden statistics;
- a cloud-only correctness fix or a separate cloud fork of the engine;
- process-local locks presented as distributed writer fencing;
- silent retries, partial results, swallowed errors, or acknowledgement before
  durable graph visibility;
- maintained parallel truth or cold full-history reconstruction per request;
- an ingest tier that acknowledges a write before the graph shows it, a
  Lance MemWAL included: per-table durability still needs graph-level
  validation, a fold and one cross-table publication, and that coordination
  costs more than the commit it defers ([ingestion](ingestion.md));
- a node or edge type the engine writes into a user's graph on its own, such
  as an automatic actor node: it changes the schema format for every reader,
  and an export then holds rows that a load refuses;
- a file in a checkout that carries graphs, credentials or identity (the
  retired `omnigraph.yaml`): one file with two owners can be neither
  committed safely nor kept personal, and a checkout could redirect a token.

Cases a decision record has argued: the flat locator objects and legacy data files under
`__history/legacy/`, written once by the storage upgrade (from stamp 8, 9 or 13), in the
[legacy commit files](../decisions/0068-graph-commit-record.md).

## Support boundaries

What Omnigraph supports today is stated where a caller observes it, in
[the contract](contract.md#gaps): one writing process per cluster (`G2`,
`G13`), Azure writes as a preview (`G4`), schema apply on `main` with no
other live branch (`D6`), and maintenance only when asked (`maintain:1`).
The mechanism behind each boundary has one owner:
[control plane](control-plane.md#support-boundaries) for serving, apply and
graph removal, and [writer epoch](writes.md#writer-epoch) for what the fence
covers. Change a boundary only with the owning code, tests, guide and, when
it is costly to reverse, a [decision record](documentation.md#decisions).

## Review checklist

- Which statements of [the contract](contract.md#citing) does it keep, close,
  decide or change?
- Does one snapshot or authority token cover the whole operation?
- Is graph visibility still one manifest publication?
- Is every pre-publication durable effect unreachable until the manifest names
  it, and are the table pins and accepted contract complete in that publication?
- Does it read linear HEAD outside the collector's foreign-version listing or
  qualified original-version admission for an unregistered added-type path, or
  rebuild a row without carrying `omnigraph.last_linear_version` forward?
- Does the collector's mark set still cover every retained pin, and does its
  sweep still reach every unretained published version?
- Are names kept separate from stable identity?
- Does a missing physical optimization preserve logical correctness?
- Are retries, memory, I/O, and failure outcomes bounded?
- Does policy still cover non-HTTP writers?
- Is current truth stated once and linked elsewhere?
- Did the change extend the existing test owner and read the complete relevant
  Lance domain?
