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
   visible table pointer and graph-lineage update. A writer may move Lance HEADs
   first only under durable recovery ownership. Per-table publication is never
   graph publication.

3. **Every operation uses one coherent accepted view.** A read holds one
   immutable snapshot for its lifetime. A writer captures schema, catalog,
   branch, graph head, native ref identity, and table baselines from one
   attempt, then revalidates that complete authority before effects. A retry
   starts a new attempt; it never combines fresh and stale facts.

4. **A mutation publishes once.** `mutate`, `load`, schema apply, merge, and
   equivalent multi-table operations stage all participants and publish once.
   They do not acknowledge or publish per statement or per table. The D2
   constructive-versus-destructive mutation split remains explicit.

5. **Recovery is part of the commit protocol.** Any independently durable
   effect that could become graph-visible must have enough persisted identity,
   authority, and intended outcome to roll forward or compensate safely.
   Ambiguous or foreign movement fails closed. Writers resolve or refuse
   relevant recovery before replanning. See [recovery.md](recovery.md).

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
    accepted schema are authoritative. Immutable version-pinned state may be
    cached; mutable-tip caches are hints, never commit authority. Do not
    maintain a shadow copy that can drift or rebuild a warm projection from
    full history on every call.

13. **Evidence matches the boundary.** Test the layer whose contract changed.
    Irreversible storage, protocol, or substrate decisions require an RFC plus
    compatibility, refusal, crash, and rebuild evidence. Cost claims require a
    checked-in instrument. Extend the existing owner in
    [testing.md](testing.md) before adding a parallel fixture.

## Deny-list

Treat these shapes as rejected until an RFC demonstrates why the case is
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
- maintained parallel truth or cold full-history reconstruction per request.

## Current support boundaries

- The server is cluster-only. Runtime graph add/remove is performed by
  `cluster apply` followed by restart, not an HTTP mutation.
- Azure writes require the admission wrapper and remain a qualification preview
  pending the adversarial live-Azure matrix. The narrower managed-identity
  smoke proof is complete.
- Some Optimize and destructive full-recovery decisions retain a
  one-mutation-process boundary because Lance does not expose the exact
  caller-owned maintenance transaction proof they would need for distributed
  takeover. The live write-entry heal's effect-free retirement (issue #554)
  relies on the same boundary: its proof-then-delete is fenced by
  process-local gates, so a second mutation process's live Armed intent, or
  an already-transmitted storage write of a just-dropped in-process writer,
  is outside what it can observe — the same bounded residual the Full-sweep
  abandonment of an effect-free intent has always carried.
- Physical index reconciliation is explicit; there is no background scheduler
  whose queue is a second authority.

These are constraints, not roadmap ledgers. Change one only with the owning
code, tests, guide, and—when irreversible—RFC.

## Review checklist

- Does one snapshot or authority token cover the whole operation?
- Is graph visibility still one manifest publication?
- Is every pre-publication durable effect owned by recovery?
- Are names kept separate from stable identity?
- Does a missing physical optimization preserve logical correctness?
- Are retries, memory, I/O, and failure outcomes bounded?
- Does policy still cover non-HTTP writers?
- Is current truth stated once and linked elsewhere?
- Did the change extend the existing test owner and read the complete relevant
  Lance domain?
