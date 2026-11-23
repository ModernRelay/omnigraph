# Unified graph-write protocol

**Decides:** every graph write prepares against one captured branch authority without writer gates, revalidates that whole authority under ordered gates, and becomes visible through one `__manifest` publication, or prepares again.
**Contract:** keeps E1, E2, E3, E5, write:3
**Code:** `crates/omnigraph/src/db/omnigraph.rs` (`WriteTxn`, `WriteAuthorityToken`), `crates/omnigraph/src/db/reprepare.rs`, `crates/omnigraph/src/db/write_queue.rs`, `crates/omnigraph-catalog/src/publisher.rs`, `crates/omnigraph/tests/forbidden_apis.rs`
**Cited as:** RFC 0022

## Context

Mutation, load, merge, schema apply, index builds and Optimize use different
Lance operations, and each had its own check and its own race. A write's
validation reads tables it does not write: a `@unique` value, an edge
endpoint, a cardinality bound, a merge classification. A publication that
compares only the touched tables admits a commit that no serial order of the
writers produces. Process-local queues order writers in one process and
nothing across two. Table effects are detached commits
([Detached table commits](0067-detached-table-commits.md)), so this protocol
owns only capture, revalidation and publication.

## Decision

- An attempt captures, from one coherent `__manifest` read, its base snapshot
  and a coarse authority token: the branch incarnation, the exact graph head
  and the accepted schema identity (IR hash and identity domain).
- Preparation (validation, merge classification, embedding, staging) runs
  without writer gates, against the catalog built from the captured schema,
  never the handle's current one.
- Before publication the attempt takes its gates in one total order: the
  schema gate, then the branch gate, then the table gates sorted by key. The
  gates serialize writers inside one process and are never the correctness
  authority across processes.
- Under the gates the attempt revalidates the whole token. Every graph-content
  publication and every schema change advances the graph head, so the head
  arbitrates the tables a write only read.
- A mismatch sends the attempt back to prepare again from its request on the
  current head. A head moved only by publications that changed no row carries
  the attempt instead
  ([Writes carried over row-preserving publications](2026-11-10-writes-carried-over-row-preserving-publications.md)).
  A plan is never published over rows it did not see.
- Preparing again is bounded (`MAX_PRE_EFFECT_REPREPARES`); exhaustion returns
  `ReadSetChanged` to the caller. A caller's expected head makes a moved head
  a terminal `PreconditionFailed`, never a reprepare.
- One graph commit is one `__manifest` publication. The publisher compares the
  token again on every attempt; a lost race is a refusal, never a rebase.
- The writer set is closed. Raw storage, coordinator and dataset handles are
  crate-private, and `forbidden_apis.rs` classifies every public write entry
  point and durable call.

## Rejected

- **One generic staged-storage method:** Lance has no staged form for every
  operation and writers move versions differently, so one method would hide
  the differences or fill with writer-kind branches.
- **Compare-and-swap on the touched tables only:** uniqueness, edge endpoints,
  cardinality and merge classification read tables the write does not touch.
- **Refreshing expected versions under the old plan:** a plan validated on one
  base is not valid on another; an insert can have become an update.
- **Process-local queues as distributed fencing:** they coordinate one process;
  across processes only the publication arbitrates (invariant deny-list).
