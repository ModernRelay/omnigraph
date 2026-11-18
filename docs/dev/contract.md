# The contract

**Audience:** everyone who changes or reviews Omnigraph, coding agents included
**Authority:** what each operation promises its caller, and where the engine
differs today; the rules that keep the promises are in
[invariants](invariants.md), the mechanisms in the area guides, and the work
the promises serve in [the work](workload.md)

Omnigraph is one shared, typed graph that many agents read and change. Every
change is one commit. Every read sees one commit. Branches are separate
workspaces joined by merge. History stays until someone removes it on purpose.

This page is organized by operation. Operations fall into seven kinds, and
the page asks the same questions of each kind. It uses plain words; the
precise terms behind them (linearizability, session guarantees, snapshot
isolation) are in [the work](workload.md#in-the-literatures-terms).

## How to read this page

Every statement has an address, so a change can say which one it touches.

| Thing | Address | Example |
|---|---|---|
| A rule for every operation | `E` and a number | `E2` |
| A cell of the grid | `kind.column` | `write.sees` |
| A cell of the pairs table | `kind+kind`, in table order | `read+forget` |
| A detail | `kind:` and a number | `merge:3` |
| A gap | `G` and a number | `G2` |
| An open decision | `D` and a number | `D1` |

A mark after a statement says how far to trust it.

| Mark | Meaning |
|---|---|
| none | Claimed. No test is named for it yet |
| `✓` | A named test holds it |
| `G2` | The target is decided and today differs. The statement is the target; the gap row says what happens today |
| `D1` | The target is not decided. The statement is today's behavior; the decision row says what is open |
| `?` | Nobody has stated it |

## Words

- **Graph:** nodes and edges under one schema, at one storage root. Every
  promise on this page is about one graph.
- **Entity:** one node or edge, identified by its type and id. A `@key`
  derives the id from the key properties, so the same key is the same entity
  on every branch.
- **Schema:** the accepted `.pg`: types, properties and constraints
  (required, `@key`, `@unique`, `@range`, `@check`, enums, edge cardinality).
- **Commit:** an unchangeable version of the whole graph, with an id, its
  parent or parents, its branch, and its actor when known.
- **Branch:** a name for a line of commits. `main` always exists. A deleted
  name that is created again is a different branch.
- **Head:** the newest commit of a branch.
- **Proposal:** a commit stored on no branch (`as commit`), published later
  or dropped. A **snapshot** (`as unattached branch`) is a stored commit that
  is never published.
- **Door:** a way in: the embedded engine, the CLI, or the HTTP server with
  its MCP surface.
- **Physical state:** files, indexes, caches and layout. It is not part of
  the graph.

## Kinds

An operation's kind is what it is allowed to change. Operation names are
those of the shared inventory in
`crates/omnigraph-api-types/src/operations.rs`.

| Kind | May change | Operations |
|---|---|---|
| `read` | nothing | `query`, `query invoke` of a stored read, `queries list`, `export`, `blob get`, `blob stat`, `schema show`, `schema plan`, `lint`, `snapshot`, `commit list`, `commit show`, `commit changes`, `diff`, `branch list`, `branch diff`, `branch merge base`, `changes poll`, `changes baseline` |
| `write` | one commit on one branch | `mutate`, `query invoke` of a stored mutation, `load`, `blob put`, `blob clear`, `commit publish`; a mutation written `as commit` or `as unattached branch` stores its commit on no branch |
| `merge` | one commit on the target branch | `branch merge` |
| `schema` | one commit on `main` that changes the schema | `schema apply` |
| `branch` | a name, never data | `branch create`, `branch delete`, `commit drop`; claiming a branch (embedded engine only) |
| `maintain` | physical state | `optimize`, `index build`, `index rebuild`, `upgrade`, `schema upgrade-system-columns` |
| `forget` | removes commits | `cleanup`; removing a graph from a cluster deployment |

Two operations are two steps:

- `load` onto a branch that does not exist yet: `branch`, then `write` `D14`.
- `branch merge` that also deletes its source: `merge`, then `branch`.

The steps are not all-or-nothing together. If a later step fails, the earlier
one stays, and the answer says so (`effects: partial`) `D12`.

## Every operation

**E1. It sees one commit.** An operation reads the graph as of one commit,
every type together, for its whole run. It never sees part of another
operation's change.
*Why:* an agent decides from what it read, and a mixed view is a state that
never existed. *Kept by:* invariant 3.

**E2. It changes all or nothing.** An operation that changes the graph
publishes one commit or none. No statement, type or table becomes visible on
its own.
*Why:* an agent can stop at any point, and half a step misleads every other
agent. *Kept by:* invariants 2 and 4.

**E3. It leaves the graph valid.** Every constraint of the schema holds at
every commit, whichever door or writer made it. An operation that would break
one is refused before it publishes.
*Why:* the graph is shared, so one bad write is every agent's wrong premise.
*Kept by:* invariant 8.

**E4. Its answer is true and final.** Success names the commit `D15`, or says
that nothing was published, and comes only after the commit is stored for good
and visible to every later read `G1`. A failure says what it left behind:
nothing, part, or unknown `D12`. An unknown outcome is settled by reading
history, never by sending the request again unseen.
*Why:* agents act on answers with nobody watching, and they retry.
*Kept by:* invariants 5 and 11.

**E5. It is bounded.** It returns its whole answer within declared limits, or
refuses. It never presents a cut-off answer as complete. Contention ends
after a bounded number of attempts with a refusal.
*Why:* nobody is watching to notice a hung or shortened answer.
*Kept by:* invariant 11.

**E6. It is checked.** The server resolves the caller from its token; a client
cannot name itself. The server checks every action against the installed
policy. The engine checks every write when a policy is installed, before the
first lasting effect, so a denied write changes nothing. A direct CLI write to
a cluster graph uses that cluster's policy. Reads through the engine or the
direct CLI are not checked `D9`. The CLI, HTTP and MCP share one list of
operations, one set of error codes and one failure shape; the embedded engine
returns its own error type.
*Why:* agents of different authority share one graph.
*Kept by:* invariant 10.

**E7. It stays in its kind.** An operation changes only what its kind may
change. Only `forget` removes published commits `D1`. Only `maintain`
rewrites physical state, and it never changes an answer `G3`.
*Why:* history is the agents' memory, and a search must mean the same
whatever has been built to speed it up. *Kept by:* invariants 7 and 12.

## Each kind

| | sees | changes | refuses when | cost grows with |
|---|---|---|---|---|
| **read** | head at start, or a named commit | nothing | commit forgotten; search index missing | rows touched |
| **write** | head at publish; a `.gq` mutation may require a named head `D3` `D4` | one commit, or none `D2` | constraint broken; named head moved | what changed |
| **merge** | two heads and their ancestor | one commit on the target, or none | sides disagree on an entity `D5`; constraint broken | what both sides changed |
| **schema** | head of `main` at publish | one commit, or none | stored data breaks a new rule; another branch is live `D6` | types changed |
| **branch** | a commit to start from | a name, never data `D1` | name taken | not the graph's size |
| **maintain** | all heads | physical state, never an answer `G3` | graph is held | what is out of shape |
| **forget** | all branches | removes commits | consent missing; graph is held | history removed |

- **Head at start:** the branch head when the operation begins. A commit
  published during the operation is not seen.
- **Head at publish:** the branch head when the change lands. If the head
  moved, the write is prepared again on the new head, or refused.
- **Named head:** the caller says "only if the head is still commit X".

Not allowed (`E6`) and over a limit (`E5`) apply to every kind and are not
repeated in the grid. The cost column restates invariant 11 for each kind;
the quantities behind it are in [the work](workload.md).

## Two at once

What happens when two operations run at the same time. A merge acts as a
write on its target and reads its source at the start.

- `free`: both run, and neither waits for or sees the other.
- `queue`: both run, one after the other.
- `one`: one wins and the other is refused.

| | read | write | schema | branch | maintain | forget |
|---|---|---|---|---|---|---|
| **read** | free | free | read may wait in one process `D7` | read may wait in one process `D7` | free | free `G5` |
| **write** | | same branch: queue; other branch: free `G2` | write waits `D8` | `?` | write waits `D8` | write waits `D8` |
| **schema** | | | queue | one: apply is refused while another branch is live `D6` | `?` | queue |
| **branch** | | | | same name: one; other names: free `G2` | `?` | `?` |
| **maintain** | | | | | `?` | `?` |
| **forget** | | | | | | `?` |

## Details

Statements that only one kind needs. Where a guide already owns the topic,
the guide has the full rules and this page keeps the promise.

### read

1. `read:1` A result is a bag of rows, or a set under `return distinct`. Rows
   have no order unless the query gives one with `order` or a ranking. Under
   a sort, ties break by the ids of the query's bindings `D10`.
2. `read:2` On one branch, a client's requests made one after another always
   see its own acknowledged writes and never go back in time, whichever
   process answers. A client that needs a fixed view names a commit.
   Exception: a process that claimed a branch reads it without checking the
   head, so its reads are stale once another process replaces it.
3. `read:3` A past state is read by commit id, through today's schema. The
   read is refused when a property it asks for was added, renamed or
   created again since that commit.
4. `read:4` `nearest` says whether it searched everything (`exhaustive`). A
   full-text search is refused when its index is not built.
5. `read:5` The change feed delivers a branch's own commits in order from a
   cursor the caller holds and never skips one. A merge commit carries what
   it merged. A consumer sees each commit at least once. Forgotten history, a
   schema change and a cursor from an earlier branch of the same name are
   refusals, never silent gaps.
6. `read:6` Any two commits can be compared, except across a schema change
   over tables that hold rows, which is refused.

Full rules: [search](../user/search/index.md),
[changes](../user/branching/changes.md).

### write

1. `write:1` Inside one mutation, a later insert or update sees the earlier
   statements' rows. Its delete statements all read the starting commit and
   remove everything they match. A mutation holds inserts and updates, or
   deletes, never both.
2. `write:2` A strict insert never becomes an upsert. An insert into a `@key`
   type is an upsert by design. A key cannot be updated: changing it deletes
   one entity and inserts another.
3. `write:3` A write with a named head applies only if that commit is still
   the head. Without one, a write lands on whatever the head is when it
   publishes. A proposal publishes only while the head is the commit it was
   built on.

Full rules: [writes](writes.md).

### merge

1. `merge:1` A merge compares source and target with one common ancestor,
   entity by entity. The source is unchanged, and only the target head moves.
2. `merge:2` The same change on both sides is kept once, deletes included.
   Different changes to one entity are a conflict: `divergent_insert`,
   `divergent_update`, `delete_vs_update`. Changes to different entities
   never conflict, unless together they break a constraint.
3. `merge:3` A merged result that breaks a constraint is refused:
   `orphan_edge`, `unique_violation`, `cardinality_violation`,
   `value_constraint_violation`.
4. `merge:4` Conflicts, orphan edges and uniqueness violations name the
   entity. Cardinality and value violations name the kind only.
5. `merge:5` Nothing is resolved automatically, and there is no "ours" or
   "theirs" rule. A conflict is resolved by writing to a branch and merging
   again.
6. `merge:6` The common ancestor is picked by a fixed rule, so the same merge
   gives the same result. A merge over its declared size limits is refused.
7. `merge:7` A merge checks what was written, not what was read. Two branches
   that each raised a counter from 3 to 4 merge to 4.

Full rules: [merge](../user/branching/merge.md).

### schema

1. `schema:1` A rename keeps a type's or property's identity and history. A
   drop followed by an add of the same name is a new thing.
2. `schema:2` Apply checks stored data against each constraint it adds and
   refuses the ones the data breaks.

### branch

1. `branch:1` A branch starts at a head or at a published commit of a live
   branch. Creating it copies no data.
2. `branch:2` A proposal becomes unreadable when it is dropped or when it
   expires. It expires when another commit lands on its target.

### maintain

1. `maintain:1` Maintenance runs only when asked. Nothing runs it in the
   background.
2. `maintain:2` On a served cluster, only `optimize` runs beside the server.

### forget

1. `forget:1` `cleanup` removes commits outside the keep rule (`--keep`,
   `--older-than`). It never removes a live head, a commit needed as a merge
   ancestor, or a tagged commit.
2. `forget:2` `cleanup` asks for consent in the CLI only.
3. `forget:3` Removing a graph from a cluster deployment deletes its whole
   root.
4. `forget:4` A deleted value's bytes are gone only after `cleanup` and a
   rewrite of the file that held them `D11`.

## Gaps

A gap needs a decided target. "Decided by" names the invariant or accepted
RFC that set it.

| Gap | Where | Today | Decided by |
|---|---|---|---|
| `G1` | `E4` | On a local disk nothing is forced to disk: a commit survives a process crash, not a power loss. On an object store it is stored for good at success | Invariants deny-list: no acknowledgement before durable visibility |
| `G2` | `write+write`, `branch+branch` | One writer process per cluster is supported. Branch create and delete, schema apply, dropping a proposal and `cleanup` are safe from one process only | [Graph access and cross-process writes](../rfcs/2026-10-16-graph-access-serving-and-cross-process-writes.md), accepted, in progress |
| `G3` | `E7`, `maintain.changes` | Building a full-text index can change `bm25` scores, their order and a limited result | Invariant 7 |
| `G4` | Scope | Azure writes are a preview that needs an external admission lease | [RFC 0029](../rfcs/0029-azure-blob-storage.md), accepted, in progress |
| `G5` | `read+forget` | Reported, not traced end to end: `cleanup` can remove a commit that a running read is using | Invariant 3 |

## Decisions

Each row is a question whose answer would change a statement on this page.
Until it is decided, the statement is today's behavior.

| Decision | Where | Today | Open question |
|---|---|---|---|
| `D1` | `E7`, `branch.changes` | Deleting a branch makes the commits only it held unreadable at once | Keep them readable by id until `cleanup`, or make delete a `forget`? |
| `D2` | `write.changes` | A write that changes no value still publishes a commit, and so does a load with no rows | Publish nothing? |
| `D3` | `write.sees` | Only a `.gq` mutation can require a named head | Every write? |
| `D4` | `write.sees` | A named head is refused by any commit in between, related or not | A condition as narrow as what the caller read? |
| `D5` | `merge.refuses` | Two agents that changed different properties of one entity conflict | Merge by property? |
| `D6` | `schema.refuses`, `schema+branch` | Schema apply runs on `main` only, with no other branch live | A schema change made on a branch and merged? |
| `D7` | `read+schema`, `read+branch` | Inside one process a read can wait behind schema apply or branch create. While an export runs, branch create and delete, schema apply, `optimize` and `cleanup` are refused, and an export is refused while one of them runs | Reads never wait or refuse? |
| `D8` | `write+schema`, `write+maintain`, `write+forget` | Writers wait while schema apply, `optimize` or `cleanup` runs | Writes continue? |
| `D9` | `E6` | Reads are checked at the server only. Policy scope stops at the branch class | Check reads at every door? Scope by type and property? |
| `D10` | `read:1` | Groups that tie on the sort keys come back in no stated order | One total order for every sorted read? |
| `D11` | `forget:4` | `optimize` does not rewrite every file that holds a deleted value | An erasure path? |
| `D12` | `E4`, Kinds | A stream that fails after its headers ends in a cut-off body. A merge that deletes its source can report success when the delete failed | Every failure typed? |
| `D13` | Scope | One server serves a cluster. There is no read-only serving mode | Read-only servers? |
| `D14` | Kinds | A load onto a new branch is two steps | One commit? |
| `D15` | `E4` | `schema apply` reports a manifest version on success, not a commit id | Name the commit? |

New capabilities that the work asks for change no statement here until they
are decided, so they are listed in [the work](workload.md#asked-not-decided):
revert, a change feed that waits, task and reason on a commit, a listing of
work in progress, a request identity, exclusive claims, merge checks, declared
dependencies, search over the schema, and valid time.

## Not promised

- One all-or-nothing change across two commits, two branches or two graphs.
  Work on a branch and merge it.
- A view across two branches at once. A merge, a two-sided diff and a branch
  create read each branch on its own.
- A read "as of" a time, or a stale read that trades freshness for speed.
- Revert, or restoring a branch to an earlier commit. Undo is a new commit,
  or a branch started at the earlier commit.
- Automatic conflict resolution.
- A rule the schema does not declare.
- Per-row policy.
- A recall figure for a `nearest` that did not search everything.
- Erasure by delete alone, or by a deadline.
- A latency bound.
- A consistent backup from copying a root while writers run.

## Scope

- **One graph.** No operation spans two graphs. A shallow clone reads its
  base's files. `init`, which creates a graph, is outside the kinds. Which
  graphs, policies and stored queries a cluster holds is owned by
  [control plane](control-plane.md), and so are server state reads such as
  health, `access` and operation status.
- **Doors.** The embedded engine, the CLI and the HTTP server with MCP. A
  process that reads or writes the files directly is outside the contract;
  keeping it out is the operator's job.
- **The store.** A local filesystem with hard links, or an object store with
  put-if-absent and read-after-write: S3-compatible, or Azure `G4`. Omnigraph
  copies nothing to a second place and encrypts nothing at rest.
- **Failures covered.** A process may stop at any point. A storage request
  may fail, land late or answer unclearly. A response may be lost. Storage
  that lies is not covered.
- **Time.** Commits are ordered by branch history, never by a clock. Clocks
  are used for timestamps, which are information only, for `now()` in a
  query, for `cleanup --older-than` and the seven-day orphan rule, and for
  the S3 server lock lease.
- **Processes.** One writer process per cluster `G2`. One server serves a
  cluster, and read-only CLI commands may read beside it `D13`.
- **Trust.** A direct or embedded caller states its own actor. A served
  graph with no policy denies every authenticated request, unless the
  operator lets static tokens read it. An operator can also start a server
  with no authentication. Both choices are logged at startup.
- **Outside the commit.** Stored vectors are computed by `embed` or by the
  client. A `nearest` given text calls the embedding provider during the
  read. An external Blob names an object outside the root, which a commit
  does not keep.

## Citing

A design, an RFC or a pull request for a non-trivial change carries one line:

```text
Contract: keeps E2, write.changes; closes G2; decides D1
```

| Verb | Meaning |
|---|---|
| `keeps` | The change touches this statement and it still holds |
| `closes` | The gap is gone. Delete its row; its number is not used again |
| `decides` | The decision is settled. It becomes a statement, or a gap with its RFC |
| `changes` | A statement changes. Write the RFC first |

A cost claim also names the activity it is for in [the work](workload.md).

A test names the statement it holds in a comment, `// contract: merge:2`. A
statement gets its `✓` from such a test, never by hand.
