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
| A direction | named in [Direction](#direction) | not cited as kept, only as moved toward or away from |

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
  on every branch. An edge is directed from `src` to `dst`, has a type,
  properties and its own id; unkeyed edges of one type may join the same two
  nodes more than once, and a `@key` over the pair makes it one entity.
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
one is refused before it publishes. Valid means the constraints the schema
declares: any other rule over several entities is the caller's, and
concurrent writes can break it (`write:3`, `merge:7`).
*Why:* the graph is shared, so one bad write is every agent's wrong premise.
*Kept by:* invariant 8.

**E4. Its answer is true and final.** Success names the commit `D15`, or says
that nothing was published, and comes only after the commit is stored for good
and visible to every later read `G1`. A failure says what it left behind:
nothing, part, or unknown `D12`, and whether sending it again can succeed:
a moved head or contention may, a broken constraint or a denial cannot. An
unknown outcome is settled by reading history for a mark the caller put in
its write (a named head, or a key or unique value it chose), never by
sending the request again unseen.
*Why:* agents act on answers with nobody watching, and they retry.
*Kept by:* invariants 5 and 11.

**E5. It is bounded.** It never presents a cut-off answer as complete; that
holds always. It returns its whole answer within declared limits or refuses,
and contention ends in a refusal after a bounded number of attempts; these
hold while storage requests return or time out.
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

**Two strengths of promise.** `E1` to `E4`, `E7`, the never-skips part of
`read:5` and the refusals of `merge:3` are integrity: a violation is
permanent, and no retry or wait repairs it. The delay of the change feed,
the coverage of indexes and the freshness of values derived by a pipeline
are timeliness: they may lag and catch up. `read:2` is the one timeliness
this page promises.

## Each kind

| | sees | changes | refuses when | cost grows with |
|---|---|---|---|---|
| **read** | head at start, or a named commit | nothing | commit forgotten; search index missing | rows touched |
| **write** | head at publish; a `.gq` mutation may require a named head `D3` `D4` | one commit, or none `D2` | constraint broken; named head moved | what changed |
| **merge** | two heads and their ancestor | one commit on the target, or none | sides disagree on an entity `D5`; constraint broken | what both sides changed |
| **schema** | head of `main` at publish | one commit, or none | stored data breaks a new rule; another branch is live `D6` | types changed |
| **branch** | a commit to start from | a name, never data `D1` | name taken | not the graph's size |
| **maintain** | all heads | physical state, never an answer `G3` | graph is held; the branch moved since it began | what is out of shape |
| **forget** | all branches | removes commits | consent missing; graph is held | history removed |

- **Head at start:** the branch head when the read captures its snapshot,
  after it is authorized and after any wait for a schema change (`D7`). A
  commit published before that moment is seen; one published after it is
  not.
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
| **read** | free | free | read waits for the apply `D7` | read waits for a create, not a delete `D7` | free; waits for `schema upgrade-system-columns` `D7` | free `G5` |
| **write** | | same branch: queue; other branch: free `G2` | queue; a write that selects rows prepared on the old schema is refused `D8` | create: queue; delete of the same branch: queue, and a write landing after it is refused; delete of another branch: free | `optimize`: a write on `main` waits for the run, then lands unchanged; other branches free. `index build`: free, and the build is refused if a write lands first `D8` | write waits; `cleanup` is refused if `main` moved before it began `D8` |
| **schema** | | | queue | one: apply is refused while another branch is live `D6`; create: queue | queue; the maintenance may be refused | queue |
| **branch** | | | | same name: one; creates queue with each other and with deletes; deletes of different names: free | create: queue; delete: free with `optimize`, queue with `cleanup` and with `index build` on that branch | queue |
| **maintain** | | | | | queue; a second `optimize` or `index build` started without the server is then refused | queue; `cleanup` is refused if `optimize` published first |
| **forget** | | | | | | `cleanup`: queue; graph removal: one |

The table describes one process. Across two processes, a `write`, `merge`,
`schema` or `maintain` that loses the race for a head is prepared again or
refused, and every pair involving `branch` or `forget` is unsupported `G2`.
An export or a `changes baseline` is the exception among reads: it and a
`schema`, `branch` create or delete, `optimize` or `cleanup` refuse each
other without waiting `D7`.

## Details

Statements that only one kind needs. Where a guide already owns the topic,
the guide has the full rules and this page keeps the promise.

### read

1. `read:1` A result is a bag of rows, or a set under `return distinct`. Rows
   have no order unless the query gives one with `order` or a ranking. Under
   a sort, ties break by the ids of the query's bindings `D10`.
2. `read:2` On one branch, a client's requests made one after another always
   see its own acknowledged writes and never go back in time, whichever
   process answers. More than that: any client that learns, by any channel,
   that a commit was acknowledged and then reads that branch sees it. A
   client that needs a fixed view names a commit. Exception: a process that
   claimed a branch reads it without checking the head, so once another
   process replaces it its reads are stale, and a client can see an older
   state than it saw before.
3. `read:3` A past state is read by commit id, through today's schema. The
   read is refused when a property it asks for was added, renamed or
   created again since that commit.
4. `read:4` `nearest` says whether it searched everything (`exhaustive`). A
   full-text search is refused when its index is not built.
5. `read:5` The change feed delivers a branch's own commits in order from a
   cursor the caller holds and never skips one. A merge commit carries what
   it merged, and the merged branch's own commits do not appear. A feed can
   start at any retained commit of that history. A consumer sees each commit
   at least once, and a commit delivered again is the same block with the
   same commit id, so a consumer that stores the commit id or the cursor
   with its effect applies each commit once. Two branches' feeds have no
   order between them. Forgotten history, a schema change and a cursor from
   an earlier branch of the same name are refusals, never silent gaps.
6. `read:6` Any two commits can be compared, except across a schema change
   over tables that hold rows, which is refused.
7. `read:7` `changes baseline` returns an exact snapshot of one commit and a
   cursor that resumes the feed with the first commit after it. A consumer
   installs the snapshot before it saves the cursor.

Full rules: [search](../user/search/index.md),
[changes](../user/branching/changes.md).

### write

1. `write:1` Inside one mutation, a later insert or update sees the earlier
   statements' rows. Its delete statements all read the starting commit and
   remove everything they match. A mutation holds inserts and updates, or
   deletes, never both. So a writer that records a position (a sync's
   place in its source) can commit it with its inserts and updates, and
   needs a second commit for deletes.
2. `write:2` A strict insert never becomes an upsert. An insert into a `@key`
   type is an upsert by design. A key cannot be updated: changing it deletes
   one entity and inserts another.
3. `write:3` A write with a named head applies only if that commit is still
   the head; the comparison is made at publication with the current head,
   never with the snapshot the write was prepared on. Without one, a write
   lands on whatever the head is when it publishes: a value read in one
   request and written back in another can then overwrite a commit made in
   between (a lost update), which cannot happen inside one mutation. `blob put` and `blob clear` can require the cell's ETag
   instead, and `commit publish` always requires the commit it was built on. A proposal publishes only while the head is the commit it was
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
   `value_constraint_violation`. A value is therefore unique on the branch
   that wrote it, and final only once merged: a claim made on a task branch
   is not exclusive until it reaches the shared branch.
4. `merge:4` Conflicts, orphan edges and uniqueness violations name the
   entity. Cardinality and value violations name the type; a cardinality
   message names the source node in its text, with no entity field.
5. `merge:5` Nothing is resolved automatically, and there is no "ours" or
   "theirs" rule. A conflict is resolved by writing to a branch and merging
   again.
6. `merge:6` The common ancestor is picked by a fixed rule, so the same merge
   gives the same result. A merge over its declared size limits is refused.
7. `merge:7` A merge checks what was written, not what was read. Two branches
   that each raised a counter from 3 to 4 merge to 4. To count across
   branches, record each increment as its own entity and count them in a
   read: both branches' entities survive the merge.

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
   expires. It expires when its target's head moves past the commit it was
   built on, or when its target branch is deleted or created again.

### maintain

1. `maintain:1` Maintenance runs only when asked. Nothing runs it in the
   background.
2. `maintain:2` On a served cluster, `optimize` runs only through the
   server, as a recorded operation the operator enables. `index build`,
   `index rebuild` and `cleanup` are refused while the server holds the
   cluster, and `upgrade` refuses any graph inside a cluster.
3. `maintain:3` Between maintenance runs the cost of a read grows with the
   files and deletions written since the last `optimize`, and storage grows
   with retained commits until `cleanup`. Neither is bounded by the engine
   `D17`.

### forget

1. `forget:1` `cleanup` removes commits outside the keep rule (`--keep`,
   `--older-than`). It never removes a live head, a commit needed as a merge
   ancestor, or a tagged commit. A change-feed cursor retains nothing: a
   consumer behind the keep rule is refused and installs a new baseline.
2. `forget:2` `cleanup` asks for consent in the CLI only.
3. `forget:3` Removing a graph from a cluster deployment deletes its whole
   root.
4. `forget:4` A deleted value's bytes are gone only after `cleanup` and a
   rewrite of the file that held them `D11`. There is no way to remove one
   value from retained history: `cleanup` removes whole commits, so a value
   in a live head, a merge ancestor or a tagged commit stays, and so does
   any copy the operator's storage versioning or backups hold.

## Gaps

A gap needs a decided target. "Decided by" names the invariant or accepted
RFC that set it.

| Gap | Where | Today | Decided by |
|---|---|---|---|
| `G1` | `E4` | On a local disk nothing is forced to disk: a commit survives a process crash, not a power loss. On an object store it is stored for good at success | Invariants deny-list: no acknowledgement before durable visibility |
| `G2` | `write+write`, `branch+branch` | One writer process per cluster is supported. Branch create and delete, schema apply, dropping a proposal and `cleanup` are safe from one process only | [Graph access and cross-process writes](../rfcs/2026-10-16-graph-access-serving-and-cross-process-writes.md), accepted, in progress |
| `G3` | `E7`, `maintain.changes` | Building a full-text index can change `bm25` scores, their order and a limited result | Invariant 7 |
| `G4` | Scope | Azure writes are a preview that needs an external admission lease | [RFC 0029](../rfcs/0029-azure-blob-storage.md), accepted, in progress |
| `G5` | `read+forget` | Nothing coordinates `cleanup` with a running read, in one process or across two. A read pinned to a commit the keep rule drops is refused as reclaimed when it opens a table after the removal, and fails with an untyped storage error when a file under an open table is removed. No test covers it | Invariant 3 |

## Decisions

Each row is a question whose answer would change a statement on this page.
Until it is decided, the statement is today's behavior.

| Decision | Where | Today | Open question |
|---|---|---|---|
| `D1` | `E7`, `branch.changes` | Deleting a branch makes its commits that no live branch reaches through parents or merged parents unreadable by id at once, so a published commit is not final across a delete: a reader that acted on one cannot read it again. `commit show` can still return such a commit's record | Keep them readable by id until `cleanup`, or make delete a `forget`? |
| `D2` | `write.changes` | A write that changes no value still publishes a commit, and so does a load with no rows | Publish nothing? |
| `D3` | `write.sees` | Only a `.gq` mutation can require a named head | Every write? |
| `D4` | `write.sees` | A named head is refused by any commit in between, related or not | A condition as narrow as what the caller read? |
| `D5` | `merge.refuses` | Two agents that changed different properties of one entity conflict | Merge by property? It can build an entity neither branch wrote, so the merge would have to check per-entity constraints too |
| `D6` | `schema.refuses`, `schema+branch` | Schema apply runs on `main` only, with no other branch live | A schema change made on a branch and merged? The test would be that each side's schema reads the other's data: additions optional or defaulted, no identity reused |
| `D7` | `read+schema`, `read+branch` | Inside one process a read can wait behind schema apply or branch create. In one process an export or `changes baseline` and a branch create or delete, schema apply, `schema upgrade-system-columns`, `optimize` or `cleanup` refuse each other without waiting, and two exports refuse each other; nothing excludes them across processes | Reads never wait or refuse? |
| `D8` | `write+schema`, `write+maintain`, `write+forget` | Writers wait while schema apply or `cleanup` runs, and writers on `main` wait while `optimize` runs, for a time a run started without the server does not bound | Writes continue? |
| `D9` | `E6` | Reads are checked at the server only. Policy scope stops at the branch class | Check reads at every door? Scope by type and property? |
| `D10` | `read:1` | Groups that tie on the sort keys come back in no stated order | One total order for every sorted read? |
| `D11` | `forget:4` | `optimize` does not rewrite every file that holds a deleted value | An erasure path? |
| `D12` | `E4`, Kinds | A stream that fails after its headers ends in a cut-off body. A merge that deletes its source can report success when the delete failed | Every failure typed? |
| `D13` | Scope | One server serves a cluster. There is no read-only serving mode | Read-only servers? |
| `D14` | Kinds | A load onto a new branch is two steps | One commit? |
| `D15` | `E4` | The CLI's direct `schema apply` reports a manifest version and no commit id; the engine and a served deployment name the commit | Name the commit at every door? |
| `D17` | `maintain:3` | Maintenance runs only when asked, so at a steady write rate the debt grows until an operator acts, and `optimize` then makes writers on `main` wait | Who runs maintenance when the work never stops: a reconciler started by a threshold, or only the operator? |
| `D18` | `read:3` | A past read is refused when a property it asks for was added or renamed since | Resolve a past read through identity: a renamed property under its new name, one added since as null, refusing only one created again? |
| `D16` | `E5` | A served request can raise four resource settings up to the larger of the server's value and the built-in default, so a budget an operator set below the default can be raised back to it | The server's value as the ceiling? |

## Direction

Where the work asks this contract to go. The work is several agents sharing
one typed model to retrieve and update context and to coordinate
([the work](workload.md)), and it asks for more than any statement above
promises. Nothing in this table is promised or decided: each row needs an
accepted RFC, and until then no statement changes. It is here so that a
design can move toward it and never away from it. A design that makes a row
harder to reach says so in its contract line.

| The work asks for | Because | Related |
|---|---|---|
| A condition as narrow as what the caller read | Many agents write shared state, and an unrelated commit should not refuse a decision | `D4` |
| Exclusive claims with one winner | Agents take tasks from one pool, and an unrelated commit should never make a claim fail. A claim that expires needs a version its holder's later writes must name, not only an expiry | `D4` |
| Merge by property | Two agents that enriched different properties of one entity did not disagree | `D5` |
| Declared dependencies, reported at merge when stale | A conclusion should not merge on evidence that changed since the fork | `merge:7` |
| A check that gates a merge | Unchecked work by parallel agents compounds errors | `merge.refuses` |
| Schema change on a branch, merged with branches live | The model changes under the same review as the data | `D6` |
| A request identity on every write | Agents retry after a lost answer, and a retry must apply at most once. The identity must come from the client: one the server assigns does not survive a lost answer | `E4` |
| Revert as one operation | A wrong but valid change, by an agent, a person or a whole pipeline run, must be undoable without losing the rest | Not promised |
| Task and reason on a commit | An agent holds no memory of its own: why a change was made is a read | `E6` |
| A change feed that waits, with a condition | Agents and pipelines react to changes without polling, and wake only for what concerns them. A read names the commit it saw and a wait starts after it, so the two join without a gap | `read:5` |
| Work in progress visible in the state | Agents avoid duplicate work only if they can see who is working on what | `branch` |
| Search over the schema | Finding which type or property holds something is among an agent's most frequent reads | `read` |
| Valid time and supersession | The work asks what was the case, not only what was recorded | Words |
| Any number of reader and writer processes | Read load grows with agents, and writers work on many branches at once. Added readers keep the freshness check: a cached head would bring back lag and break `read:2` | `G2`, `D13` |
| Service through maintenance and process replacement | The work is continuous, which includes old and new binaries serving one graph during a replacement | `D8`, Scope |
| A check of stored state against its history | Errors in a shared graph compound, and storage that lies is outside the contract, so it should at least be detectable | Scope |
| A write that changes nothing publishes nothing | A sync mostly rewrites what did not change | `D2` |

Today's behavior and the full wording of each ask are in
[the work](workload.md#asked-not-decided).

## Not promised

- One all-or-nothing change across two commits, two branches or two graphs.
  Work on a branch and merge it.
- A view across two branches at once. A merge, a two-sided diff and a branch
  create read each branch on its own.
- A read "as of" a time, or a stale read that trades freshness for speed.
- Revert, or restoring a branch to an earlier commit. Undo is a new commit,
  or a branch started at the earlier commit.
- Automatic conflict resolution.
- Order across branches by cause: a commit records no dependency on what its
  writer read on another branch.
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
  put-if-absent and read-after-write: S3-compatible, or Azure `G4`. The
  store's put-if-absent is the one agreement the whole contract rests on:
  it decides which commit is next on a branch. A store that accepts two
  writes of one version, or whose read misses an acknowledged write, breaks
  `E1`, `E2` and `write:3` with nothing to detect it; a shared network
  filesystem may be such a store. Omnigraph copies nothing to a second place
  and encrypts nothing at rest.
- **Failures covered.** A process may stop at any point. A storage request
  may fail, land late or answer unclearly. A response may be lost. Storage
  that lies is not covered. Statements about what is seen and changed hold
  under any delay or pause; bounds (`E5`) hold only while storage answers.
  Two faults are not failures of the engine and are met another way. A bug
  in a binary is the same in every process running it. A caller, an agent
  included, may make a valid but wrong change: it stays recoverable from
  history, by a new commit or a branch at the earlier commit, until
  `cleanup` removes it. `cleanup` and removing a graph are the only
  operations that cannot be undone, and they are guarded by consent.
- **Time.** Commits are ordered by branch history, never by a clock. Clocks
  are used for timestamps, which are information only, for `now()` in a
  query, for `cleanup --older-than` and the seven-day orphan rule, and for
  the S3 server lock lease. A skewed clock can make `cleanup --older-than`
  drop or keep a commit early, never a live head, a merge ancestor or a
  tagged commit. No promise about what is seen or in what order depends on
  a clock.
- **Processes.** One writer process per cluster `G2`. One server serves a
  cluster, and read-only CLI commands may read beside it `D13`. A process
  paused past its lock lease can publish graph content only through the
  compare-and-swap on the head, which refuses it if the head moved; its
  branch deletes and `cleanup` deletes carry no such check and can still
  land.
- **Versions.** A binary refuses a storage format it does not know before it
  reads, and never misreads it. A newer binary's publish may raise a
  branch's format, after which older binaries refuse that branch, so every
  process on a graph is replaced together
  ([versioning](versioning.md)).
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
