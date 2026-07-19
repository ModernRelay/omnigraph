# Mutations

Write statements live inside a `query` declaration whose body is one or more
mutation statements (the [query language](../queries/index.md) covers the read
shape and shared declaration syntax).

```
query onboard($name: String, $title: String) {
  insert Person { name: $name, title: $title }
}
```

An edge type is inserted the same way — its endpoint columns are just
properties in the assignment block (`insert WorksAt { person: $p, org: $o }`).

## Statements

- `insert <Type> { prop: <value>, … }`
- `update <Type> set { prop: <value>, … } where <prop> <op> <value>`
- `delete <Type> where <prop> <op> <value>`

`<value>` is a literal, `$param`, or `now()`.

On a blob-bearing type, an update materializes and rewrites blob payloads only
for the rows matched by its predicate, including blobs the update does not
change. This keeps correctness independent of physical index state, but adds
read/write I/O proportional to the matched blob bytes; use selective update
predicates for large blobs.

## Atomicity

A change query publishes **one commit** at the end of the query. Multiple
insert/update statements accumulate in memory and commit together — a mid-query
failure leaves the graph untouched. See [transactions](../branching/transactions.md)
for the per-query atomicity contract and [branches](../branching/index.md) for
multi-query workflows.

Concurrent changes use optimistic concurrency over the whole target branch.
Retryable insert/upsert/load operations whose branch authority changed before
physical effects may be discarded and fully revalidated with a bounded
internal retry. A load in `append` mode remains strict insert through such a
retry; it never changes mode. Strict Update/Delete/Overwrite operations instead
return a structured read-set conflict. This branch-wide token is deliberately
conservative: a change to a different table can invalidate a prepared strict
write because constraints may have read it.

Every node and edge table is keyed physically by `id`. The storage transaction
for an insertion or upsert carries an exact-`id` conflict filter:

- a keyed node `insert` is an upsert by its derived `id`;
- generated-ID node and edge inserts are strict inserts;
- `load --mode append` is strict insert: an existing `id` returns structured
  `key_conflict` and the existing row is unchanged;
- `load --mode merge` is upsert: an existing `id` is updated.

For a node with `@key`, the derived `id` represents the complete typed key
tuple, not only its first property. Single-column keys use the canonical scalar
spelling; composite keys use a JSON array of canonical scalar strings in stable
property-identity order. The same renderer is shared by mutation and load, and
every member of a composite key is immutable during an update. See the
[schema language](../schema/index.md#table-layout) for the exact scalar rules.

A concurrent same-key conflict has effect-aware handling. If no table effect
from the attempt landed, strict insert is rechecked against fresh
manifest-visible state and returns terminal `key_conflict` only when one of its
attempted IDs now exists. A generic storage conflict with no exact match is
never mislabeled as a duplicate: the engine discards the whole strict attempt
and performs a bounded reprepare without changing it to upsert. Upsert likewise
discards the whole attempt and reruns preparation and validation. If an earlier
table already advanced, or the engine cannot prove the attempt effect-free, it
returns `recovery_required` and retains the recovery intent. No partial attempt
is retried around unresolved state.

Keyed mutation and load tables use one storage transaction per graph commit.
Each table's accumulated strict-insert or upsert input is limited to 8,192 rows
and 32 MiB of staged Arrow memory. An update also streams its predicate matches
against the table's remaining budget after pending rows are counted and
same-query pending IDs shadow committed rows; stored Blob size is checked before
its payload is read. Exceeding either limit returns HTTP **413** with structured
`resource_limit` details before the recovery intent is armed, with no durable
effect. Submit a larger incremental load as explicit chunks—
each chunk is a separate atomic graph commit—or use `--mode overwrite` for an
initial bulk replacement.

Blob values supplied as external URIs have mode-dependent storage semantics.
Keyed insert/upsert and load `append`/`merge` sum the declared ranges or object
sizes before reading payload bytes, reject an aggregate above 32 MiB, and copy
accepted bytes into the staged blob. Load `overwrite` keeps the external URI
cell as a reference. This distinction exists because Lance's merge-insert
builder cannot accept the `WriteParams` option used by Overwrite.

If the synchronous barrier finds an unresolved overlapping recovery intent, or
if a conflict is discovered after a Lance table effect is durable, the request
returns `recovery_required` with an operation id. Do not immediately retry that
request; reopen the graph read-write (or restart the server) so the durable
recovery intent is resolved first.

## Inserts/updates and deletes cannot mix in one query

A single change query must be **either insert/update-only or delete-only**.
Mixing the two is rejected at parse time, before any I/O:

> `mutation '<name>' on the same query mixes inserts/updates and deletes; split
> into separate mutations: (1) inserts and updates, then (2) deletes.`

Run two separate queries instead — the inserts/updates first, then the deletes.
Each query is still atomic on its own. This is a deliberate rule: inserts,
updates, and deletes all stage and commit through the same path, but keeping a
single query to one kind means its read-your-writes stays unambiguous (a read
within the query never has to reconcile rows you inserted against rows you
deleted in the same query). If you need the inserts/updates and deletes to land
as **one** atomic commit, run them on a branch and merge it.

## Bulk loading

For loading data from files rather than inline statements, use
[`omnigraph load`](../cli/index.md) (`--mode overwrite|append|merge`) — it is the
single bulk-write command and applies the same schema validation and atomic
publish as inline mutations. `append` means strict insert, `merge` means upsert,
and `overwrite` replaces the target image; these are logical semantics, not
names of raw Lance operations.
