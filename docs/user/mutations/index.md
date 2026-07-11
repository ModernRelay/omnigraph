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
Insert/Merge/Append operations whose branch changed before physical effects are
discarded and fully revalidated with a bounded internal retry. Strict
Update/Delete/Overwrite operations instead return a structured conflict. This
branch-wide token is deliberately conservative: a change to a different table
can invalidate a prepared strict write because constraints may have read it.

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
publish as inline mutations.
