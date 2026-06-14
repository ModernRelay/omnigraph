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

## Atomicity

A change query publishes **one commit** at the end of the query. Multiple
insert/update statements accumulate in memory and commit together — a mid-query
failure leaves the graph untouched. See [transactions](../branching/transactions.md)
for the per-query atomicity contract and [branches](../branching/index.md) for
multi-query workflows.

## Inserts/updates and deletes cannot mix in one query

A single change query must be **either insert/update-only or delete-only**.
Mixing the two is rejected at parse time, before any I/O:

> `mutation '<name>' on the same query mixes inserts/updates and deletes; split
> into separate mutations: (1) inserts and updates, then (2) deletes.`

Run two separate queries instead — the inserts/updates first, then the deletes.
The restriction exists because inserts/updates and deletes commit through
different paths today, and mixing them in one query creates ordering hazards
(e.g. a same-row insert-then-delete, or a cascading delete of a just-inserted
edge). Keeping the two kinds in separate queries keeps each one atomic and
correct.

## Bulk loading

For loading data from files rather than inline statements, use
[`omnigraph load`](../cli/index.md) (`--mode overwrite|append|merge`) — it is the
single bulk-write command and applies the same schema validation and atomic
publish as inline mutations.
