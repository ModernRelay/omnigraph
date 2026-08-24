# Mutations and Loads

Mutation statements live inside a named `.gq` query. Run them with
`omnigraph mutate`.

```gq
query hire($person_id: String, $company_id: String, $role: String) {
  insert WorksAt {
    from: $person_id,
    to: $company_id,
    role: $role
  }
}
```

Edge endpoints use the reserved assignments `from` and `to`; their values are
the logical ids of existing endpoint nodes.

## Statements

```gq
insert Person { email: $email, display_name: $name }
update Person set { display_name: $name } where email = $email
delete Person where email = $email
```

Assignment values can be literals, parameters, or `now()`.

A mutation query may contain several inserts and updates, or several deletes,
but it cannot mix inserts or updates with deletes. Split that workflow into two
queries, or run the queries on a branch and merge when the combined result is
ready.

## Atomicity

An effectful mutation query publishes one graph commit. All of its statements
become visible together, or none do. Separate mutation commands are separate
commits, even when they run consecutively.

With `--json`, a successful effectful mutation returns `commit` with the
exact `graph_commit_id` and commit metadata published by that attempt. A
successful mutation that matches no entities publishes no commit and returns
`"commit": null`. Load JSON responses use the same exact receipt.

For a multi-command workflow, use a branch as isolated staging. Earlier changes
remain committed on that branch; merging makes the resulting branch state
visible on the target in one atomic step. See
[Branches, Commits, and History](../branching/index.md).

## Insert and update identity

- Inserting a node with `@key` is an upsert by its derived id.
- Inserting a node without a key is a strict insert with a generated or supplied
  id.
- Edge inserts are strict inserts with a generated or supplied id.
- Key properties cannot be changed by an update.

All declared value, uniqueness, endpoint, and cardinality constraints are
checked before publication.

## Bulk loading

`omnigraph load` accepts newline-delimited JSON. One file can contain nodes and
edges of several types:

```jsonl
{"type":"Person","data":{"email":"ada@example.com","display_name":"Ada"}}
{"type":"Company","data":{"slug":"acme","name":"Acme"}}
{"edge":"WorksAt","from":"ada@example.com","to":"acme","data":{"role":"Engineer"}}
```

Here, `Person.email` and `Company.slug` are single-property String keys, so their
derived ids are exactly `ada@example.com` and `acme`. The edge uses those ids in
`from` and `to`.

Choose the mode explicitly:

| Mode | Existing id | Use |
|---|---|---|
| `append` | Fails with `key_conflict` | Add entities without replacing anything. |
| `merge` | Updates the existing entity | Upsert a batch. |
| `overwrite` | Replaces every node or edge type represented in the batch; types absent from the batch remain unchanged | Rebuild from a complete export or seed. |

```bash
omnigraph load --data batch.jsonl --mode merge graph.omni
```

One load request is one graph commit. Use `--branch <name> --from <base>` to
create a missing review branch and load onto it in the same workflow.

## Limits and conflicts

Incremental keyed writes are bounded to 8,192 entities and 32 MiB per touched type
in one commit. Every strict load also rejects an input whose projected in-memory
representation exceeds 32 MiB. Split a larger import into explicit commits; use
one initial overwrite only when it fits, followed by merge chunks.

A stale strict update, delete, or overwrite can return `read_set_conflict`.
Refresh the branch and retry deliberately. A `key_conflict` means an append or
strict insert found an existing id; it never silently becomes an upsert.

If a write returns `recovery_required`, do not immediately resubmit it. Reopen
the graph read-write or restart the server, then retry from a fresh branch head.

## Blobs

Blob assignments accept managed `base64:` data and, when allowed by graph
policy, external URI references. Ownership differs by load mode, and Blob bytes
count toward write limits. See the canonical [Blob guide](../blobs.md) before
loading them.

## Conditional mutations

Use `--if-commit` when a mutation should apply only to the graph state that
the caller read:

```bash
# The JSON response includes the commit pinned for these rows.
omnigraph query find_person --query queries.gq --store graph.omni --json

omnigraph mutate update_person --query queries.gq --store graph.omni \
  --params '{"name":"Ada"}' \
  --if-commit <graph_commit_id> --json
```

The condition compares the effective head of the target branch. Any
intervening commit invalidates it, including a commit that changed an unrelated
entity or type. On mismatch, nothing is written: the CLI exits with code 4 and
JSON output contains `precondition_failure` with `expected` and optional
`actual` commit ids. Re-read the branch and decide again; do not blindly retry
the old mutation. The precondition is still checked when the mutation would
match no entities.

Use the `graph_commit_id` returned with the original query rows. Fetching a
head id afterward creates a race between the read and the precondition. For the
HTTP header and dedicated conditional routes, see the
[HTTP server guide](../operations/server.md#conditional-mutations).
