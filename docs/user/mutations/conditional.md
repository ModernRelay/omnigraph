# Conditional mutations

Use `--if-commit` when a mutation should apply only to the graph state that
the caller read:

```bash
# The JSON response includes the commit pinned for these rows.
omnigraph query find_person --query queries.gq --target ./graph.omni --json

omnigraph mutate update_person --query queries.gq --target ./graph.omni \
  --params '{"name":"Ada"}' \
  --if-commit <graph_commit_id> --json
```

The condition compares the effective head of the target branch. Any
intervening commit invalidates it, including a commit that changed an unrelated
entity or type. On mismatch, nothing is written: the CLI exits with code 4
against a server and with code 1 on an embedded graph such as this `--target`
run, and JSON output contains `precondition_failure` with `expected` and
optional `actual` commit ids. Re-read the branch and decide again; do not blindly retry
the old mutation. The precondition is still checked when the mutation would
match no entities.

Use the `graph_commit_id` returned with the original query rows. Fetching a
head id afterward creates a race between the read and the precondition. For the
HTTP header and dedicated conditional routes, see the
[HTTP server guide](../operations/server.md#conditional-mutations).
