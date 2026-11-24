# Branch statements in GQ

**Decides:** branch create, delete, merge and list are GQ statements, each standing alone in its file and naming every branch it touches, and the compiler's classification of the file picks the door: control writes enter through `POST /mutate`, control reads through `POST /query`, each running the same handler body as its `/branches` route.
**Contract:** keeps E2, E6, merge.refuses, branch:4
**Code:** `crates/omnigraph-compiler/src/query/query.pest`, `crates/omnigraph-compiler/src/query/ast.rs`, `crates/omnigraph-server/src/handlers/dispatch.rs`, `crates/omnigraph-server/src/handlers.rs`, `crates/omnigraph-api-types/src/lib.rs`, `crates/omnigraph-cli/src/commands/query/prepare.rs`, `crates/omnigraph-gqt-core/src/lib.rs`
**Cited as:** RFC 0055

## Context

Branch operations existed only as HTTP routes and CLI verbs. A script or
agent working on a branch switched from GQ for rows to HTTP for refs and
back, and the logic-test format, which runs GQ steps, could not hold a fork,
write, merge, read story, so merge defects had Rust-only regressions or
none. The reasons for keeping refs off the language (rows versus refs, one
policy action per route, one atomic mutation per body) are real and must
survive the move.

## Decision

- A file is a settings prefix and then either `query` declarations or one
  top-level statement. `branch create <name> [from <parent>]`, `branch
  delete <name>`, `branch merge <source> [into <target>]` and `branch list`
  are statements, never part of a mutation body, with no parameters. The
  file body is an enum, so every consumer must match the statement arm.
- Keywords are word-bounded atomic rules and nothing is reserved; outside
  the statement head, `branch`, `merge` and `list` stay ordinary identifiers.
  A name outside the identifier alphabet is quoted.
- Classification picks the door. Control writes go to `POST /mutate` and
  `omnigraph mutate`; control reads (`branch list`, and the branch diff and
  merge-base reads) to `POST /query` and `omnigraph query`. A statement at
  the wrong door is a 400 naming the right one.
- A statement names its branches itself. A request branch, snapshot, query
  name, parameters or commit precondition beside one is refused, so no
  request carries two sources for one branch name.
- Each statement calls the same handler body as its route (`branch_*_body`):
  the same Cedar action and scope, admission check, engine call and error
  mapping. `branch list` is `read` with no branch scope, as `GET /branches`.
- A control write answers `ChangeOutput` with an `outcome` object (`kind`
  `created`, `deleted` or `merged`, the last with `merge` of
  `already_up_to_date`, `fast_forward` or `merged`), zero counts, and the
  target head as `commit` when the merge published one. `branch list` answers
  `ReadOutput` rows `{name}` sorted by byte order.
- A merge conflict is the same 409 with the same `merge_conflicts` list the
  route answers, never an outcome.
- The CLI classifies the source before sending, posts a statement with no
  request target, refuses target flags beside one locally, and asks for
  consent on `branch delete` as the `branch delete` verb does. The
  `omnigraph branch` verbs keep their routes and build no GQ text.
- Statements are never stored queries; the catalog loader refuses them.
- `.gqt` steps take an optional `branch: <name>`, hold a statement as a step
  body, and `outcome: <word>` asserts a merge's result.

## Rejected

- **Logic-test directives only (`--- branch`, `--- merge`):** closes the test
  gap, but agents still switch channels and the runner would own a branch
  grammar the compiler does not.
- **A session current branch or `checkout`:** the API is stateless and every
  request names its branch.
- **Branch operations inside a mutation body:** a body publishes one commit on
  one branch and a merge is its own commit; mixing them needs a transaction
  scope that does not exist.
- **A dedicated `POST /branch` route:** a third door, so a script picks a
  transport per statement family again, and every client must learn it.
- **A merge conflict as a 200 outcome:** splits the one error mapping, and
  every `/mutate` consumer reads 200 as applied.
- **An optional statement field on the file type:** a consumer can ignore a
  field, not a variant.
- **Reserve the keywords:** breaks existing property names for nothing.
