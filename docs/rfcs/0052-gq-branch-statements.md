---
rfc: "0052"
title: "Branch statements in GQ"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-09-04
updated: 2026-09-04
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0052: Branch statements in GQ

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

> The number is provisional: `0052` is the next available number at the
> upstream commit this document is anchored to, and the file, the heading,
> the frontmatter, and the registry row are renumbered together when the PR
> opens if another RFC has taken it by then.

## Summary

GQ gains one new top-level form beside `query … { read body }` and
`query … { mutation body }`: a ***branch statement***, one of `branch create
<name> [from <parent>]`, `branch delete <name>`, `branch merge <source>
[into <target>]`, and `branch list`, standing alone in its file. A branch
statement never appears inside a mutation body, takes no parameters, and
names every branch it touches itself. The compiler classifies it, as it
already classifies a body as read or mutation, and the classification picks
the ***door***, the route or CLI verb a statement enters through: `branch
create`, `branch delete`, and `branch merge` are ***control writes*** and
enter through `POST /mutate` and `omnigraph mutate`; `branch list` is a
***control read*** and enters through `POST /query` and `omnigraph query`.
A statement at the wrong door is refused with the same HTTP 400 the read
door gives a smuggled `insert` today. Each statement is authorized with the
Cedar action and scope pair its HTTP route uses today (`branch_create`,
`branch_delete`, `branch_merge`; `read` with no scope for the list) and runs
the same handler body and engine call, so no new permission path exists.
The answer travels in the door's existing envelope: `ReadOutput` rows for
`branch list`, `ChangeOutput` with a new optional `outcome` object for the
three control writes. A merge conflict is not an outcome: it is the same
HTTP 409 that `POST /branches/merge` answers today, carrying the same
conflict list. The CLI's `omnigraph query` and `omnigraph mutate`
classify the source of `-e`/`--query-string` and `--query <file>` before
sending and post a statement with no request target. RFC 0045's `.gqt`
format gains a `branch: <name>` argument on
`--- query` and `--- mutate` step headers, a branch statement as a step
body, and an `outcome:` expect mode, nothing else; the three merge-family
findings become its first cases.

The boundary that does not change: the four `/branches` routes stay and
keep their handlers and response types; the engine crate is untouched;
`QueryRequest` and `ChangeRequest` keep their fields; there is no session,
current branch, or checkout, since every request keeps naming its target;
a mutation body still publishes as one commit on one branch; the
`omnigraph branch` verb tree stays on its routes and shares only its
output text and its delete confirmation with the statement path. Whether
every statement kind should share one door is deferred to a follow-up RFC
(Alternatives 3).

## Motivation

Branch create, delete, merge, and list exist, at upstream commit `8ca9b12c`
(every `file:line` anchor in this document is at that commit), as four
HTTP routes, `GET` and `POST /branches`, `DELETE /branches/{branch}`,
`POST /branches/merge` (`crates/omnigraph-server/src/lib.rs:1895-1900`),
and as the CLI verbs wrapping them
(`crates/omnigraph-cli/src/main.rs:410-531`). GQ, the
language every read and every mutation is written in, has no statement for
any of them: the grammar's only top-level form is `query_decl`
(`crates/omnigraph-compiler/src/query/query.pest:8`). Three costs follow.

First, no logic test can pin a merge bug. RFC 0045's runner knows three
step kinds, `Query`, `Mutate`, `Restart` (`crates/omnigraph-gqt/src/lib.rs:72-76`),
grouped by `Item::Loop` (`lib.rs:62-69`),
and runs every query and mutate step against `main`
(`lib.rs:1295, 1365`). The merge family found by the deterministic
simulation harness therefore has Rust-only regressions or none:
[#583](https://github.com/ModernRelay/omnigraph/issues/583) (an edge
inserted on both sides of a fork is duplicated by the merge),
[#600](https://github.com/ModernRelay/omnigraph/issues/600) (a second
merge of an already-merged branch, closed: the harness driver re-merged a
merged branch and the engine answered correctly), and the 2026-09-04
nightly finding on
seed 221206, where a merge re-adopted an edge a sibling merge had deleted.
Each is a five-step story (fork, write, write, merge, read) that the format
was built to hold and cannot.

Second, a script or an agent that works on a branch switches channels
twice: GQ for rows, HTTP for refs, GQ again. Third, the CLI's ad-hoc
source flags, `--query <file>` and `-e`/`--query-string`, cannot carry a
branch operation; those live only in the verb tree
`BranchCommand::{Create, List, Delete, Merge}`
(`crates/omnigraph-cli/src/cli.rs:613-659`), so a script
that mixes rows and refs mixes verbs too. The verb tree itself stays (User
and operational behavior).

No RFC proposes or rejects the language route, and no document records why
branch control was kept off it; the closest written statements are the
atomicity table of `docs/user/branching/index.md:59-71` and RFC 0045's
sentence that mutation results carry no rows. The reasons are readable
from the code (rows versus refs, one Cedar action per route, one atomic
mutation per body), and this RFC keeps all three intact. A branch
statement adds control, not expressiveness: it has no match, return, or
projection semantics, and Compatibility makes it reversible, so it does
not pre-empt a holistic review of the language. A local fix is
not enough because the gap is a contract: a new query statement kind, a
wire-visible answer shape, and a test-format amendment, each named in the
registry's RFC-required list (`docs/rfcs/README.md:18`).

## User and operational behavior

The four statements, spelled once:

| Statement | Door | Default | Answer |
|---|---|---|---|
| `branch create <name> [from <parent>]` | `POST /mutate`, `omnigraph mutate` | parent `main` | `ChangeOutput`, `outcome.kind = "created"` |
| `branch delete <name>` | `POST /mutate`, `omnigraph mutate` | none | `ChangeOutput`, `outcome.kind = "deleted"` |
| `branch merge <source> [into <target>]` | `POST /mutate`, `omnigraph mutate` | target `main` | `ChangeOutput`, `outcome.kind = "merged"` with `outcome.merge` one of `already_up_to_date`, `fast_forward`, `merged`; a conflict is the HTTP 409 `POST /branches/merge` answers today |
| `branch list` | `POST /query`, `omnigraph query` | none | `ReadOutput`, one row per branch, column `name`, sorted by `name` in byte order |

The read door stays read-only: `branch list` is the only branch statement
`POST /query` accepts, it publishes no commit and changes no ref, and
`branch create`, `branch delete`, and `branch merge` are refused there
before any engine call.

Postconditions, one per statement. After `branch create <name> [from
<parent>]` answers `created`, `branch list` contains `<name>` and a read on
`<name>` returns the rows a read on `<parent>` returned at that moment.
After `branch delete <name>` answers `deleted`, `branch list` no longer
contains `<name>` and a read naming `<name>` fails as a read on an unknown
branch fails today (404, `branch '<name>' not found`, the same answer a
never-created branch gets). After `branch merge <source> [into <target>]`
answers `merged`, the target holds the merged state as
`docs/user/branching/index.md:68` promises for the route: "The resulting
source state becomes visible on the target in one atomic commit." A
conflicting merge publishes nothing: the
engine returns `OmniError::MergeConflicts` before any table state is
published (`crates/omnigraph/src/exec/merge.rs:5190-5192, 5333-5335`), so
the target's head and the source are unchanged.

Guarantee: a statement and its route produce the same engine effect, the
same Cedar decision, the same admission check, and the same error mapping
for every input, because they run one handler body (Design). They differ
only in envelope; a merge conflict is the same 409 on both fronts.

Guarantee: `branch list` is authorized as `GET /branches` is, `read` with
`branch: None` and `target_branch: None`, so for one actor it lists exactly
the names `GET /branches` lists, and a policy written for one branch
decides `branch list` exactly as it decides `GET /branches` today. A
scope-free `read` satisfies only a rule written for any branch, because a
branch-scoped rule compiles to `context.has_branch && …`
(`crates/omnigraph-policy/src/lib.rs:802-807`), so an actor whose only
`read` rule is branch-scoped is denied the listing on both fronts. A
branch-scoped listing is a tightening this RFC does not make
(Alternatives 9).

From the CLI, the statements arrive through the existing verbs and their
existing source flags (`--query <file>`, `-e`/`--query-string`,
`cli.rs:107-117, 140-150`):

```
omnigraph mutate -e 'branch create b0'
omnigraph mutate -e 'branch create "review/add-benchmark"'
omnigraph mutate -e 'branch merge b0 into main'
omnigraph query  -e 'branch list' --format table
```

A name outside the identifier alphabet, which includes every name with a
`/`, a `-`, a `.`, an uppercase letter, or a leading digit
(`review/add-benchmark`, `release.1.2`), is quoted; `b0` and `main` are
bare (Design, Grammar). Both verbs classify
the source before sending: a statement is posted with no request target,
and `--branch`, `--snapshot`, `--if-commit`, a positional `name`,
`--params`, or `--params-file` beside one fails locally
with the server's message (Design, CLI). `omnigraph query` renders the list
in all five of its formats unchanged (`read_format.rs:13-17`), since the
answer is an ordinary `ReadOutput`. Without `--json`, `omnigraph mutate`
prints one line per control write: `created branch b0 from main`, `deleted
branch b0`, `merged b0 into main: fast_forward`; a merge conflict is the
409 the client already turns into an error, exit code 1 with the server's
`error` text (`helpers.rs:523-528`), as every non-2xx answer is today. The
`omnigraph branch create|list|delete|merge` verbs stay on their routes
(`GraphClient::branch_create_from`, `branch_delete`, `branch_merge`,
`branch_list`) and keep their output text (`created branch <name> from
<parent>`, `deleted branch <name>`, `merged <source> into <target>:
<outcome>`, `main.rs:433, 472, 520-525`), their `--delete-branch`
composition (`POST /branches/merge` with `delete_branch: true`; the
statement path has no composition), and their `confirm_destructive` prompt
on delete (`main.rs:466`); the statement path shares their text renderer
and the delete confirmation. Deprecating the verb tree is not part of this
RFC.

Refusals. Each is an HTTP 400 on the server and a non-zero exit with the
same message in the CLI, naming the statement and the door to use, in the
shape of the existing `query '{}' contains mutations (insert/update/delete);
use POST /mutate for write queries`
(`crates/omnigraph-server/src/handlers.rs:1126-1131`):

| Rule | Refusal |
|---|---|
| a control write at the read door | `statement 'branch merge' is a control write; use POST /mutate` |
| `branch list` at the write door | `statement 'branch list' is a read; use POST /query` |
| `QueryRequest.branch` or `.snapshot`, or `ChangeRequest.branch`, set alongside a branch statement | `a branch statement names its branches itself; drop the request target` |
| `QueryRequest.name` or `.params`, or `ChangeRequest.name` or `.params`, set alongside a branch statement | `a branch statement takes no name and no parameters` |
| the `Omnigraph-If-Graph-Commit` header on `POST /mutate/if-graph-commit` (CLI `--if-commit`) alongside a branch statement (on `POST /mutate` the header is refused before any parse, `handlers.rs:1280`) | `a branch statement takes no commit precondition` |
| a branch statement on the deprecated `POST /read` or `POST /change` | `branch statements are not served on deprecated routes; use POST /mutate or POST /query` |
| a branch statement beside any `query` declaration, or inside a mutation body | a parse error from the compiler (Design, Grammar) |

Guarantee: no branch statement is ever executed at a door other than its
own, and a request that reaches the engine carries exactly one source of
truth for every branch name it acts on.

A refusal is a rule applied to people, so its evasions and honest routes
follow.

| Evasion | What stops it |
|---|---|
| wrap `branch merge` in `query m() { … }` to ride the mutation door as a mutation | the grammar: `branch_stmt` is a top-level alternative, not a `mutation_stmt`; the file does not parse |
| send a control write to `POST /read`, which does not reject mutations (`handlers.rs:587`) | deprecated routes refuse every branch statement before authorization |
| steer a merge's target, or make it conditional, through the request envelope (`ChangeRequest.branch`, `Omnigraph-If-Graph-Commit`) | the request-target and precondition refusals above: two sources for one fact are never reconciled (on `POST /mutate` the header is refused before any parse, `handlers.rs:1280`) |
| store a statement in the stored-query catalog and invoke it through `POST /queries/{name}` or `omnigraph mutate <name>` | refused at `cluster validate` and at server boot: a catalog file is a list of `query` declarations, so the cluster loader refuses a `QueryFile::Branch` with a diagnostic in the `query_parse_error` family (`omnigraph-cluster/src/config.rs:150`) and `QueryRegistry::from_specs` refuses it at load (Design, AST) |
| run the statement through the embedded CLI (`--store`) to skip server Cedar | nothing new to skip: in embedded mode no Cedar runs for any statement kind today. Every embedded arm opens the handle bare, `Omnigraph::open(uri)` (directly or through `open_embedded`, `client.rs:303-305`), and `Omnigraph::open` installs no policy (`crates/omnigraph/src/db/omnigraph.rs:785`; only `with_policy`, `:854`, does, and the CLI never calls it), so `enforce` returns `Ok` when no policy is configured (`:918-920`); the verbs' embedded arms (`client.rs:966-1050`) have the same property. `--store` is the operator's own machine and credentials; a statement there carries exactly the policy a verb carries, none. This RFC records the existing embedded rule and does not change it |
| put two statements in one request to get two forks in one call | the grammar: one statement per file; two statements are two requests, as they are two routes today |

| Honest route | Accepted |
|---|---|
| `POST /mutate` with `{"query": "branch create b0"}` and no `branch` field; `POST /query` with `{"query": "branch list"}` and no target | yes, the canonical doors |
| `POST /branches`, `DELETE /branches/{branch}`, `POST /branches/merge`, `GET /branches` | yes, unchanged |
| `omnigraph mutate -e '…'` or `--query <file>`, `omnigraph query -e 'branch list'`, with or without `--store`; `omnigraph branch …` on its routes | yes |
| a `.gqt` `--- mutate` step holding a control write, a `--- query` step holding `branch list` | yes (Design, Logic tests) |

Operationally nothing is new: a control write is admission-gated per actor
exactly as its route is (`state.workload.try_admit(&actor_arc, 256)` after
Cedar, `handlers.rs:2148, 2230, 2303`), `branch list` is not, and no
route, Cedar action, or configuration is added.

## Design

### Grammar

`query.pest` gains one top-level alternative and one statement family:

```
query_file    = { SOI ~ (branch_stmt | query_decl*) ~ EOI }

branch_stmt   = { kw_branch ~ (branch_create | branch_delete | branch_merge | branch_list) }
branch_create = { kw_create ~ branch_name ~ (kw_from ~ branch_name)? }
branch_delete = { kw_delete ~ branch_name }
branch_merge  = { kw_merge ~ branch_name ~ (kw_into ~ branch_name)? }
branch_list   = { kw_list }
branch_name   = { ident | string_lit }

kw_branch     = @{ "branch" ~ !(ASCII_ALPHANUMERIC | "_") }
kw_create     = @{ "create" ~ !(ASCII_ALPHANUMERIC | "_") }   // likewise kw_delete, kw_merge, kw_list, kw_from, kw_into
```

Each keyword is an atomic rule closed by a word boundary, so `branch
createb0`, `branchcreate b0`, and `branch merge b0 intomain` are parse
errors rather than statements acting on a misread name; pest's implicit
whitespace between non-atomic tokens would otherwise make the space
optional. The grammar has no reserved-word list: every keyword is a string
literal inside a rule, `ident` is any lowercase-start word
(`query.pest:111`), and the only exclusion anywhere is `edge_ident`'s
`!"not"` (`:108`). Nothing is reserved here either. The leading keyword
`branch` disambiguates at the one position where it can appear: a file
today must begin with `query` (`query.pest:8, 11`), so no file that parses
today changes meaning, and pest's ordered choice tries `branch_stmt` first
because `query_decl*` also matches the empty file. Inside bodies nothing
changes: `branch`, `merge`, `into`, `list`, and `create` remain ordinary
identifiers wherever one is legal, and `from` stays the plain identifier
edge inserts use (`insert Knows { from: $a, to: $b }`, `query.pest:32`,
recognized by string at `typecheck.rs:368`).

A branch name is an `ident` or a `string_lit`. `main` and `b0` are bare; a
name outside the identifier alphabet is quoted (`branch create
"review/add-benchmark"`), and so is a branch named `from` or `into` where
the bare word would read as the keyword: `branch create from main` parses
`from` as the name and fails at `main`, and `branch create b0 from` and
`branch merge b0 into` fail at end of input; `list` is a keyword only
directly after `branch`, so a branch named `list` is bare everywhere. A
quoted name is the decoded content of the literal (`\"`, `\\`, `\n`,
`\r`, `\t`; any other escape is a parse error, `decode_string_literal`,
`crates/omnigraph-compiler/src/error.rs:60-90`); the compiler refuses an
empty name, a name with leading or trailing whitespace, and a name
carrying a control character, so that the
spelled name and the name the engine acts on are one string; every other
rule is the engine's (`normalize_branch_name` trims and refuses empty,
`ensure_logical_branch_name` refuses an incarnation-shaped segment, and
`ensure_branch_create_namespace_safe` refuses an existing name and an
ancestor or descendant of a live name, `crates/omnigraph/src/db/omnigraph.rs:3744-3756,
2939-2960`, `crates/omnigraph/src/branch_names.rs:65-75`), and
`ChangeOutput.branch` carries the spelled name. A statement binds no
`$vars`: `param_list` belongs to `query_decl` (`query.pest:11, 35-36`).

Exclusivity is grammatical: a file is either a list of `query`
declarations or exactly one branch statement, so the two forms never share
a file, one atomic mutation per body is untouched by construction, and a
stored-query catalog can never carry a branch statement.

### AST and classification

The AST has no declaration enum today: `pub struct QueryFile { pub queries:
Vec<QueryDecl> }` (`crates/omnigraph-compiler/src/query/ast.rs:4-6`), and
read versus mutation is `mutations.is_empty()` on `QueryDecl`
(`ast.rs:9-19`). The third form is a new type beside `QueryDecl`, and
`QueryFile` becomes the classification:

```rust
#[derive(Debug, Clone)]
pub enum QueryFile {
    Queries(Vec<QueryDecl>),
    Branch(BranchStmt),
}

#[derive(Debug, Clone)]
pub enum BranchStmt {
    Create { name: String, from: Option<String> },
    Delete { name: String },
    Merge { source: String, into: Option<String> },
    List,
}

impl BranchStmt {
    pub fn is_write(&self) -> bool  // false only for List
}
```

A file-level enum rather than an added `Option<BranchStmt>` field, because
a consumer can ignore an `Option` and cannot ignore a variant: every
`.queries` consumer becomes a `match` and the compiler enumerates them. At
that commit they are `find_named_query`
(`omnigraph-compiler/src/query_input.rs:256`), the one seam through which
the engine parses (`crates/omnigraph/src/exec/query.rs:66, 118`,
`exec/mutation.rs:1082`); `select_named_query_decl` (`handlers.rs:2479`);
the CLI's `select_named_query` (`omnigraph-cli/src/helpers.rs:795`); the
runner's `file.queries.as_slice()` (`omnigraph-gqt/src/lib.rs:770`); the
cluster stored-query loader (`omnigraph-cluster/src/config.rs:158`) and
its per-query check `validate_query_source` (`config.rs:1081`);
`lint_query_file` (`query/lint.rs:129`); the stored-query registry's
parse site `QueryRegistry::from_specs` (`omnigraph-server/src/queries.rs:103`);
`typecheck_query_decl` and `typecheck_query` (`typecheck.rs:98-113`); and
`lower_query` (`ir/lower.rs:34`). For the typechecker, linter, lowerer,
registry, and cluster loader the new arm is a refusal (a branch statement
has no plan and is never stored); for `find_named_query` it is a refusal
too, which is why the engine crate stays untouched: the engine reaches a
declaration only through `find_named_query`, and a `Branch` file has none
to return. For the runner, the CLI, and the server the arm is the dispatch
below. Test files reach declarations through a `QueryFile::single_decl()`
helper, so the `.queries[0]` sites in `parser_tests.rs`,
`typecheck_tests.rs`, `lower_tests.rs`, and `omnigraph-gqt/src/tests.rs`
are one edit. `parse_query_decl`
(`parser.rs:47`) is untouched; a sibling `parse_branch_stmt` fills
`BranchStmt`.

The compiler classifies and never refuses a door: the read door's refusal
of a mutation is the server's (`run_query`, `handlers.rs:1126-1131`), and
the same holds here. Beside the three name refusals in Grammar, the
compiler owns two refusals, both parse
errors: a branch statement beside a `query` declaration, and a branch
statement inside a mutation body (no `mutation_stmt` alternative exists).

### Server dispatch

Both doors parse, classify, refuse the wrong kind, then authorize per kind:

1. `run_query` (`handlers.rs:1105`) and `run_mutate` (`handlers.rs:1032`)
   gain a first step, `classify(query) -> QueryFile`, which is
   `parse_query`, before any target resolution. Today `run_query`
   resolves and Cedar-authorizes the read target
   (`resolve_authorized_read_target`, action `Read` on the request's
   branch, default `main`) before parsing (`handlers.rs:1123` then
   `:1124`), and today `run_mutate` authorizes `Change` and admits before
   parsing (`handlers.rs:1046, 1061, 1066`); after this RFC the parse
   comes first at both doors, so a branch statement never pays a `Read`
   check on `main` on top of its own action. Likewise `run_mutate`'s own
   `Change` check and `try_admit` (`handlers.rs:1046-1064`) are skipped
   for a `Branch` file; the handler body's are the only ones. For
   `QueryFile::Queries` the remaining steps keep today's order, with one
   visible consequence owned in Compatibility: a parse error (400) now
   precedes the `Read` denial (403) on `/query` and `/read`, and precedes
   the `Change` denial (403) and the admission check on `/mutate`,
   `/change`, and `/mutate/if-graph-commit`.
2. `QueryFile::Branch(stmt)` with `stmt.is_write()` at `run_query`, or
   `BranchStmt::List` at `run_mutate`, is the wrong-door 400. Then the
   request checks: `QueryRequest.branch`, `.snapshot`, `.name`, or
   `.params`, `ChangeRequest.branch`, `.name`, or `.params`, or an
   expected head present alongside a branch statement is a 400. To make
   these checks possible inside the shared functions rather than in each
   axum shell: `run_mutate` takes `branch: Option<String>` (today every
   caller defaults it to `main` before the call, `handlers.rs:1221, 1281,
   1333, 1538`),
   `run_query` returns `ReadDispatch::{Rows(String, ReadTarget,
   QueryResult, Option<String>), BranchList(Vec<String>)}` in place of
   today's tuple (`handlers.rs:1114-1121`), and the `reject_mutations:
   bool` parameter (`/read` passes the literal `false`, `handlers.rs:587`)
   becomes `door: Door::{Query, Read, Mutate, Change}`, on which the
   refusal table keys: `Read` and `Change` refuse every branch statement,
   `Query` refuses a control write, `Mutate` refuses `branch list`
   (`/mutate/if-graph-commit` is `Mutate` with an expected head,
   `handlers.rs:1335`). `/queries/{name}` (`handlers.rs:1539, 1557`)
   passes `Mutate` and `Query`; its source is registry-owned and never a
   `Branch` file (AST), so neither refusal fires there. The
   request types do not change: `QueryRequest { query, name, params,
   branch, snapshot }` (`crates/omnigraph-api-types/src/lib.rs:648-666`)
   and `ChangeRequest` (`:801`) already carry any GQ source string.
3. Each `server_branch_*` handler splits into its axum shell (extractors)
   and a body function that both the route and the statement path call,
   so the `PolicyRequest`, the admission check, the engine call, and the
   error mapping are one piece of code:

| Statement | Handler body | Cedar `PolicyRequest` (`handlers.rs`) | Engine call |
|---|---|---|---|
| `branch create` | `server_branch_create` | `BranchCreate`, `branch: Some(from)`, `target_branch: Some(name)` (`:2138-2140`) | `db.branch_create_from_as(ReadTarget::branch(&from), &name, actor)` (`:2152`; `db/omnigraph.rs:3344`) |
| `branch delete` | `server_branch_delete` | `BranchDelete`, `branch: None`, `target_branch: Some(name)` (`:2222-2224`) | `db.branch_delete_as(&name, actor_id)` (`:2234`; `db/omnigraph.rs:3458`) |
| `branch merge` | `server_branch_merge` | `BranchMerge`, `branch: Some(source)`, `target_branch: Some(target)` (`:2293-2295`) | `db.branch_merge_as(&source, &target, actor_id)` (`:2307`; `crates/omnigraph/src/exec/merge.rs:4825-4851`) |
| `branch list` | `server_branch_list` | `Read`, `branch: None`, `target_branch: None` (`:2088-2090`) | `db.branch_list()` (`:2095`; `db/omnigraph.rs:3438`), then `sort()` (`:2097`) |

The three write actions are the only branch actions Cedar has
(`PolicyAction`, `crates/omnigraph-policy/src/lib.rs:18-74`; schema lines
`:849-851`); `branch list` is authorized as `read` with no scope, exactly
as its route is, not as a branch action. The `delete_branch` composition
of `POST /branches/merge` (a second `BranchDelete` check, `:2349`) has no
statement clause: an author writes `branch merge b0` then `branch delete
b0`, two statements, two checks, which is what the route does internally.

4. The answer. `branch list` fills `ReadOutput { query_name, target,
   row_count, columns, rows, graph_commit_id }`
   (`api-types lib.rs:271-285`) with `query_name: "branch list"`,
   `target: { branch: null, snapshot: null }` (the statement reads the
   ref list, not a branch, matching its scope-free Cedar request; no
   route emits this value today, since `read_target_from_request` always
   fills one of the two, `handlers.rs:2468-2477`, so Compatibility names
   it), `columns: ["name"]`, `rows` = one `{"name": "<branch>"}` per
   branch sorted by `name` in byte order (`Vec<String>::sort`,
   `handlers.rs:2097`), `row_count = rows.len()`, and `graph_commit_id`
   absent (`skip_serializing_if`, `api-types lib.rs:283`). The three
   control writes fill `ChangeOutput { branch,
   query_name, affected_nodes, affected_edges, actor_id, commit }`
   (`api-types lib.rs:313-320`) with `branch` = the branch that received
   the effect (the new branch; for `branch delete` the deleted branch,
   which no longer exists when the answer is read; the merge target),
   `query_name` = the statement's two words, both counts `0` (a control
   write moves refs, not rows; the counts are documented as not reported
   for control writes, in the field docs and in `print_change_human`),
   `commit` as the next paragraph says, and one new field:

```
outcome: Option<BranchOutcomeOutput>    // serde: skip when None
```

`commit` is `null` for `created`, `deleted`, and a merge whose `merge` is
`already_up_to_date`, none of which publishes a commit (a fresh branch's
inherited head is read back with any read on it, `api-types
lib.rs:278-282`). For `fast_forward`
and `merged` it is the target's head after the merge, filled by the
handler body: the engine's `MergeOutcome` carries no commit id
(`AlreadyUpToDate | FastForward | Merged`, `db/omnigraph.rs:69-74`), so
the body reads `db.list_commits(Some(target))`, whose first entry is the
newest by that function's contract (`db/omnigraph.rs:3556-3563`), and
renders it through `api::commit_output` as the `CommitOutput` a mutation
body's answer already carries (`api-types lib.rs:371-381, 1342`). The
merge holds both branch gates through publication
(`exec/merge.rs:4889-4892`) and the head read runs after they are
released, so under a concurrent writer on the target the id can name a
later commit; the exact merge commit id is an engine follow-up
(`MergeOutcome` carrying it), not this RFC. The engine crate stays
untouched.

`BranchOutcomeOutput` is a tagged object, `kind` in `snake_case`, its
fields taken from the route outputs that exist today
(`BranchCreateOutput`, `BranchDeleteOutput`, `BranchMergeOutput`,
`api-types lib.rs:123-140, 185-200`):

| `kind` | Fields | Source of each field |
|---|---|---|
| `created` | `from`, `name` | `BranchCreateOutput` minus `actor_id`, which `ChangeOutput` already carries, and minus `uri`, the graph URI the caller already addressed |
| `deleted` | `name` | `BranchDeleteOutput` minus `actor_id` and `uri` |
| `merged` | `source`, `target`, `merge` | `BranchMergeOutput`; `merge` is `BranchMergeOutcome`, wire strings `already_up_to_date`, `fast_forward`, `merged` (`api-types lib.rs:174-181`), under the key `merge` so that `merged` the kind (a merge that completed) and `merged` the three-way result never share a key |

A merge conflict has no `kind`: it is the route's error. `branch_merge_as`
returns `Err(OmniError::MergeConflicts)` (`crates/omnigraph/src/error.rs:172-173`),
the shared body maps it through `ApiError::from_omni` to
`ApiError::merge_conflict` (`omnigraph-server/src/lib.rs:1081-1089,
930-937`), status 409, `ErrorOutput { error, code: "conflict",
merge_conflicts: [MergeConflictOutput] }` (`api-types lib.rs:1244-1249`),
with `error` beginning `merge conflicts: ` (`summarize_merge_conflicts`,
`lib.rs:1260`). `MergeConflictOutput { entity_kind, type_name, entity_id,
kind, message }` (`api-types lib.rs:243-249`) and its seven kinds
(`:204-226`) are untouched.

Guarantee: a mutation body's `ChangeOutput` is byte-identical before and
after this RFC, because `outcome` is skipped when absent; a `branch list`
answer is a well-formed `ReadOutput` for every existing consumer of that
type, including the five CLI renderers; a conflicting `branch merge` and a
conflicting `POST /branches/merge` answer the same status, the same
`ErrorOutput`, and the same `merge_conflicts` list for the same input.

The one mechanism this design adds is "the compiler's classification picks
the door". Remove it and either every statement needs its own transport,
the four routes that exist today (Alternatives 1) or a fifth route for the
statement family (Alternatives 8), or one door accepts every kind
unclassified, which puts a write behind the read door's `Read` check.
Classification is already how the read door keeps writes out
(`handlers.rs:1126`); this RFC makes that rule file-level and applies it
symmetrically at the write door.

### CLI

The remote arms of both verbs change, because today both always send a
request target and `mutate` posts to a deprecated route. `Command::Query`
builds a `ReadTarget` from `--branch`/`--snapshot`, default `main`
(`resolve_read_target`, `main.rs:1116`), and `GraphClient::query`
serializes it as `branch` or `snapshot` on every `QueryRequest`
(`client.rs:862-876`). `Command::Mutate` resolves `branch` to `main`
(`resolve_branch(branch, None, "main")`, `main.rs:1164`;
`main_tests.rs:59-69` pins that the legacy body always carries `branch`),
and `GraphClient::mutate`
posts to `POST /mutate/if-graph-commit` when `--if-commit` is given and
otherwise to the deprecated `POST /change` with
`legacy_change_request_body` (`client.rs:782-797`, `helpers.rs:1061-1077`).
The wire cannot tell a defaulted target from an explicit one, and the
refusal table refuses both the target and the deprecated route, so without
a CLI change every statement the CLI sends is refused. Both verbs therefore
classify before sending: the remote arm parses the source (the embedded
arm already does, `select_named_query`, `helpers.rs:795-813`) and, for
`QueryFile::Branch`, `GraphClient::query` posts `QueryRequest` with
`branch: None, snapshot: None, name: None, params: None`, and
`GraphClient::mutate` posts `ChangeRequest` with `branch: None` to `POST
/mutate`, never to `/change`. An explicit `--branch`, `--snapshot`,
`--if-commit`, a positional query `name`, `--params`, or `--params-file`
(`ParamsArgs`, `cli.rs:897-900`) beside a statement fails locally with the
server's message, before any round trip. A `BranchStmt::Delete` runs
`confirm_destructive("branch delete", …)` on both arms
(`main.rs:466`; `helpers.rs:52-61` refuses a non-local target without
`--yes` or a TTY answer), the same consent step the `omnigraph branch
delete` verb takes,
so the statement path cannot delete on a remote server without it.
Mutation bodies keep
today's requests byte for byte, including the `/change` path. The CLI's
exact request shape for each statement is pinned in `data_routes.rs`
(Evidence and tests).

The embedded arm (`--store`) must classify locally, because it never meets
the server. After `parse_query`, a `QueryFile::Branch` dispatches to the
engine calls the embedded `BranchCommand` arms make today:
`branch_create_from_as` (`client.rs:969`), `branch_delete_as` then
`wait_for_fork_reclaims` (`:1000-1003`), `branch_merge_as` (`:1041`),
`branch_list` (`:325`). No Cedar runs in this arm for any statement kind,
because none runs there for anything today: every embedded arm opens the
handle bare, `Omnigraph::open(uri)` (directly or through `open_embedded`,
`client.rs:303-305`), and `Omnigraph::open` installs no policy
(`db/omnigraph.rs:785`; only `with_policy`, `:854`, does, and the CLI
never calls it), so `enforce` returns `Ok` with no policy configured
(`db/omnigraph.rs:918-920`). The existing embedded rule holds unchanged. A
conflicting merge in this arm is `Err(OmniError::MergeConflicts)` from the
engine, rendered as the CLI renders any engine error, message beginning
`merge conflicts: ` (`error.rs:172`), exit code 1.

The text renderer is shared. A `ChangeOutput` carrying `outcome` prints
the verb tree's line for that kind (`created branch b0 from main`,
`deleted branch b0`, `merged b0 into main: fast_forward`, `main.rs:433,
472, 520-525`) instead of `print_change_human`'s `changed main via branch
merge: 0 nodes, 0 edges` (`output.rs:879-887`), which would hide the
outcome word. The `omnigraph branch …` verbs keep calling
`GraphClient::branch_create_from`, `branch_delete`, `branch_merge`, and
`branch_list`, that is, the routes and the embedded engine calls they use
today; they build no GQ source, since a verb that rendered its arguments
into a statement string would be the shape of ad-hoc string generation
the deny-list rejects where a structured form exists
(`docs/dev/invariants.md:108-109`). `read` and `change` stay the visible aliases
of `query` and `mutate` (`cli.rs:101, 134`).

### Logic tests (RFC 0045 amendment)

Two amendments to RFC 0045's File format, both fail-closed under its own
evolution rule (`0045-gq-logic-tests.md:739-742`: "Format evolution is
fail-closed: unknown sections, unknown header keys, and missing required
headers are refusals, never silent skips, so an older harness refuses a
newer logic test rather than mis-running it"). That rule names sections
and header keys, not step arguments or expect modes, so the fail-closed
claim for these two amendments rests on the runner's own refusals: the
`takes no arguments` check (`lib.rs:749-751`) and `parse_expect_header`'s
unknown-mode refusal (`lib.rs:330-369`), and the `does not parse` refusal
on a statement body (`lib.rs:764-769`).

1. `--- query` and `--- mutate` accept one optional argument, `branch:
   <name>`, the branch the step runs against; absent, `main`, as today
   (`lib.rs:1295, 1365`). The seam is the `rest` of the header line, split
   off at the first space (`lib.rs:737-740`) and refused today with
   `` `--- {kind}` takes no arguments `` (`lib.rs:749-751`). A fifth
   `HEADER_KEYS` entry (`lib.rs:140`: `issue, red_on, notes, traversal`)
   is not the seam: a case header is per case, a branch target is per
   step. The argument follows the shape of `--- expect error:
   <substring>` (a word, a colon, the trimmed remainder); anything else in
   `rest` is refused with the grammar.
2. A `--- mutate` step may hold a control write and a `--- query` step may
   hold `branch list`, classified by the compiler. The wrong kind is
   refused beside the existing read/mutation refusals (`lib.rs:777-786`)
   with two new exact strings, since a statement is not a declaration:
   `` a control write under `--- query` is refused; use `--- mutate` `` and
   `` `branch list` under `--- mutate` is refused; use `--- query` ``. A
   step holding a statement refuses `branch: <name>` on its header (`a
   branch statement names its branches itself`) and a following `---
   params` (`a branch statement takes no params`; today `--- params`
   attaches to any pending step, `lib.rs:802-827`); the step's name, used
   in labels, is the statement's two words. `branch create` and `branch
   delete` take `ok` or `error: <substring>`. `branch merge` takes `ok`,
   `error: <substring>`, or the new mode `outcome: <word>`, body empty,
   `<word>` one of `already_up_to_date`, `fast_forward`, `merged`,
   asserting `outcome.merge`; `outcome:` on any other step is refused. A
   conflicting merge is an error, so `ok` fails on it and `error: merge
   conflicts` pins it (the substring is the start of
   `OmniError::MergeConflicts`'s message, `error.rs:172`, which the runner
   sees directly from the embedded handle). `affected:` on a control write
   is refused (no counts exist). `branch list` under `--- query` takes
   `unordered`, `ordered`, or `error:` over rows `{"name": "…"}`; its rows
   are sorted by `name` in byte order, a total order, so `ordered` is
   accepted and the `order`-clause refusal for declarations
   (`ordered_refusal`, `lib.rs:606-609`) does not apply to it. An
   `outcome:` word or `affected:` counts in a new case are blessed from a
   run, never copied from a design document.

The runner executes against the embedded handle (`lib.rs:31`), opened by
`Omnigraph::init` (`lib.rs:1410`) and reopened by `Omnigraph::open` on
`--- restart` (`lib.rs:1447`), neither with a policy, so a statement there
exercises compiler and engine, never the server's Cedar dispatch; the actor
is `None` and `enforce` is a no-op (`db/omnigraph.rs:918-920`). After a
`branch delete` step the runner awaits `wait_for_fork_reclaims` before the
next step, as the CLI does before exit (`client.rs:1000-1003`), because
`branch_delete_as` returns at the manifest flip and reclaims forks in a
background task (`db/omnigraph.rs:3455-3458`) and `--- restart` drops the
handle (`lib.rs:1445-1446`). Loops do not reach a statement: `${i}`
substitutes only in params and expect bodies and query and mutate bodies
stay literal (`0045:419-423`); a statement step has no params, and `${`
in a statement body is refused as in any other step body
(`lib.rs:1003-1005`). `--- restart` (`0045:391`) remains the one step
that is not GQ; this
amendment adds no directive, since the statements are GQ.

The two format pieces in one fragment (the outcome word is illustrative
and is blessed from a run when a case is written, per the blessing rule
above):

```
--- mutate
branch merge b0 into main

--- expect outcome: fast_forward

--- query
branch list

--- expect unordered
{"name": "b0"}
{"name": "main"}
```

Guarantee: an older harness refuses a case using either amendment (the
argument with `takes no arguments`, the expect mode as an unknown mode, a
statement body as `does not parse`, `lib.rs:764-769`) and never mis-runs
it.

## Invariants

- 2, one graph-content publication door: unchanged; a statement calls the
  engine function its route calls.
- 4, a mutation publishes once: strengthened by construction; a branch
  statement cannot share a file with a mutation body, and a merge keeps
  publishing as one commit (`docs/user/branching/index.md:68`).
- 9, query semantics are typed structures: the statement is an AST
  variant and the door is chosen by matching on it, never by a transport
  flag or a string compare on the request.
- 10, trust at the boundary, enforced at the engine: the server path
  authorizes with the route's own `PolicyRequest`; the embedded path
  reaches the `_as` entry points, which apply the gate whenever a policy
  is configured; the embedded CLI and the runner configure none today
  (Design, CLI), a property this RFC records and does not change.
- 13, evidence matches the boundary: grammar and AST in the compiler,
  dispatch and refusals in the server, the format in the logic tests; the
  engine's merge semantics keep their nineteen integration-test owners
  (`grep -l branch_merge crates/omnigraph/tests/*.rs`).

Deny-list: no side channel for query semantics (the classification is the
AST), no ad-hoc string generation, no parallel truth. No known gap
changes.

## Compatibility and reversibility

Wire. `ChangeOutput` gains one optional, skip-when-absent field; every
existing mutation response is byte-identical. `ReadOutput` gains no field
here, and its value space gains one value: `target: { branch: null,
snapshot: null }`, which only a `branch list` answer carries and no route
emits today. `branch list` fills `ReadOutput` whatever `rows`' Rust type
is, so RFC 0051 (JSON output via Arrow) may change that type without
touching this RFC, and `branch list` inherits the `Accept` negotiation
RFC 0051 adds to `POST /query`: under its Arrow `Accept` it answers a
one-column `name` IPC stream. The four `/branches` routes and their
output types,
`QueryRequest`, and `ChangeRequest` are unchanged. The deprecated `POST
/read` and `POST /change`, whose envelopes are frozen (`LegacyReadOutput`,
`api-types lib.rs:291`), refuse branch statements. A statement and its
route never diverge in behavior: a merge conflict is the same 409 on both
(Design, step 4).

Error precedence on `/query`, `/read`, `/mutate`, `/change`, and
`/mutate/if-graph-commit` changes for every caller, not only for
statements (`/queries/{name}` shares the functions, but its source is
parsed at load, `queries.rs:108`, so no order change is observable
there): today an actor denied `Read` gets 403 before any
parse (`handlers.rs:1123` then `:1124`); after this RFC a parse error, 400
with the parser's diagnostics, precedes the denial, so an authenticated
actor denied `read` spends parser time on a route with no admission gate
(`run_query` doc, `handlers.rs:1100-1104`); on the write routes the parse
now runs before `try_admit` (`handlers.rs:1061`), so an unparseable body
no longer consumes an admission slot and the parse itself is no longer
admission-bounded. A caller with no credentials never reaches the handler
when a policy is configured (`require_bearer_auth`, `handlers.rs:259-286`,
answers 401 first). Accepted, because the parse reads nothing beyond the
request body, and `/mutate` takes the same reorder: the door's Cedar
action is known only after classification, so both doors parse, classify,
then authorize (today `/mutate` authorizes and admits first,
`handlers.rs:1046-1066`); the alternative, parsing first only when
the source begins with `branch`, would make the door's behavior depend on
a string compare on the request, which Invariants 9 forbids.

CLI. `omnigraph query` and `omnigraph mutate`, given a statement through
`-e`/`--query-string` or `--query <file>`, change their remote requests
for statements only: no `branch`, `snapshot`, `name`, or `params`, and
`POST /mutate` instead of `POST /change`. Every request for
a mutation body or a read query is byte-identical to today's (Design,
CLI). The `omnigraph branch` verbs are unchanged.

Clients. Every client of `POST /mutate` becomes a branch-control client
after this RFC: an SDK `mutate` call and an MCP `mutate` tool (RFC 0003)
each carry `branch merge b0 into main` once their server accepts it.
Server-side Cedar is the gate on both fronts, so no actor's permissions
widen. A client that withheld branch operations from its users while
exposing a `mutate` entry point must now classify the source it sends or
accept the change.

Storage: none. Policy: no new action and no policy-file change; a policy
that grants `branch_merge` today grants `branch merge` tomorrow, and one
that denies it denies both fronts. Logic tests: fail-closed per RFC 0045.

Reverting: remove the grammar alternative and the `Branch` variant (the
compiler then enumerates every arm to delete), the `outcome` field, and
the header argument. Statements in flight become parse errors; routes and
verbs are unaffected; `.gqt` cases using the amendment are refused and
stay readable as behavior records.

## Alternatives

1. **Do nothing.** The merge family keeps Rust-only regressions, agents
   keep two channels, the reasons for the split stay unwritten.
2. **Keep HTTP-only and extend RFC 0045 with directives** (the simplest
   competitor): `--- branch <name>` (fork from `main`), `--- merge
   <source>` (into `main`, with an outcome expect), plus the same `branch:`
   step argument. This is the shape DuckDB's sqllogictest uses for
   `restart` and `load` and CockroachDB's logic tests for `user` and
   `upgrade` (per each project's documentation; not verified against
   source), and `--- restart` is its in-repo precedent. It closes the test gap alone at
   a fraction of the cost. The failing scenario is the Motivation's second
   cost: an agent still switches channels, and the runner would own a
   branch grammar the compiler does not, so one operation would have two
   spellings, one testable and one shippable. The directive route is the
   right answer if the language route is rejected, not beside it.
3. **One door for every statement kind** (deferred). The language is
   already one; the split is transport, kept for four code-visible
   reasons and one routing constraint: separate Cedar actions (`Read`
   versus `Change`, `handlers.rs:1175, 1050`), different targets (reads
   take branch or snapshot, writes branch only), a write-only CAS
   precondition (`/mutate/if-graph-commit`, `lib.rs:1868`), different
   answers (rows versus two counts), and the routing constraint: a proxy
   that dispatches by route path, without reading bodies, must be able to
   send every write to the writer, so write statements keep a mutation
   route path. None of the four code-visible reasons requires two doors:
   one route could dispatch on classification and answer every kind with
   rows, and the routing constraint asks only that write statements keep
   a mutation route path. The cost is a deprecation window for two routes
   and two verbs and a wider RFC. This
   RFC is written for two doors and survives one: every rule above holds
   after the doors merge, and the `outcome` object becomes a row. A
   follow-up RFC may adopt it, citing this one.
4. **A session "current branch" or `checkout` statement.** Dolt's
   `DOLT_CHECKOUT` sets a per-session branch. Rejected: the HTTP API is
   stateless, every route names its branch per request
   (`QueryRequest.branch`, `ChangeRequest.branch`, default `main`), and
   no server, compiler, or user-doc surface has a session or
   working-branch notion; the engine's coordinator-open branch used by
   `branch_create` (`db/omnigraph.rs:3291`) is exposed by no route.
5. **Branch operations inside a mutation body.** Rejected: one mutation
   query publishes as one commit on one branch and a merge is its own
   atomic commit (`docs/user/branching/index.md:65, 68`); a body holding
   both would be two publications or a transaction scope that does not
   exist (`BEGIN`/`ROLLBACK` are not provided, `:61`).
6. **Reserve the keywords** with a negative lookahead on `ident`. Rejected
   in Grammar: it breaks existing property names for nothing.
7. **An `Option<BranchStmt>` field on `QueryFile`** instead of the
   file-level enum. Rejected in AST: a consumer can ignore a field.
8. **A dedicated `POST /branch` route for the statement family**, taking
   `{"query": "branch …"}` and classifying per kind for Cedar as this RFC
   does. Its honest accounting: it removes the wrong-door refusals, the
   request-target, name, and params refusals,
   the deprecated-route refusal, the `outcome` field with its zero counts,
   the parse-before-authorize reorder at both doors, and the
   `ReadDispatch`/`Door` surgery, at the cost of one route, one client
   method, and the CLI's `-e` verbs dispatching to it after a local parse.
   Rejected: a third door contradicts the one-language goal in Motivation
   (a script would again pick a transport per operation kind, now by
   statement family instead of by route), it adds a route the CLI and
   every SDK must learn, and classification is already how the read door
   keeps writes out; the refusals it removes are the cost of keeping two
   doors, paid once in the shared functions.
9. **A branch-scoped `branch list`**, listing only the branches an actor
   holds `read` on. Not done here: `GET /branches` lists every name under
   one scope-free `read` today, and the statement inherits that rule (User
   and operational behavior); a scoped listing is a route change to make
   on both fronts at once. A richer listing is reserved as an argument
   form, `branch list verbose`, so that the one-column `name` rows this
   RFC pins keep their shape when more columns are wanted. A dry-run
   merge, if it is ever added, is its own control-read statement form
   answering 200 with conflict rows, distinct from the real merge of
   Alternatives 10.
10. **A merge conflict as an `outcome`**, `kind: "conflict"` with the
    conflict list in a 200 `ChangeOutput`, so that a `.gqt` case could
    write `expect outcome: conflict`. Rejected: the 409 already carries
    the structured list (`ErrorOutput.merge_conflicts`, documented in
    `docs/user/branching/merge.md:38-53`), so an agent reads it as data
    today; a 200 for a write that published nothing would split the
    shared error mapping (`OmniError::MergeConflicts` to 409 in one place,
    `omnigraph-server/src/lib.rs:1081`) into two, against the guarantee
    that a statement and its route map errors identically; and every
    `/mutate` consumer treats 200 as applied.

Precedent audit. In-repo: the read door's classification-by-body refusal
(`handlers.rs:1126-1131`) is the nearest pattern, extended here to the
file level and both doors; the one divergence, parsing before target
resolution, is justified in Server dispatch and owned in Compatibility.
RFC 0041's inline queries put any GQ source in `QueryRequest.query`, so
the request types need no change; RFC 0042 makes
the branch name the only public identity (`0042:21-23`); RFC 0045's
fail-closed evolution rule carries the amendment. External (each per the
project's documentation; not verified against source): Dolt exposes
versioning as statements inside SQL, `CALL DOLT_BRANCH()`,
`DOLT_CHECKOUT()`, `DOLT_MERGE()`, and makes conflicts queryable through
the `dolt_conflicts` system table; this RFC takes the statement-kind
shape, not the checkout, and the conflicts-as-data lesson is already met
by the 409's list. DuckDB's sqllogictest `restart` and `load` and
CockroachDB's `user` and `upgrade` are the directive shape of
Alternatives 2. Neon and lakeFS expose branching through API, CLI, and
console, not through a statement in the query language, the status quo
shape. The RFC corpus mentions Dolt once, as a merge-by-reference peer
(`0001:389`), and never `DOLT_MERGE`, DuckDB's `restart`, or
CockroachDB's directives.

## Evidence and tests

Existing owners to extend:

- Compiler: a parser test beside `parse_query` (the crate has no `tests/`
  directory at that commit) for each statement, each default, the quoted
  name, the keyword boundary (`branch createb0` refused), the empty,
  whitespace-padded, and control-character name refusals, the two
  compiler refusals, and a
  property named `branch` still parsing inside a body.
- Server: `crates/omnigraph-server/tests/data_routes.rs` for dispatch,
  the refusal table, the 400-before-403 precedence, the CLI's exact
  request shape for each statement, and a conflicting `branch merge`
  answering the same 409 body as `POST /branches/merge`; `auth_policy.rs`
  for the per-kind Cedar decision (allow and deny per action; `branch
  list` under `read`); `openapi.rs` for the `outcome` field and the
  null-null `target` value.
- CLI: `crates/omnigraph-cli/tests/cli_queries.rs` for `-e` statements in
  remote and embedded mode, rendering `branch list` in all five formats
  with the null-null target, the three text lines for control writes, and
  the delete confirmation on a non-local target;
  `parity_matrix.rs` for verb-versus-statement output parity.
- Logic tests: `crates/omnigraph-gqt/tests/gq_logic_tests.rs` plus the
  cases below; refusal tests for the header argument's grammar, the
  wrong-kind step, `branch:` and `--- params` on a statement step,
  `outcome:` on a non-merge step, and `affected:` on a control write.

Engine tests are not extended; merge semantics keep their owners.

First cases: the three merge-family findings, in the first `.gqt` files
that can hold them. Ten cases are committed at that commit
(`git ls-tree 8ca9b12c crates/omnigraph-gqt/cases/`); these are the
eleventh through thirteenth.

1. `issue_583_merge_duplicates_edge_inserted_on_both_sides.gqt`
   ([#583](https://github.com/ModernRelay/omnigraph/issues/583)): fork
   `b0`, insert the same edge on `b0` and on `main`, `branch merge b0`,
   read the edges on `main`; red returned two rows for one edge.
2. `issue_600_second_merge_of_merged_branch.gqt`
   ([#600](https://github.com/ModernRelay/omnigraph/issues/600)): fork,
   write, `branch merge b0`, `branch merge b0` again, read. The engine's
   answer to a re-merge is what the case pins, a scenario no `.gqt` can
   express today; its expect line is blessed from a run (Design, Logic
   tests).
3. The seed-221206 re-adoption (issue not yet filed, `# issue: none`),
   the scenario the harness found on 2026-09-04, in full. The two
   `affected:` lines are illustrative and are blessed from a run when the
   case is written (Design, Logic tests); the two `expect unordered`
   bodies are the claim:

```
# issue: none
# red_on: 2026-09-04, DST nightly run #9, seed 221206, arm window:mutation.post_no_effect_pre_gate: main returned (w6, charlie) after the b0 merge; expected only (bob, w6)
# notes: edge born on main, both forks inherit it, a duplicate add on b0 (zero effect at the set level),
# notes: delete on b1, merge b1 (the delete reaches main), merge b0 (re-adopts the deleted edge).

--- schema
node Person {
    name: String @key
}

edge Knows: Person -> Person

--- seed
{"type":"Person","data":{"name":"w6"}}
{"type":"Person","data":{"name":"charlie"}}
{"type":"Person","data":{"name":"bob"}}
{"edge":"Knows","from":"w6","to":"charlie"}
{"edge":"Knows","from":"bob","to":"w6"}

--- mutate
branch create b0

--- expect ok

--- mutate
branch create b1

--- expect ok

--- query
branch list

--- expect unordered
{"name": "b0"}
{"name": "b1"}
{"name": "main"}

--- mutate branch: b0
query duplicate_add() {
    insert Knows { from: "w6", to: "charlie" }
}

--- expect affected: nodes=0 edges=0

--- mutate branch: b1
query delete_outgoing_from_w6() {
    delete Knows where from = "w6"
}

--- expect affected: nodes=0 edges=1

--- mutate
branch merge b1

--- expect ok

--- query
query edges_on_main_after_b1() {
    match {
        $a: Person
        $a knows $b
    }
    return { $a.name, $b.name }
}

--- expect unordered
{"a.name": "bob", "b.name": "w6"}

--- mutate
branch merge b0

--- expect ok

--- query
query edges_on_main_after_b0() {
    match {
        $a: Person
        $a knows $b
    }
    return { $a.name, $b.name }
}

--- expect unordered
{"a.name": "bob", "b.name": "w6"}
```

The two merges take `expect ok` rather than `outcome:` because the outcome
word is not the case's subject; the final row set is. `ok` on a merge
fails if the merge conflicts, so a conflicting merge is blamed at the
merge step, never at the read after it. Acceptance for the amendment: all
three cases red where a fix has not landed and green after, run by the
existing walker.

## Rollout

1. **Compiler** (`omnigraph-compiler`, `omnigraph-cluster`): grammar,
   `BranchStmt`, the `QueryFile` enum, the two parse refusals, the three
   name refusals, the refusing `Branch` arms in typecheck, lint, lower,
   and the registry, and the cluster loader's two `.queries` sites
   (`config.rs:158` and `validate_query_source`), whose `Branch` arm is a
   diagnostic in the `query_parse_error` family, so `omnigraph cluster
   validate` refuses a catalog the server would refuse at boot. Ships
   alone: nothing accepts
   a statement yet, every existing `.gq` file parses as before.
   `implementation` advances to `in-progress`.
2. **Server and wire** (`omnigraph-server`, `omnigraph-api-types`):
   parse-first ordering in `run_query` and `run_mutate` (`classify`,
   `ReadDispatch`, `Door`), the wrong-door, request-target, name-and-params,
   precondition, and deprecated-route refusals, the handler-body split,
   `BranchOutcomeOutput` and the `outcome` field, the `commit` fill for a
   merge. Same change: `openapi.json` regenerated, `docs/user/branching/index.md`
   and `merge.md` amended with the statement spellings (`AGENTS.md:146,
   169` require both in the change that adds the endpoint or format).
   Ships alone: statements work over HTTP; the CLI's `-e` verbs reach them
   in phase 4.
3. **Logic tests** (`omnigraph-gqt`, RFC 0045 amendment): the `branch:`
   step argument, the `outcome:` expect mode, the statement-step
   refusals, the embedded dispatch with the reclaim join, the three
   cases, and the amendment sentences in RFC 0045's File format and
   Execution semantics with a Decision log entry pointing here. Ships
   alone.
4. **CLI** (`omnigraph-cli`): both verbs classify before sending; the
   remote arm's statement requests (no target, `POST /mutate`), the local
   refusals, the embedded arm's dispatch, the shared text renderer for a
   `ChangeOutput` carrying `outcome`, and the `data_routes.rs` request
   shape test, and the `confirm_destructive` guard on `BranchStmt::Delete`
   with its `cli_queries.rs` case. Same change: `docs/user/cli/reference.md`
   and `cli/index.md` amended, and the agent-facing surface,
   `skills/omnigraph/SKILL.md`, `skills/omnigraph/references/data.md`,
   and `skills/omnigraph/references/commands.md`, gains the statement
   spelling beside the verb spelling with the wrong-door rule. The
   `omnigraph branch` verbs are
   untouched.
   Ships alone. `implementation` advances to `complete`.

The RFC PR adds this file as `docs/rfcs/0052-gq-branch-statements.md`, its
registry row, and the next-number bump to `0053` in the same PR, with
`scripts/check-docs.py` green.

## Unresolved questions

None.

## Decision log

- 2026-09-04: initial draft.
- 2026-09-04: a `branch merge` conflict stays the route's HTTP 409, not a
  200 `outcome` (Alternatives 10).
