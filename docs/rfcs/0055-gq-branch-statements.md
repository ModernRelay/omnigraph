---
rfc: "0055"
title: "Branch statements in GQ"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-04
updated: 2026-09-06
discussion: https://github.com/ModernRelay/omnigraph/pull/626
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0055: Branch statements in GQ

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

> The number is provisional: `0055` is the next available number at the
> upstream commit this document is anchored to, and the file, the heading,
> the frontmatter, and the registry row are renumbered together before the
> PR merges if another RFC has taken it by then.

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
findings are its first cases, one in the corpus and two held for their
fixes (Evidence and tests).

The boundary that does not change: the four `/branches` routes stay and
keep their handlers and response types; the engine crate is untouched;
`QueryRequest` and `ChangeRequest` keep their fields; there is no session,
current branch, or checkout, since every request keeps naming its target;
a mutation body still publishes as one commit on one branch; the
`omnigraph branch` verb tree stays on its routes and shares only its
output text and its delete confirmation with the statement path. This RFC
is the branch family of a wider direction under which every operation a
graph exposes becomes a GQ statement (Motivation): the one-language RFC
makes the rules set here for a control read and a control write its
general contract, applies them to load, schema, snapshot, commit history,
the change feed, blob inspection, and maintenance, one statement family
each, through the same two doors, and delegates the branch family to this
document; neither document waits on the other. The one-door question
stays deferred (Alternatives 3).

## Motivation

Branch create, delete, merge, and list exist, at upstream commit `d520d2bb`
(every `file:line` anchor in this document is at that commit), as four
HTTP routes, `GET` and `POST /branches`, `DELETE /branches/{branch}`,
`POST /branches/merge` (`crates/omnigraph-server/src/lib.rs:1919-1924`),
and as the CLI verbs wrapping them
(`crates/omnigraph-cli/src/main.rs:440-561`). GQ, the
language every read and every mutation is written in, has no statement for
any of them: the grammar's only top-level form is `query_decl`
(`crates/omnigraph-compiler/src/query/query.pest:8`). Three costs follow.

First, no logic test can pin a merge bug. RFC 0045's runner knows three
step kinds, `Query`, `Mutate`, `Restart` (`crates/omnigraph-gqt/src/lib.rs:79-83`),
grouped by `Item::Loop` (`lib.rs:69-76`),
and runs every query and mutate step against `main`
(`lib.rs:1412, 1511`). The merge family found by the deterministic
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
(`crates/omnigraph-cli/src/cli.rs:701-747`), so a script
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

The branch family is one family of a wider direction, not the whole of it.
The direction is one language for every operation a graph exposes: load,
schema, snapshot, commit history, the change feed, blob inspection, and
maintenance are today each an HTTP route and a CLI verb, or a CLI verb
alone (`optimize`, `rebuild-full-text-indexes`, `repair`, and `cleanup`
are direct-storage verbs with no route,
`crates/omnigraph-cli/src/cli.rs:20-21`), so an agent leaves GQ for every
one of them and cannot reach the maintenance four at all. The one-language
RFC applies the rules this RFC sets for a control read and a control write
to those operations, one statement family each, and delegates the branch
family to this document, adding nothing to it; neither document waits on
the other. Branches have a document of their own because they are the only
operations an agent performs in the middle of its work, between its reads
and its writes, and because they carry the test gap above. SQL took the same
route: `VACUUM`, `REINDEX`, and `CREATE TABLE` are statements in the
language, and Dolt mirrors every CLI command as a `dolt_*` procedure,
`DOLT_GC()` included (per its documentation; not verified against source).

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
published (`crates/omnigraph/src/exec/merge.rs:5264-5266, 5407-5409`), so
the target's head and the source are unchanged.

Guarantee: a statement and its route produce the same engine effect, the
same Cedar decision, the same admission check, and the same error mapping
for every input the compiler accepts, because they run one handler body
(Design); a padded or control-character name is refused at the statement
and trimmed, or passed, by the route (Grammar), the one input class where
the fronts differ. They differ
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
`cli.rs:113-123, 146-156`):

```
omnigraph mutate -e 'branch create b0'
omnigraph mutate -e 'branch create "review/add-benchmark"'
omnigraph mutate -e 'branch merge b0 into main'
omnigraph query  -e 'branch list' --format table
```

A name outside the identifier alphabet (a lowercase letter or `_`, then
letters, digits, or `_`), which includes every name with a `/`, a `-`, a
`.`, a leading uppercase letter, or a leading digit
(`review/add-benchmark`, `release.1.2`, `B0`), is quoted; `b0`, `bX`,
`_x`, and `main` are bare (Design, Grammar). Both verbs classify
the source before sending: a statement is posted with no request target,
and `--branch`, `--snapshot`, `--if-commit`, a positional `name`,
`--params`, or `--params-file` beside one fails locally
with the server's message (Design, CLI). `omnigraph query` renders the list
in every text format unchanged (five of its six, `read_format.rs:13-19`;
`--format arrow` is RFC 0051's IPC path), since the
answer is an ordinary `ReadOutput`. Without `--json`, `omnigraph mutate`
prints one line per control write: `created branch b0 from main`, `deleted
branch b0`, `merged b0 into main: fast_forward`; a merge conflict is the
409 the client already turns into an error, exit code 1 with the server's
`error` text (`helpers.rs:562-573`), as every non-2xx answer is today. The
`omnigraph branch create|list|delete|merge` verbs stay on their routes
(`GraphClient::branch_create_from`, `branch_delete`, `branch_merge`,
`branch_list`) and keep their output text (`created branch <name> from
<parent>`, `deleted branch <name>`, `merged <source> into <target>:
<outcome>`, `main.rs:463, 502, 550-555`), their `--delete-branch`
composition (`POST /branches/merge` with `delete_branch: true`; the
statement path has no composition), and their `confirm_destructive` prompt
on delete (`main.rs:496`); the statement path shares their text renderer
and the delete confirmation. Deprecating the verb tree is not part of this
RFC.

Refusals. Each is an HTTP 400 on the server and a non-zero exit with the
same message in the CLI; the first six name the statement and the door to
use, in the
shape of the existing `query '{}' contains mutations (insert/update/delete);
use POST /mutate for write queries`
(`crates/omnigraph-server/src/handlers.rs:1156-1161`):

| Rule | Refusal |
|---|---|
| a control write at the read door | `statement 'branch merge' is a control write; use POST /mutate` |
| `branch list` at the write door | `statement 'branch list' is a read; use POST /query` |
| `QueryRequest.branch` or `.snapshot`, or `ChangeRequest.branch`, set alongside a branch statement | `a branch statement names its branches itself; drop the request target` |
| `QueryRequest.name` or `.params`, or `ChangeRequest.name` or `.params`, set alongside a branch statement | `a branch statement takes no name and no parameters` |
| the `Omnigraph-If-Graph-Commit` header on `POST /mutate/if-graph-commit` (CLI `--if-commit`) alongside a branch statement (on `POST /mutate` the header is refused before any parse, `handlers.rs:1310`) | `a branch statement takes no commit precondition` |
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
| send a control write to `POST /read`, which does not reject mutations (`handlers.rs:614`) | deprecated routes refuse every branch statement before authorization |
| steer a merge's target, or make it conditional, through the request envelope (`ChangeRequest.branch`, `Omnigraph-If-Graph-Commit`) | the request-target and precondition refusals above: two sources for one fact are never reconciled (on `POST /mutate` the header is refused before any parse, `handlers.rs:1310`) |
| store a statement in the stored-query catalog and invoke it through `POST /queries/{name}` or `omnigraph mutate <name>` | refused at `cluster validate` and at server boot: a catalog file is a list of `query` declarations, so the cluster loader refuses a `QueryFile::Branch` with a diagnostic in the `query_parse_error` family (`omnigraph-cluster/src/config.rs:150`) and `QueryRegistry::from_specs` refuses it at load (Design, AST) |
| run the statement through the embedded CLI (`--store`) to skip server Cedar | nothing new to skip: in embedded mode no Cedar runs for any statement kind today. Every embedded arm opens the handle bare, `Omnigraph::open(uri)` (directly or through `open_embedded`, `client.rs:321-323`), and `Omnigraph::open` installs no policy (`crates/omnigraph/src/db/omnigraph.rs:796`; only `with_policy`, `:865`, does, and the CLI never calls it), so `enforce` returns `Ok` when no policy is configured (`:929-931`); the verbs' embedded arms (`client.rs:1009-1095`) have the same property. `--store` is the operator's own machine and credentials; a statement there carries exactly the policy a verb carries, none. This RFC records the existing embedded rule and does not change it |
| put two statements in one request to get two forks in one call | the grammar: one statement per file; two statements are two requests, as they are two routes today |

| Honest route | Accepted |
|---|---|
| `POST /mutate` with `{"query": "branch create b0"}` and no `branch` field; `POST /query` with `{"query": "branch list"}` and no target | yes, the canonical doors |
| `POST /branches`, `DELETE /branches/{branch}`, `POST /branches/merge`, `GET /branches` | yes, unchanged |
| `omnigraph mutate -e '…'` or `--query <file>`, `omnigraph query -e 'branch list'`, with or without `--store`; `omnigraph branch …` on its routes | yes |
| a `.gqt` `--- mutate` step holding a control write, a `--- query` step holding `branch list` | yes (Design, Logic tests) |

Operationally nothing is new: a control write is admission-gated per actor
exactly as its route is (`state.workload.try_admit(&actor_arc, 256)` after
Cedar, `handlers.rs:2181, 2263, 2336`), `branch list` is not, and no
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
literal inside a rule, `ident` is a lowercase letter or `_` followed by
letters, digits, or `_` (`query.pest:111`), and the only exclusion
anywhere is `edge_ident`'s
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
carrying a control character (Unicode category Cc, `char::is_control`), so
that the
spelled name and the name the engine acts on are one string; every other
rule is the engine's (`normalize_branch_name` trims and refuses empty,
`ensure_logical_branch_name` refuses an incarnation-shaped segment, and
`ensure_branch_create_namespace_safe` refuses an existing name and an
ancestor or descendant of a live name, `crates/omnigraph/src/db/omnigraph.rs:3787-3799,
2982-3003`, `crates/omnigraph/src/branch_names.rs:65-75`), and
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchStmt {
    Write(BranchWrite),
    List,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchWrite {
    Create { name: String, from: Option<String> },
    Delete { name: String },
    Merge { source: String, into: Option<String> },
}

impl BranchWrite {
    pub fn statement_name(&self) -> &'static str  // "branch create" through "branch merge"
}

impl BranchStmt {
    pub fn is_write(&self) -> bool  // false only for List
    pub fn statement_name(&self) -> &'static str  // "branch create" through "branch list"
    pub fn not_a_declaration_message(&self) -> String
    // "`branch create` is a branch statement, not a query declaration"
}
```

The control writes are one sub-type so a consumer that serves only them
(`run_branch_statement`, `GraphClient::branch_write_statement`, the
runner's control step) takes a `BranchWrite` and has no `List` arm to
refuse or mark unreachable.

`not_a_declaration_message` is the one refusal text every consumer whose
arm is a refusal prints, so it is wire-visible wherever that arm is
reached; `statement_name` is the two words it names.

A file-level enum rather than an added `Option<BranchStmt>` field, because
a consumer can ignore an `Option` and cannot ignore a variant: every
`.queries` consumer becomes a `match` and the compiler enumerates them. At
that commit they are `find_named_query`
(`omnigraph-compiler/src/query_input.rs:262`), the one seam through which
the engine parses (`crates/omnigraph/src/exec/query.rs:66, 118`,
`exec/mutation.rs:1095`); the server's `run_query` and `run_mutate`
(`handlers.rs:1135, 1062`; `select_named_query_decl`, `handlers.rs:2512`,
takes the classified `Vec<QueryDecl>` after this RFC and never parses, so
the server's `Branch` arm lives in the two door functions only);
the CLI's `select_named_query` (`omnigraph-cli/src/helpers.rs:846`); the
runner's `file.queries.as_slice()` (`omnigraph-gqt/src/lib.rs:796`); the
cluster stored-query loader (`omnigraph-cluster/src/config.rs:158`) and
its per-query check `validate_query_source` (`config.rs:1293`);
`lint_query_file` (`query/lint.rs:129`); the stored-query registry's
parse site `QueryRegistry::from_specs` (`omnigraph-server/src/queries.rs:103`).
For the linter, the registry, and the cluster loader the new arm is a
refusal (a branch statement has no plan and is never stored), lint's under
its parse code `Q000` and the cluster loader's as `query_parse_error`;
`typecheck_query_decl`, `typecheck_query` (`typecheck.rs:98, 110`) and
`lower_query` (`ir/lower.rs:34`) take a `QueryDecl`, never see the file,
and need none. For `find_named_query` it is a refusal
too, which is why the engine crate stays untouched: the engine reaches a
declaration only through `find_named_query`, and a `Branch` file has none
to return. For the runner and the server the arm is the dispatch below;
for the CLI the dispatch sits in both verbs before `select_named_query` is
reached (Design, CLI), and that helper's own `Branch` arm stays a refusal
no verb reaches. Test files reach declarations through a `QueryFile::single_decl()`
helper, so the `.queries[0]` sites in `parser_tests.rs`,
`typecheck_tests.rs`, `lower_tests.rs`, and `omnigraph-gqt/src/tests.rs`
are one edit. `single_decl` is test support: it is `#[track_caller]` and
panics unless the file holds exactly one declaration, and no production
code calls it. `parse_query_decl`
(`parser.rs:47`) is untouched; a sibling `parse_branch_stmt` fills
`BranchStmt`.

The compiler classifies and never refuses a door: the read door's refusal
of a mutation is the server's (`run_query`, `handlers.rs:1156-1161`), and
the same holds here. Beside the three name refusals and the escape refusal
in Grammar, the compiler owns two structural refusals, both parse
errors: a branch statement beside a `query` declaration, and a branch
statement inside a mutation body (no `mutation_stmt` alternative exists).

### Server dispatch

Both doors parse, classify, refuse the wrong kind, then authorize per kind:

1. `run_query` (`handlers.rs:1135`) and `run_mutate` (`handlers.rs:1062`)
   gain a first step, `classify(query) -> QueryFile`, which is
   `parse_query`, before any target resolution. Today `run_query`
   resolves and Cedar-authorizes the read target
   (`resolve_authorized_read_target`, action `Read` on the request's
   branch, default `main`) before parsing (`handlers.rs:1153` then
   `:1154`), and today `run_mutate` authorizes `Change` and admits before
   parsing (`handlers.rs:1076, 1091, 1096`); after this RFC the parse
   comes first at both doors, so a branch statement never pays a `Read`
   check on `main` on top of its own action. Likewise `run_mutate`'s own
   `Change` check and `try_admit` (`handlers.rs:1076-1094`) are skipped
   for a `Branch` file; the handler body's are the only ones. For
   `QueryFile::Queries` the remaining steps keep today's order, with one
   visible consequence owned in Compatibility: a parse error (400) now
   precedes the `Read` denial (403) on `/query` and `/read`, and precedes
   the `Change` denial (403) and the admission check on `/mutate`,
   `/change`, and `/mutate/if-graph-commit`.
2. `QueryFile::Branch(BranchStmt::Write(_))` at `run_query`, or
   `QueryFile::Branch(BranchStmt::List)` at `run_mutate`, is the wrong-door
   400. Then the
   request checks: `QueryRequest.branch`, `.snapshot`, `.name`, or
   `.params`, `ChangeRequest.branch`, `.name`, or `.params`, or an
   expected head present alongside a branch statement is a 400. To make
   these checks possible inside the shared functions rather than in each
   axum shell: `run_mutate` takes `branch: Option<String>` (today every
   caller defaults it to `main` before the call, `handlers.rs:1251, 1311,
   1336, 1541`),
   `run_query` returns `ReadDispatch::{Rows(String, ReadTarget,
   QueryResult, Option<String>), BranchList(Vec<String>)}` in place of
   today's tuple (`handlers.rs:1144-1151`), and the `reject_mutations:
   bool` parameter (`/read` passes the literal `false`, `handlers.rs:614`)
   becomes `door: Door::{Query, Read, Mutate, Change}`, on which the
   refusal table keys: `Read` and `Change` refuse every branch statement,
   `Query` refuses a control write, `Mutate` refuses `branch list`
   (`/mutate/if-graph-commit` is `Mutate` with an expected head,
   `handlers.rs:1365`). `/queries/{name}` (`handlers.rs:1569, 1587`)
   passes `Mutate` and `Query`; its source is registry-owned and never a
   `Branch` file (AST), so neither refusal fires there. The
   request types do not change: `QueryRequest { query, name, params,
   branch, snapshot }` (`crates/omnigraph-api-types/src/lib.rs:665-683`)
   and `ChangeRequest` (`:818`) already carry any GQ source string.
3. Each `server_branch_*` handler splits into its axum shell (extractors;
   the shell keeps the `server_branch_*` name, since its `#[utoipa::path]`
   attribute and its `ApiDoc` registration stay on it) and a body function
   `branch_*_body` that both the route and the statement path
   (`run_branch_statement`) call, so the `PolicyRequest`, the admission
   check, the engine call, and the error mapping are one piece of code:

| Statement | Handler body | Cedar `PolicyRequest` (`handlers.rs`) | Engine call |
|---|---|---|---|
| `branch create` | `branch_create_body` (shell `server_branch_create`) | `BranchCreate`, `branch: Some(from)`, `target_branch: Some(name)` (`:2171-2173`) | `db.branch_create_from_as(ReadTarget::branch(&from), &name, actor)` (`:2185`; `db/omnigraph.rs:3387`) |
| `branch delete` | `branch_delete_body` (shell `server_branch_delete`) | `BranchDelete`, `branch: None`, `target_branch: Some(name)` (`:2255-2257`) | `db.branch_delete_as(&name, actor_id)` (`:2267`; `db/omnigraph.rs:3501`) |
| `branch merge` | `branch_merge_body` (shell `server_branch_merge`) | `BranchMerge`, `branch: Some(source)`, `target_branch: Some(target)` (`:2326-2328`) | `db.branch_merge_as(&source, &target, actor_id)` (`:2340`; `crates/omnigraph/src/exec/merge.rs:4893-4919`) |
| `branch list` | `branch_list_body` (shell `server_branch_list`) | `Read`, `branch: None`, `target_branch: None` (`:2121-2123`) | `db.branch_list()` (`:2128`; `db/omnigraph.rs:3481`), then `sort()` (`:2130`) |

The three write actions are the only branch actions Cedar has
(`PolicyAction`, `crates/omnigraph-policy/src/lib.rs:18-74`; schema lines
`:849-851`); `branch list` is authorized as `read` with no scope, exactly
as its route is, not as a branch action. Since RFC 0053, `authorize`
applies the credential's own action ceiling, and for a signed data
credential the applied-policy requirement, before Cedar
(`handlers.rs:407-418`), and `resolve_graph_handle` refuses a graph the
credential does not select (`:335-339`); a statement reaches both through
the same `PolicyRequest` and the same extractor as its route, so a
credential that cannot reach a route cannot reach its statement. The
`delete_branch` composition
of `POST /branches/merge` (a second `BranchDelete` check, `:2382`) has no
statement clause: an author writes `branch merge b0` then `branch delete
b0`, two statements, two checks, which is what the route does internally.
The route's admission slot moves with the body: `branch_merge_body` holds
the `try_admit` guard through the engine call and drops it on return, so
`server_branch_merge`'s `delete_merged_source_branch` tail runs after the
slot is released, where before the split the route held it through the
deletion. The deletion never admitted on its own, so the one observable
change is the actor's in-flight count during that tail.

4. The answer. `branch list` fills `ReadOutput { query_name, target,
   row_count, columns, rows, graph_commit_id }`
   (`api-types lib.rs:285-300`) with `query_name: "branch list"`,
   `target: { branch: null, snapshot: null }` (the statement reads the
   ref list, not a branch, matching its scope-free Cedar request; no
   route emits this value today, since `read_target_from_request` always
   fills one of the two, `handlers.rs:2501-2510`, so Compatibility names
   it), `columns: ["name"]`, `rows` = one `{"name": "<branch>"}` per
   branch sorted by `name` in byte order (`Vec<String>::sort`,
   `handlers.rs:2130`), `row_count = rows.len()`, and `graph_commit_id`
   absent (`skip_serializing_if`, `api-types lib.rs:298`). The three
   control writes fill `ChangeOutput { branch,
   query_name, affected_nodes, affected_edges, actor_id, commit }`
   (`api-types lib.rs:329-336`) with `branch` = the branch that received
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
lib.rs:293-297`). For `fast_forward`
and `merged` it is the target's head after the merge, filled by the
handler body: the engine's `MergeOutcome` carries no commit id
(`AlreadyUpToDate | FastForward | Merged`, `db/omnigraph.rs:65-70`), so
the body reads `db.list_commits(Some(target))`, whose first entry is the
newest by that function's contract (`db/omnigraph.rs:3599-3606`), and
renders it through `api::commit_output` as the `CommitOutput` a mutation
body's answer already carries (`api-types lib.rs:387-397, 1359`). That
read is a `list_commits` walk of the target's whole history, O(history),
which is the merge's own complexity class, so the fill costs the answer
nothing the merge did not already cost. The
merge holds both branch gates through publication
(`exec/merge.rs:4957-4960`) and the head read runs after they are
released, so under a concurrent writer on the target the id can name a
later commit; and the read is non-fatal, since the merge is already
durable when it runs: a failed read answers `commit: null` and the merge
stands, never an error for a write that happened. Both the walk and the
race are retired by the engine follow-up (`MergeOutcome` carrying the
id), not by this RFC. The engine crate stays untouched.

`BranchOutcomeOutput` is a tagged object, `kind` in `snake_case`, its
fields taken from the route outputs that exist today
(`BranchCreateOutput`, `BranchDeleteOutput`, `BranchMergeOutput`,
`api-types lib.rs:137-154, 199-214`):

| `kind` | Fields | Source of each field |
|---|---|---|
| `created` | `from`, `name` | `BranchCreateOutput` minus `actor_id`, which `ChangeOutput` already carries, and minus `uri`, the graph URI the caller already addressed |
| `deleted` | `name` | `BranchDeleteOutput` minus `actor_id` and `uri` |
| `merged` | `source`, `target`, `merge` | `BranchMergeOutput`; `merge` is `BranchMergeOutcome`, wire strings `already_up_to_date`, `fast_forward`, `merged` (`api-types lib.rs:188-195`), under the key `merge` so that `merged` the kind (a merge that completed) and `merged` the three-way result never share a key |

A merge conflict has no `kind`: it is the route's error. `branch_merge_as`
returns `Err(OmniError::MergeConflicts)` (`crates/omnigraph/src/error.rs:172-173`),
the shared body maps it through `ApiError::from_omni` to
`ApiError::merge_conflict` (`omnigraph-server/src/lib.rs:1105-1113,
930-937`), status 409, `ErrorOutput { error, code: "conflict",
merge_conflicts: [MergeConflictOutput] }` (`api-types lib.rs:1273-1278`),
with `error` beginning `merge conflicts: ` (`summarize_merge_conflicts`,
`lib.rs:1272`). `MergeConflictOutput { entity_kind, type_name, entity_id,
kind, message }` (`api-types lib.rs:257-263`) and its seven kinds
(`:218-240`) are untouched.

Guarantee: a mutation body's `ChangeOutput` is byte-identical before and
after this RFC, because `outcome` is skipped when absent; a `branch list`
answer is a well-formed `ReadOutput` for every existing consumer of that
type, including the five text renderers; a conflicting `branch merge` and a
conflicting `POST /branches/merge` answer the same status, the same
`ErrorOutput`, and the same `merge_conflicts` list for the same input.

The one mechanism this design adds is "the compiler's classification picks
the door". Remove it and either every statement needs its own transport,
the four routes that exist today (Alternatives 1) or a fifth route for the
statement family (Alternatives 8), or one door accepts every kind
unclassified, which puts a write behind the read door's `Read` check.
Classification is already how the read door keeps writes out
(`handlers.rs:1156`); this RFC makes that rule file-level and applies it
symmetrically at the write door.

### CLI

The remote arms of both verbs change, because today both always send a
request target and `mutate` posts to a deprecated route. `Command::Query`
builds a `ReadTarget` from `--branch`/`--snapshot`, default `main`
(`resolve_read_target`, `main.rs:1180`), and `GraphClient::query`
serializes it as `branch` or `snapshot` on every `QueryRequest`
(`client.rs:895-909`). `Command::Mutate` resolves `branch` to `main`
(`resolve_branch(branch, None, "main")`, `main.rs:1232`;
`main_tests.rs:59-69` pins that the legacy body always carries `branch`),
and `GraphClient::mutate`
posts to `POST /mutate/if-graph-commit` when `--if-commit` is given and
otherwise to the deprecated `POST /change` with
`legacy_change_request_body` (`client.rs:813-828`, `helpers.rs:1114-1130`).
The wire cannot tell a defaulted target from an explicit one, and the
refusal table refuses both the target and the deprecated route, so without
a CLI change every statement the CLI sends is refused. Both verbs therefore
classify before either arm runs: `main.rs` parses the source with
`parse_query` (a declaration source on the embedded arm is then parsed a
second time by `select_named_query`, `helpers.rs:846-864`, whose `Branch`
arm stays a refusal no verb reaches) and dispatch on the classification
alone: only `QueryFile::Branch` takes the statement path, while
`QueryFile::Queries` and a source the CLI's compiler cannot parse both
take today's path unchanged. For a statement,
`GraphClient::branch_list_statement` posts `QueryRequest` with
`branch: None, snapshot: None, name: None, params: None` to `POST /query`,
and `GraphClient::branch_write_statement` posts `ChangeRequest` with
`branch: None` to `POST /mutate`, never to `/change`; `GraphClient::query`
and `GraphClient::mutate` keep today's requests for every other source.
Neither request struct carries `skip_serializing_if` on these fields, so
a statement's `branch`, `snapshot`, `name`, and `params` travel as
explicit `null`s rather than absent keys, and the server reads absent and
`null` alike as absent; adding `skip_serializing_if` would change a
declaration request's bytes, which the byte-identity guarantee forbids.
An explicit `--branch`, `--snapshot`,
`--if-commit`, a positional query `name`, `--params`, or `--params-file`
(`ParamsArgs`, `cli.rs:991-994`) beside a statement fails locally with the
server's message, before any round trip. A `BranchWrite::Delete` runs
`confirm_destructive("branch delete", …)` on both arms
(`main.rs:496`; `helpers.rs:52-61` refuses a non-local target without
`--yes` or a TTY answer), the same consent step the `omnigraph branch
delete` verb takes,
so the statement path cannot delete on a remote server without it.
Mutation bodies keep
today's requests byte for byte, including the `/change` path. The exact
request body each statement sends is pinned in `main_tests.rs` beside the
legacy `/change` pin, through `branch_statement_query_request` and
`branch_statement_change_request` (`helpers.rs`); the path, `/mutate` and
never `/change`, is proved by the remote round trip in `cli_queries.rs`,
since the server refuses a statement on `/change` with the
deprecated-route text; the server's acceptance of the bare-source shape
is pinned in `data_routes.rs` (Evidence and tests).

The embedded arm (`--store`) must classify locally, because it never meets
the server. After `parse_query`, a `QueryFile::Branch` dispatches to the
engine calls the embedded `BranchCommand` arms make today:
`branch_create_from_as` (`client.rs:1012`), `branch_delete_as` then
`wait_for_fork_reclaims` (`:1044-1047`), `branch_merge_as` (`:1086`),
`branch_list` (`:344`). No Cedar runs in this arm for any statement kind,
because none runs there for anything today: every embedded arm opens the
handle bare, `Omnigraph::open(uri)` (directly or through `open_embedded`,
`client.rs:321-323`), and `Omnigraph::open` installs no policy
(`db/omnigraph.rs:796`; only `with_policy`, `:865`, does, and the CLI
never calls it), so `enforce` returns `Ok` with no policy configured
(`db/omnigraph.rs:929-931`). The existing embedded rule holds unchanged. A
conflicting merge in this arm is `Err(OmniError::MergeConflicts)` from the
engine, rendered as the CLI renders any engine error, message beginning
`merge conflicts: ` (`error.rs:172`), exit code 1.

The text renderer is shared. A `ChangeOutput` carrying `outcome` prints
the verb tree's line for that kind (`created branch b0 from main`,
`deleted branch b0`, `merged b0 into main: fast_forward`, `main.rs:463,
502, 550-555`) instead of `print_change_human`'s `changed main via branch
merge: 0 nodes, 0 edges` (`output.rs:882-890`), which would hide the
outcome word; the `actor_id:` line `print_change_human` prints after it
for any `mutate` follows, and no write-target echo precedes it, since
`echo_write_target` belongs to the verbs and `mutate` never echoed. The
`omnigraph branch …` verbs keep calling
`GraphClient::branch_create_from`, `branch_delete`, `branch_merge`, and
`branch_list`, that is, the routes and the embedded engine calls they use
today; they build no GQ source, since a verb that rendered its arguments
into a statement string would be the shape of ad-hoc string generation
the deny-list rejects where a structured form exists
(`docs/dev/invariants.md:108-109`). `read` and `change` stay the visible aliases
of `query` and `mutate` (`cli.rs:107, 140`).

### Logic tests (RFC 0045 amendment)

Two amendments to RFC 0045's File format, both fail-closed under its own
evolution rule (`0045-gq-logic-tests.md:821-824`: "Format evolution is
fail-closed: unknown sections, unknown header keys, and missing required
headers are refusals, never silent skips, so an older harness refuses a
newer logic test rather than mis-running it"). That rule names sections
and header keys, not step arguments or expect modes, so the fail-closed
claim for these two amendments rests on the runner's own refusals: the
`takes no arguments` check (`lib.rs:775-777`) and `parse_expect_header`'s
unknown-mode refusal (`lib.rs:338-377`), and the refusal of a statement
body, `does not parse` on a harness older than this RFC (`lib.rs:790-803`
at that commit).

1. `--- query` and `--- mutate` accept one optional argument, `branch:
   <name>`, the branch the step runs against; absent, `main`, as today
   (`lib.rs:1412, 1511`). The seam is the `rest` of the header line, split
   off at the first space (`lib.rs:758-761`) and refused today with
   `` `--- {kind}` takes no arguments `` (`lib.rs:775-777`). A fifth
   `HEADER_KEYS` entry (`lib.rs:148`: `issue, red_on, notes, traversal`)
   is not the seam: a case header is per case, a branch target is per
   step. The argument follows the shape of `--- expect error:
   <substring>` (a word, a colon, the trimmed remainder); anything else in
   `rest` is refused with `` `--- {kind}` takes no arguments but `branch:
   <name>`, got `{rest}` `` (`parse_step_branch`), which keeps the older
   `takes no arguments` text as its prefix, and `branch:` with no name
   after it with `` `--- {kind} branch:` needs a branch name ``. The name
   is not checked further at parse time: it passes to the engine, whose
   `normalize_branch_name` trims and refuses empty (Grammar).
2. A `--- mutate` step may hold a control write and a `--- query` step may
   hold `branch list`, classified by the compiler. The wrong kind is
   refused beside the existing read/mutation refusals (`lib.rs:803-812`)
   with two new exact strings, since a statement is not a declaration:
   `` a control write under `--- query` is refused; use `--- mutate` `` and
   `` `branch list` under `--- mutate` is refused; use `--- query` ``. A
   step holding a statement refuses `branch: <name>` on its header
   (`` a branch statement names its branches itself; drop the `branch:`
   argument ``) and a following `---
   params` (`a branch statement takes no params`; today `--- params`
   attaches to any pending step, `lib.rs:828-853`); the step's name, used
   in labels, is the statement's two words. `branch create` and `branch
   delete` take `ok` or `error: <substring>`. `branch merge` takes `ok`,
   `error: <substring>`, or the new mode `outcome: <word>`, body empty,
   `<word>` one of `already_up_to_date`, `fast_forward`, `merged`,
   asserting the engine's `MergeOutcome`, which the runner spells with the
   wire words itself (`merge_outcome_word`), since it holds the embedded
   handle and never a `ChangeOutput`; `outcome:` on any other step is
   refused. A
   conflicting merge is an error, so `ok` fails on it and `error: merge
   conflicts` pins it (the substring is the start of
   `OmniError::MergeConflicts`'s message, `error.rs:172`, which the runner
   sees directly from the embedded handle). `affected:` on a control write
   is refused (no counts exist). `branch list` under `--- query` takes
   `unordered`, `ordered`, or `error:` over rows `{"name": "…"}`; its rows
   are sorted by `name` in byte order, a total order, so `ordered` is
   accepted and the `order`-clause refusal for declarations
   (`ordered_refusal`, `lib.rs:614-617`) does not apply to it. A
   `branch list` rows expect carries the shape section RFC 0045 makes
   mandatory directly after every rows expect (`0045:398-430`;
   `missing_shape`, `lib.rs:736-744`), one line, `name: String`: the
   runner presents the statement's answer as one non-null `Utf8` column
   `name` (an arrow `RecordBatch` it builds itself, so `arrow-array` is a
   dependency of `omnigraph-gqt`, no longer a dev-dependency only), so
   the shape check (`0045:645-663`) holds against it as against
   any rows step, while the computed check against the compiler's
   inferred schema (`0045:664-678`) is skipped for a statement step, which
   has no declaration to infer from. An
   `outcome:` word or `affected:` counts in a new case are blessed from a
   run, never copied from a design document.

The runner executes against the embedded handle (`lib.rs:32`), opened by
`Omnigraph::init` (`lib.rs:1556`) and reopened by `Omnigraph::open` on
`--- restart` (`lib.rs:1593`), neither with a policy, so a statement there
exercises compiler and engine, never the server's Cedar dispatch; the actor
is `None` and `enforce` is a no-op (`db/omnigraph.rs:929-931`). After a
`branch delete` step the runner awaits `wait_for_fork_reclaims` before the
next step, as the CLI does before exit (`client.rs:1044-1047`), because
`branch_delete_as` returns at the manifest flip and reclaims forks in a
background task (`db/omnigraph.rs:3498-3501`): the join is what keeps the
next step, a `--- restart` reopen, a `branch list`, and the tempdir
teardown from overlapping an in-flight fork delete, the
[#542](https://github.com/ModernRelay/omnigraph/issues/542) class.
Dropping the handle at `--- restart` (`lib.rs:1591-1592`) would not
cancel those reclaims: they are `JoinHandle`s the struct holds
(`db/omnigraph.rs:282`) with no `Drop` impl and no `abort` call, so the
drop detaches them rather than stopping them, which is the overlap the
join exists to prevent. A statement step inside a `--- loop` or
`--- foreach` runs literally on every iteration, which is how a repeated
merge is written once: `${i}` substitutes only in params and expect
bodies and query and mutate bodies stay literal (`0045:467-471`); a
statement step has no params, a `branch list` rows expect inside a loop
may carry `${i}` as any expect body may, and `${` in a statement body is
refused either way: unquoted it is `does not parse` from the compiler,
and inside a quoted name (`branch create "${i}"`, which the grammar's
`string_char` admits) it parses and the runner's own substitution fence
refuses the step. `--- restart`
(`0045:438`) remains the one step
that is not GQ; this
amendment adds no directive, since the statements are GQ.

The two format pieces in one fragment, with the shape section every rows
step carries (the outcome word is illustrative and is blessed from a run
when a case is written, per the blessing rule above):

```
--- mutate
branch merge b0 into main

--- expect outcome: fast_forward

--- query
branch list

--- expect unordered
{"name": "b0"}
{"name": "main"}

--- expect shape
name: String
```

Guarantee: a harness older than this RFC refuses a case using either
amendment (the argument with `takes no arguments`, the expect mode as an
unknown mode, a statement body as `does not parse`, `lib.rs:790-795` at
that commit) and never mis-runs it.

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
emits today. `branch list` fills `ReadOutput.rows`, JSON text since
RFC 0051 (`Box<RawValue>`, `api-types lib.rs:292`), as any read does, and
inherits the `Accept` negotiation RFC 0051 schedules for `POST /query`:
under its Arrow `Accept` it answers a one-column `name` IPC stream. The
four `/branches` routes and their output types,
`QueryRequest`, and `ChangeRequest` are unchanged. The deprecated `POST
/read` and `POST /change`, whose envelopes are byte-stable
(`LegacyReadOutput`, `api-types lib.rs:306`), refuse branch statements. A
statement and its
route never diverge in behavior: a merge conflict is the same 409 on both
(Design, step 4).

Error precedence on `/query`, `/read`, `/mutate`, `/change`, and
`/mutate/if-graph-commit` changes for every caller, not only for
statements (`/queries/{name}` shares the functions, but its source is
parsed at load, `queries.rs:108`, so no order change is observable
there): today an actor denied `Read` gets 403 before any
parse (`handlers.rs:1153` then `:1154`); after this RFC a parse error, 400
with the parser's diagnostics, precedes the denial, so an authenticated
actor denied `read` spends parser time on a route with no admission gate
(`run_query` doc, `handlers.rs:1130-1134`); on the write routes the parse
now runs before `try_admit` (`handlers.rs:1091`), so an unparseable body
no longer consumes an admission slot and the parse itself is no longer
admission-bounded. A caller with no credentials never reaches the handler
when a policy is configured (`require_bearer_auth`, `handlers.rs:269-296`,
answers 401 first). Accepted, because the parse reads nothing beyond the
request body, and `/mutate` takes the same reorder: the door's Cedar
action is known only after classification, so both doors parse, classify,
then authorize (today `/mutate` authorizes and admits first,
`handlers.rs:1076-1096`); the alternative, parsing first only when
the source begins with `branch`, would make the door's behavior depend on
a string compare on the request, which Invariants 9 forbids.

CLI. `omnigraph query` and `omnigraph mutate`, given a statement through
`-e`/`--query-string` or `--query <file>`, change their remote requests
for statements only: no `branch`, `snapshot`, `name`, or `params`, and
`POST /mutate` instead of `POST /change`. Every request for
a mutation body or a read query is byte-identical to today's (Design,
CLI). No producer of a message moves, because the classification only
selects the statement path: a source the CLI's compiler cannot parse is
sent verbatim, exactly as today, so this CLI never gates a newer server's
grammar and a broken declaration file still fails with the server's text
after the round trip (`classify` is `ApiError::bad_request` over
`parse_query`'s error and the CLI prints the `error` field). An older CLI
against a newer server therefore refuses locally only what its own
envelope rules refuse, never a statement spelling it has not learned.
The `omnigraph branch` verbs are unchanged.

Clients. Every client of `POST /mutate` becomes a branch-control client
after this RFC: an SDK `mutate` call and an MCP `mutate` tool (RFC 0003)
each carry `branch merge b0 into main` once their server accepts it.
Server-side Cedar is the gate on both fronts, so no actor's permissions
widen. A client that withheld branch operations from its users while
exposing a `mutate` entry point must now classify the source it sends or
accept the change.

Storage: none. Policy: no new action and no policy-file change; a policy
that grants `branch_merge` today grants `branch merge` tomorrow, one
that denies it denies both fronts, and a data credential's action ceiling
(RFC 0053) bounds both fronts alike. Logic tests: fail-closed per RFC 0045.

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
   versus `Change`, `handlers.rs:1205, 1080`), different targets (reads
   take branch or snapshot, writes branch only), a write-only CAS
   precondition (`/mutate/if-graph-commit`, `lib.rs:1892`), different
   answers (rows versus two counts), and the routing constraint: a proxy
   that dispatches by route path, without reading bodies, must be able to
   send every write to the writer, so write statements keep a mutation
   route path. None of the four code-visible reasons requires two doors:
   one route could dispatch on classification and answer every kind with
   rows, and the routing constraint asks only that write statements keep
   a mutation route path. The cost is a deprecation window for two routes
   and two verbs and a wider RFC. This
   RFC is written for two doors and survives one: every rule above holds
   after the doors merge, and the `outcome` object becomes a row. An RFC
   of its own may adopt it, citing this one; the one-language RFC
   (Motivation) keeps the two doors and inherits the routing constraint.
4. **A session "current branch" or `checkout` statement.** Dolt's
   `DOLT_CHECKOUT` sets a per-session branch. Rejected: the HTTP API is
   stateless, every route names its branch per request
   (`QueryRequest.branch`, `ChangeRequest.branch`, default `main`), and
   no server, compiler, or user-doc surface has a session or
   working-branch notion; the engine's coordinator-open branch used by
   `branch_create` (`db/omnigraph.rs:3334`) is exposed by no route.
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
    `omnigraph-server/src/lib.rs:1105`) into two, against the guarantee
    that a statement and its route map errors identically; and every
    `/mutate` consumer treats 200 as applied.
11. **Every operation in this RFC**, the seven families Motivation names
    beside the branch family, so the direction lands as one document.
    Rejected: each family carries its own answer shape, payload form, and
    permission (the maintenance verbs have no route today, so no policy
    check meets them), so one RFC would own seven contracts and the
    smallest of them, the one with the live test gap, would wait on the
    largest. The branch rules are written to be inherited (Summary), and
    the one-language RFC delegates the branch family to this document
    instead of reopening it.

Precedent audit. In-repo: the read door's classification-by-body refusal
(`handlers.rs:1156-1161`) is the nearest pattern, extended here to the
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
  whitespace-padded, and control-character name refusals, the escape
  refusal, the two structural refusals, and a
  property named `branch` still parsing inside a body.
- Server: `crates/omnigraph-server/tests/data_routes.rs` for dispatch,
  the refusal table, the 400-before-403 precedence, the bare-source
  request shape each statement is accepted with, and a conflicting `branch merge`
  answering the same 409 body as `POST /branches/merge`; `auth_policy.rs`
  for the per-kind Cedar decision (allow and deny per action; `branch
  list` under `read`); `openapi.rs` for the `outcome` field and the
  null-null `target` value.
- CLI: `crates/omnigraph-cli/tests/cli_queries.rs` for `-e` statements in
  remote and embedded mode, rendering `branch list` in all text formats
  with the null-null target, the three text lines for control writes, and
  the delete confirmation on a non-local target; `main_tests.rs` for the
  exact request body each statement sends, beside the legacy `/change`
  pin; `parity_matrix.rs` for verb-versus-statement output parity.
- Logic tests: `crates/omnigraph-gqt/tests/gq_logic_tests.rs` plus the
  cases below; refusal tests for the header argument's grammar, the
  wrong-kind step, `branch:` and `--- params` on a statement step,
  `outcome:` on a non-merge step, and `affected:` on a control write.

Engine tests are not extended; merge semantics keep their owners.

First cases: the three merge-family findings, in the first `.gqt` files
that can hold them. Sixteen cases are committed at that commit
(`git ls-tree d520d2bb crates/omnigraph-gqt/cases/`). All three were
written and run against this tree. One is green and ships in this PR as
the seventeenth; the other two are red on the live bugs and are held out
of the corpus until their fixes land, each entering with its fix in that
fix's own PR, by the corpus's landing-order rule (RFC 0045, Decision log,
2026-09-05: no corpus case ships red). The held-out texts are the claim
each fix must turn green.

1. `issue_583_merge_duplicates_edge_inserted_on_both_sides.gqt`
   ([#583](https://github.com/ModernRelay/omnigraph/issues/583)), held
   out, red: fork `b0`, insert the same edge `Knows alice -> bob` on `b0`
   (`affected: nodes=0 edges=1`) and on `main`, `branch merge b0`, read
   the edges on `main` through the bound-edge pattern `$a $e:knows $b`,
   expecting the one row `{"a.name": "alice", "b.name": "bob"}`; red is
   `step 5 (query): row mismatch: expected 1 rows, got 2`. The read must
   bind the edge: the plain traversal `$a knows $b` deduplicates in its
   visited gate and returns one row on the live bug, so it would be green
   on red; only a bound edge, a count, or an aggregate sees the stored
   duplicate.
2. `second_merge_of_merged_branch.gqt` (`# issue: none`), in the corpus,
   green: fork `b0`, insert `bob` on `b0` (`affected: nodes=1
   edges=0`), `branch merge b0` with `expect outcome: fast_forward`,
   `branch merge b0` again with `expect outcome: already_up_to_date`, read
   `main`: `alice`, `bob`. Both outcome words were blessed from a run
   (Design, Logic tests). It carries a feature case's short name, not an
   issue-anchored one, because no engine build was ever red on it: the
   engine answered the re-merge correctly and the red state belonged to
   the DST driver, which re-merged a merged branch and reported the
   engine's `already_up_to_date` as a failure. RFC 0045 anchors a case to
   an issue only for a failure witnessed on an unfixed build (`0045:84-93`),
   so the [#600](https://github.com/ModernRelay/omnigraph/issues/600)
   provenance is a `# notes:` line and the case carries no `# red_on:`.
3. The seed-221206 re-adoption (issue not yet filed, `# issue: none`),
   named `sibling_merge_readopts_deleted_edge.gqt` under the corpus's
   short-name rule, the scenario the harness found on 2026-09-04, in
   full; held out, red, the nightly's finding reproduced: `step 9 (query):
   row mismatch: expected 1 rows, got 2`, the extra row `{"a.name": "w6",
   "b.name": "charlie"}`, with the read after the `b1` merge green. The
   two `affected:` lines were blessed from the run: the duplicate add on
   `b0` counts `nodes=0 edges=1`, because the engine mints a row per edge
   insert (edges carry no logical key, the mechanism the #583 body names),
   so the add is a no-op at the set level and one row at the table level.
   The two `expect unordered` bodies are the claim, each followed by the
   `--- expect shape` section RFC 0045 makes mandatory after a rows
   expect:

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

--- expect shape
name: String

--- mutate branch: b0
query duplicate_add() {
    insert Knows { from: "w6", to: "charlie" }
}

--- expect affected: nodes=0 edges=1

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

--- expect shape
a.name: String
b.name: String

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

--- expect shape
a.name: String
b.name: String
```

The two merges take `expect ok` rather than `outcome:` because the outcome
word is not the case's subject; the final row set is. `ok` on a merge
fails if the merge conflicts, so a conflicting merge is blamed at the
merge step, never at the read after it. Acceptance for the amendment:
`second_merge_of_merged_branch` green in the corpus under the existing
walker, and each
held-out case red against this tree as recorded above and green in the PR
that lands its fix, where it enters the corpus.

## Rollout

The change lands in one PR after this RFC is `accepted`. That PR sets
`implementation: in-progress` and `updated` in this file while the RFC is
a draft, because `scripts/check-docs.py` refuses `complete` on a draft;
the edit that sets `status: accepted` sets `implementation: complete`
beside it (`docs/rfcs/README.md` §Process 5 and 6). Its four parts are the order of the
change and its internal seams, each named by the crates it touches; they
are not separate PRs, and no state between them ever exists on `main`.

1. **Compiler** (`omnigraph-compiler`, `omnigraph-cluster`, and the
   refusing `Branch` arms the enum forces where no dispatch exists: the
   stored-query registry in `omnigraph-server` and `select_named_query`
   in `omnigraph-cli`): grammar,
   `BranchStmt`, the `QueryFile` enum, the two structural refusals, the
   three name refusals, the refusing `Branch` arms in lint, the registry,
   and the CLI's `select_named_query`, and the
   cluster loader's two `.queries` sites
   (`config.rs:158` and `validate_query_source`), whose `Branch` arm is a
   diagnostic in the `query_parse_error` family, so `omnigraph cluster
   validate` refuses a catalog the server would refuse at boot. Same
   change: `docs/user/queries/index.md`'s `Q000` line gains that a file
   holding a branch statement where declarations were expected is also a
   `Q000`. Every existing `.gq` file parses as before. The compiler's one
   refusal text, `` `<statement>` is a branch statement, not a query
   declaration ``, is answered only where a statement can never be
   dispatched: the cluster loader, the stored-query registry
   (`QueryRegistry::from_specs`), `lint_query_file`, `find_named_query`,
   and the CLI's `select_named_query` guard. Parts 2 to 4 dispatch at the
   two handlers, the runner, and the verbs, so no door on `main` ever
   answers a statement with that text.
2. **Server and wire** (`omnigraph-server`, `omnigraph-api-types`):
   parse-first ordering in `run_query` and `run_mutate` (`classify`,
   `ReadDispatch`, `Door`), the wrong-door, request-target, name-and-params,
   precondition, and deprecated-route refusals, the handler-body split
   (`branch_*_body` behind the `server_branch_*` shells),
   `BranchOutcomeOutput` and the `outcome` field, the `commit` fill for a
   merge. Same change: `openapi.json` regenerated, `docs/user/branching/index.md`
   and `merge.md` amended with the statement spellings (`AGENTS.md:147,
   169` require both in the change that adds the endpoint or format), and
   the `POST /query` and `POST /mutate` operation descriptions and the
   `QueryRequest.query` and `ChangeRequest.query` field docs name the
   statements and their doors, the user-visible doc for the endpoint
   change.
3. **Logic tests** (`omnigraph-gqt`, RFC 0045 amendment): the `branch:`
   step argument, the `outcome:` expect mode, the statement-step
   refusals, the embedded dispatch with the reclaim join, the
   `second_merge_of_merged_branch` case (the other two held out, Evidence
   and tests), and the amendment
   sentences in RFC 0045's File format and Execution semantics with a
   Decision log entry pointing here; that entry also carries a
   Compatibility item stating that the fail-closed claim for the argument
   and the mode rests on the runner's refusals, and the 0045 body's
   evolution rule, which names sections and header keys, is untouched.
4. **CLI** (`omnigraph-cli`): both verbs classify before sending; the
   remote arm's statement requests (no target, `POST /mutate`), the local
   refusals, the embedded arm's dispatch, the shared text renderer for a
   `ChangeOutput` carrying `outcome`, the `main_tests.rs` request body
   pins, and the `confirm_destructive` guard on `BranchWrite::Delete`
   with its `cli_queries.rs` case. Same change: `docs/user/cli/index.md`
   amended with the four statement commands under `Work with branches`,
   `docs/user/cli/reference.md` with one paragraph pointing there (the
   page sits at the `check-docs.py` 350-line cap, and a subsection with
   the fenced examples would need the reviewed `docs-check:
   allow-long-page` exemption), and the agent-facing surface,
   `skills/omnigraph/SKILL.md`, `skills/omnigraph/references/data.md`,
   and `skills/omnigraph/references/commands.md`, gains the statement
   spelling beside the verb spelling with the wrong-door rule. The
   `omnigraph branch` verbs are
   untouched.

The RFC PR adds this file as `docs/rfcs/0055-gq-branch-statements.md`, its
registry row, and the next-number bump to `0056` in the same PR, with
`scripts/check-docs.py` green. The one-language RFC's families land as
their own PR sets, citing this RFC for the rules they inherit; neither its
phase 1 nor this RFC's PR waits on the other: whichever lands first
introduces the `QueryFile` enum, and the other extends it (that RFC's Rollout names
the variant it adds and the general spelling it gives the refusal strings
in User and operational behavior).

## Unresolved questions

None.

## Decision log

- 2026-09-04: initial draft.
- 2026-09-04: a `branch merge` conflict stays the route's HTTP 409, not a
  200 `outcome` (Alternatives 10).
- 2026-09-06: §AST and §Rollout 1 no longer name a typechecker or lowerer
  arm, because `typecheck_query_decl`, `typecheck_query`, and `lower_query`
  take a `QueryDecl` and never see the file; §Rollout 1 names every crate
  the enum forces an arm in and states the interim refusal between phase 1
  and each door; `implementation` is set by each phase PR after this RFC is
  `accepted`. This supersedes §AST's consumer list and its refusing-arm
  sentence, §Rollout 1's crate list and its claim that no surface accepts
  a statement yet, and the two `implementation` sentences in §Rollout 1
  and 4.
- 2026-09-06: this RFC is the first family of a roadmap under which every
  operation a graph exposes becomes a GQ statement; the remaining families
  go to a follow-up RFC that inherits the control-read and control-write
  rules set here (Summary, Motivation, Alternatives 3 and 11, Rollout).
- 2026-09-06, later the same day: the one-language RFC is the umbrella
  and delegates the branch family to this document; neither waits on the
  other, and no landing order holds between the two phase 1s. This
  supersedes the "first step of a roadmap", "follow-up RFC", and
  "land once phase 1 has shipped the `QueryFile` enum" wording of the
  entry above in Summary, Motivation, Alternatives 3 and 11, and Rollout.
  Same entry: every `branch list` rows expect carries the shape section
  RFC 0045 made mandatory (Design, Logic tests), and the RFC 0053
  credential ceiling is named as bounding a statement as it bounds its
  route (Design, Server dispatch; Compatibility, Policy).
- 2026-09-06, from the implementation PR, one landing: every part lands
  in one PR, and that PR sets `implementation: in-progress` (`complete`
  is refused on a draft by `scripts/check-docs.py`, so it is set beside
  `status: accepted`); the four
  Rollout parts are the order of the change and its seams, not separate
  PRs, so no state between them ever exists on `main`, and a harness, a
  door, or a verb is either older than this RFC or has its dispatch. This
  supersedes, in Rollout, "Ships alone" on each part, "Until its door
  ships, a statement at `POST /query`, `POST /mutate`, the CLI verbs, or a
  runner step is refused with the compiler's one text", "Phase 1 merges
  after this RFC is `accepted`, and the phase 1 PR sets `implementation:
  in-progress`", "the CLI's `-e` verbs reach them in phase 4", and "the
  phase 4 PR sets `implementation: complete`"; and, in Logic tests, "`the
  runner executes query declarations only` between phases 1 and 3" in the
  preamble and in the closing guarantee, both now stating the
  older-harness refusal alone. The interim-refusal wording of the first
  2026-09-06 entry above stays as history. Addition, nothing superseded:
  the `POST /query` and `POST /mutate` operation descriptions and the
  `QueryRequest.query` and `ChangeRequest.query` field docs name the
  statements (Rollout 2).
- 2026-09-06, from the implementation PR, server: the handler bodies are
  `branch_list_body`, `branch_create_body`, `branch_delete_body`, and
  `branch_merge_body`, behind the `server_branch_*` axum shells, which
  keep their `#[utoipa::path]` attributes; `select_named_query_decl` and
  `select_named_query` take the classified `Vec<QueryDecl>`, so the
  server's `Branch` arm lives in `run_query` and `run_mutate` only; and
  `branch_merge_body` drops the admission guard before
  `server_branch_merge`'s `delete_merged_source_branch` tail. This
  supersedes the "Handler body" column naming `server_branch_create`,
  `server_branch_delete`, `server_branch_merge`, and `server_branch_list`
  (Server dispatch, step 3) and "`select_named_query_decl`
  (`handlers.rs:2512`)" listed as a `.queries` consumer with an arm of its
  own (AST and classification); the admission sentence after the
  `delete_branch` composition sentence is an addition.
- 2026-09-06, from the implementation PR, runner: the header refusal is
  spelled `` `--- {kind}` takes no arguments but `branch: <name>`, got
  `{rest}` `` and a bare `branch:` is refused with `` `--- {kind}
  branch:` needs a branch name ``, the name otherwise passing to the
  engine; `outcome:` asserts the engine's `MergeOutcome`, spelled by
  `merge_outcome_word`; the `branch list` result is an arrow
  `RecordBatch`, so `arrow-array` is a dependency of `omnigraph-gqt`; a
  statement inside a loop runs literally per iteration; `${` in a
  statement body is refused by the compiler before the substitution
  fence; and RFC 0045's Decision log entry carries a Compatibility item.
  This supersedes "anything else in `rest` is refused with the grammar",
  "asserting `outcome.merge`", "Loops do not reach a statement", and
  "`${` in a statement body is refused as in any other step body
  (`lib.rs:1075-1077`)" (Logic tests), and "with a Decision log entry
  pointing here. Ships alone" (Rollout 3).
- 2026-09-06, from the implementation PR, CLI: both verbs classify in
  `main.rs` before either arm, so a declaration source on the embedded arm
  is parsed twice and a source that does not parse fails locally with the
  compiler's message before any round trip; the exact request bodies are
  pinned in `main_tests.rs`, the `/mutate` path by the `cli_queries.rs`
  round trip, and the server's bare-source acceptance in `data_routes.rs`;
  a statement's verb line is followed by the `actor_id:` line as for any
  `mutate` and no write target is echoed; `docs/user/cli/reference.md`
  holds one paragraph pointing at `cli/index.md`, which holds the four
  commands. This supersedes "the remote arm parses the source (the
  embedded arm already does, `select_named_query`, `helpers.rs:846-864`)"
  and "The CLI's exact request shape for each statement is pinned in
  `data_routes.rs`" (CLI), "For the runner, the CLI, and the server the
  arm is the dispatch below" (AST and classification), "the CLI's exact
  request shape for each statement" under Server (Evidence and tests),
  and "the `data_routes.rs` request shape test" and
  "`docs/user/cli/reference.md` and `cli/index.md` amended" (Rollout 4);
  the producer sentence in Compatibility, CLI, and the `actor_id:`
  sentence in Design, CLI, are additions.
- 2026-09-06, from the implementation PR, first cases: the three cases
  were written and run; `issue_600_second_merge_of_merged_branch.gqt` is
  green (`fast_forward` then `already_up_to_date`, blessed) and ships in
  this PR as the seventeenth case; `issue_583_…` reads through the
  bound-edge pattern `$a $e:knows $b`, since the plain traversal is green
  on the live bug; the seed-221206 case is named
  `sibling_merge_readopts_deleted_edge.gqt`, its duplicate add blessed at
  `nodes=0 edges=1` and its two rows expects carrying `--- expect shape`;
  both are red and held out of the corpus until their fixes, per RFC
  0045's landing-order precedent. This supersedes "the three merge-family
  findings become its first cases" (Summary), "these are the seventeenth
  through nineteenth", "read the edges on `main`; red returned two rows
  for one edge", "The two `affected:` lines are illustrative",
  `--- expect affected: nodes=0 edges=0` in the fragment, the fragment's
  two rows expects without a shape section, and "Acceptance for the
  amendment: all three cases red where a fix has not landed and green
  after" (Evidence and tests), and "the three cases" (Rollout 3).
- 2026-09-06, from the implementation. CLI
  dispatch: the classification selects the statement path only, so
  `QueryFile::Queries` and a source the CLI cannot parse both keep
  today's path and the unparseable source goes over the wire verbatim;
  this supersedes "One producer moves: a source that does not parse,
  addressed at a server, now fails locally with the compiler's message
  before any round trip" (Compatibility, CLI) and the same clause in the
  2026-09-06 CLI entry above. Posting methods: the statement requests are
  posted by `GraphClient::branch_list_statement` (`POST /query`) and
  `GraphClient::branch_statement` (`POST /mutate`), superseding
  "`GraphClient::query` posts `QueryRequest` … and `GraphClient::mutate`
  posts `ChangeRequest` with `branch: None` to `POST /mutate`" (Design,
  CLI). Merge `commit`: the head read is a `list_commits` walk, O(history)
  and the merge's own class, and is non-fatal, so a failed read answers
  `commit: null` and the merge stands; this supersedes "the exact merge
  commit id is an engine follow-up (`MergeOutcome` carrying it), not this
  RFC" (Server dispatch, step 4), which now retires both the walk and the
  race. Reclaim join: the join keeps the next step, a `--- restart`
  reopen, a `branch list`, and the tempdir teardown from overlapping an
  in-flight fork delete, superseding "and `--- restart` drops the handle
  (`lib.rs:1591-1592`)" as the reason, since dropping the handle detaches
  the reclaims rather than aborting them. Substitution fence: `${` inside
  a quoted statement name parses and the runner's fence refuses the step,
  superseding "`${` in a statement body is refused by the compiler as
  `does not parse` before the substitution fence that refuses it in a
  declaration body is reached" (Logic tests). Refusal string: the header
  refusal is `` a branch statement names its branches itself; drop the
  `branch:` argument ``, superseding "`a branch statement names its
  branches itself`" (Logic tests). First cases: the shipping case is
  `second_merge_of_merged_branch.gqt` with `# issue: none`, a feature
  case because no engine build was red on it (the DST driver was), its
  [#600](https://github.com/ModernRelay/omnigraph/issues/600) provenance
  in `# notes:` and no `# red_on:`; this supersedes
  "`issue_600_second_merge_of_merged_branch.gqt` ([#600]), in the corpus,
  green" and "the case's `# red_on:` line records that driver-side state,
  since a numbered case requires the header" (Evidence and tests),
  "`issue_600` green in the corpus" (Acceptance), "the `issue_600` case"
  (Rollout 3), and the file name in the 2026-09-06 first-cases entry
  above. The wire sentence in Design, CLI (a statement's `branch`,
  `snapshot`, `name`, and `params` are absent or `null` and the server
  reads both as absent) is an addition.
- 2026-09-06, later the same day, from the implementation.
  Write-only sub-type: `BranchStmt` is `Write(BranchWrite)` or `List`,
  with `BranchWrite::{Create, Delete, Merge}` carrying the fields, so
  `run_branch_statement`, the CLI's write path, and the runner's control
  step take a `BranchWrite` and have no `List` arm; `refuse_wrong_door`
  keys on the variant. This supersedes the four-variant `BranchStmt` block
  and its `is_write` line as the whole classification (AST and
  classification), "`QueryFile::Branch(stmt)` with `stmt.is_write()` at
  `run_query`, or `BranchStmt::List` at `run_mutate`" (Server dispatch,
  step 2), and "`BranchStmt::Delete`" in Design, CLI, and Rollout 4;
  `is_write` stays as the one-line accessor. Posting method: the control
  writes are posted by `GraphClient::branch_write_statement`, superseding
  "`GraphClient::branch_statement` (`POST /mutate`)" in Design, CLI, and
  in the entry above. Module layout, an addition: `Door`,
  `ReadDispatch`, `classify`, the door and envelope refusals, and
  `run_branch_statement` live in
  `crates/omnigraph-server/src/handlers/dispatch.rs`; `run_query`,
  `run_mutate`, and every `#[utoipa::path]` shell stay in `handlers.rs`.
