---
rfc: "0056"
title: "One language for every graph operation"
track: maintainer
status: draft
implementation: not-started
authors:
  - Azim Afroozeh
created: 2026-09-06
updated: 2026-09-06
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0056: One language for every graph operation

> **PLACEHOLDER: DO NOT MERGE, NOT YET UNDER REVIEW.** This draft is
> published so the direction has a durable home beside the branch-statements
> RFC, which covers the branch family of this document. This banner is
> removed when review opens.

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

> The number is provisional: `0055` is the next available number at the
> upstream commit this document is anchored to and is reserved for the
> branch-statements RFC, whose PR opened first and renumbers to it; this
> document takes `0056`, and the file, the heading, the frontmatter, and
> the registry row are renumbered together when the PR opens if another
> RFC has taken it by then.

## Summary

Every operation a graph exposes becomes a GQ statement. The branch
statements, `branch create`, `branch delete`, `branch merge`, `branch
list`, are part of this RFC and are delegated to RFC 0055, which specifies
them and the rules that govern them; neither document waits on the other. This RFC makes those
rules the contract of a ***control statement***, a standalone top-level GQ form
that performs or inspects one graph operation, names every target it acts
on itself, takes no parameters, and never appears inside a mutation body; a
***family*** is one operation's group of statements sharing a leading
keyword, and the branch family is the first. The families added here are
`load`, `schema` (`show`, `apply`, `plan`), `snapshot`, `commit` (`list`,
`show`, `changes`), `changes poll`, `blob stat`, `queries list`, and the
four maintenance operations `optimize`, `rebuild full text indexes`,
`repair`, and `cleanup`. The compiler classifies each statement as it
classifies a body as read or mutation, and the classification picks the
***door***, the route or CLI verb a statement enters through: a ***control
write*** publishes a commit or changes durable state and enters through
`POST /mutate` and `omnigraph mutate`; a ***control read*** publishes
nothing and enters through `POST /query` and `omnigraph query`. An
operation's door is its committed form's door, so bare `repair`, a preview that
publishes nothing, still enters at the write door with `repair confirm`
(Design, Maintenance). Each
statement that has a route is authorized with the Cedar action and scope
pair that route uses today and runs its handler body and engine call;
`schema plan`, which has no route, is embedded-only and runs under no
Cedar pair (Design, Server dispatch), and the four maintenance statements
are governed by Design, Maintenance. With one
owned exception no new permission path exists on either gate, the RFC 0053
data-token action grant checked first or the Cedar decision after it: the
three maintenance operations with no Cedar action today (`optimize`,
`repair`, `cleanup`) get one, `maintain`, enforced at the HTTP boundary and
in new engine `_as` entries, the engine entries that take an actor.
A payload (loader rows, a `.pg` schema) travels inline in a ***raw
block***, a dollar-quoted span `$$ … $$` the grammar treats as one opaque
token. Answers travel in the door's envelope: `ReadOutput` rows for a
control read, one projection per family; `ChangeOutput` with a
family-specific `outcome` object for a control write. Maintenance
statements execute embedded now (the logic-test runner and `omnigraph
mutate --store`); their served form is a submitted job, as RFC 0011
Decision 11 commits (it commits `optimize`, `cleanup`, and healthy-path
`repair` to policy-gated, audited, single-coordinator jobs and defers the
mechanism to a follow-up RFC); phase 5 lands with that mechanism RFC.

The boundary that does not change: every route and verb stays with its
handler, request type, and response type, nothing is deprecated; the
engine crate gains only the three `_as` entries, in phase 4;
`QueryRequest`, `ChangeRequest`, and `ReadOutput` keep every existing
field; there
is no session, current branch, or checkout; the cluster control plane
(`cluster.yaml`, `cluster plan`, `cluster apply`, policy bundles, the
`/graphs` registry) is untouched; the streaming and byte routes (`POST
/export`, `POST /changes/baseline`, `GET /blob`) and `init` get no
statement. One door for every statement is out of scope (Alternatives 3).

## Motivation

GQ is the language of every read and every mutation, and after RFC 0055
of every branch operation. At upstream commit `c253f111` (every
`file:line` anchor in this document is at that commit; anchors into RFC
0055 name its sections, since that draft is under review in
[#626](https://github.com/ModernRelay/omnigraph/pull/626)), every other
operation is reachable only through its own route and verb, or the CLI
alone:

| Operation | HTTP route (`/graphs/{graph_id}/…`) | CLI verb (capability) | Cedar action and scope | GQ today |
|---|---|---|---|---|
| read query | `POST /query` (`/read` deprecated) | `query` (any) | `read` on the branch | yes |
| mutation | `POST /mutate`, `/mutate/if-graph-commit` (`/change` deprecated) | `mutate` (any) | `change` on the branch | yes |
| stored query | `POST /queries/{name}[/if-graph-commit]` | `query <name>`, `mutate <name>`, `alias` | `invoke_query`, then the inner gate | yes, named |
| branch create, list, delete, merge | `GET`/`POST /branches`, `DELETE /branches/{branch}`, `POST /branches/merge` | `branch …` (any) | `branch_create`, `read`, `branch_delete`, `branch_merge` | RFC 0055 |
| load | `POST /load`, `POST /load/ndjson` (`/ingest` deprecated) | `load --mode` (any) | `change` on the branch, plus `branch_create` with `from` | no |
| export | `POST /export` | `export` (any) | `export` on the branch | no, NDJSON stream |
| snapshot | `GET /snapshot` | `snapshot` (any) | `read` on the branch | no |
| schema show, apply, plan | `GET /schema`, `POST /schema/apply`; plan has no route | `schema show`, `schema apply` (any), `schema plan` (direct) | `read` no scope; `schema_apply` target `main` | no |
| commit list, show, changes | `GET /commits`, `/commits/{commit_id}`, `/commits/{commit_id}/changes` | `commit …` (any) | `read` on the branch; show: no scope | no |
| change feed poll, baseline | `GET /changes`, `POST /changes/baseline` | `changes poll`, `changes baseline` (any) | `read` on the branch; baseline: `export` | no |
| blob get, stat | `GET`/`HEAD /blob` | `blob get`, `blob stat` (any) | `read` on the resolved target | no, bytes or headers |
| stored-query catalog | `GET /queries` | `queries list` (control) | `read` on `main` | no |
| optimize, repair, cleanup | none | `optimize`, `repair`, `cleanup` (direct) | none | no |
| rebuild full-text indexes | none | `rebuild-full-text-indexes` (direct) | `change` on the branch, in the engine only | no |

Routes: `crates/omnigraph-server/src/lib.rs:1863-1956` (26 registrations
over 24 per-graph paths, 3 deprecated). Cedar per handler, in
`crates/omnigraph-server/src/handlers.rs`: `:537-539`, `:921-923`,
`:1634-1636`, `:1676-1678`, `:1734-1736`, `:1819-1832`, `:2434-2436`,
`:2487-2489`, `:2837-2839`, `:3038-3040`, `:3114-3116`, and the blob and
query reads through `resolve_authorized_read_target` (`:1178,
1205-1207`). Verb capabilities: `crates/omnigraph-cli/src/cli.rs:15-27`,
`crates/omnigraph-cli/src/planes.rs:243-282`. The rebuild gate:
`crates/omnigraph/src/db/omnigraph/table_ops.rs:81-85`. Three costs
follow, in the order this RFC weighs them.

First, agent experience. An agent that reads and writes rows puts GQ text
into one field, `QueryRequest.query` or `ChangeRequest.query`
(`crates/omnigraph-api-types/src/lib.rs:665-683, 818-839`), which every
client carries: the HTTP body, `omnigraph query -e` and `mutate -e`, an
SDK call, an MCP tool (RFC 0003), Tower, a `.gqt` step. The moment the
task needs a load, a schema change, or a look at history, the agent leaves
that field for a route with its own request type, or for a verb it cannot
run because agents have no shell, and every client grows one method per
operation: N operations times M clients, each method a place the two can
diverge, which RFC 0009's parity matrix exists to catch. A statement is
text, so one spelling reaches every client with no per-client code. RFC
0003 projects one typed tool per operation, thirteen tools carried over
plus a server-scoped `graphs_list`, and one per stored query
(`docs/rfcs/0003-mcp-server-surface.md:31-32`); under
this RFC its `read` and `change` tools become control carriers and tool
curation moves from the tool list to the Cedar action, which 0003's §5.9
policy (`:235`) already assumes. Anthropic's tool guidance points the same
way: "More tools don't always lead to better outcomes", and overlapping
tools confuse tool selection
([Writing effective tools for AI agents](https://www.anthropic.com/engineering/writing-tools-for-agents)).
Whether the statement form is better for agents in practice is the
evaluation in Evidence and tests.

Second, governance. The four maintenance operations run only from a shell
holding storage credentials: "They do not run through the HTTP server"
(`docs/user/operations/maintenance.md:10`); "Direct CLI access, including
`--cluster`, does not load the server's Cedar policy; storage permissions
are its trust boundary" (`:77-79`). No Cedar decision and no server audit
record exists for a compaction, a repair, or a destructive cleanup, and
the gap is visible on the wire: `FullTextIndexRebuildRequiredOutput`
tells a served client that "an operator must rebuild the live branch's
indexes" (`crates/omnigraph-api-types/src/lib.rs:1260-1270`, HTTP 409),
and no route lets that client do it. RFC 0011 Decision 11 already commits
maintenance to policy-gated, audited jobs; this RFC supplies the
statement those jobs are requested with.

Third, tests. RFC 0045's `.gqt` format can test only what GQ can say: its
seed is setup, loaded once through the loader's compatibility shape
(`crates/omnigraph-gqt/src/lib.rs:1560`), and `--- expect` binds to a
query or mutate step only (`lib.rs:884-888`), so the loader, a schema
migration, and every maintenance operation have Rust-only regressions or
none. RFC 0055's Alternatives 2 shows the cost of runner directives: a
grammar the compiler does not own. Statements close the gap with none.

No document records why `load`, `schema`, and the inspection reads were
kept off the language; for maintenance RFC 0010, now superseded, kept it
off the wire as "a heavyweight, destructive multi-tenant surface"
(`docs/rfcs/0010-cli-planes.md:274-278`), and RFC 0011 made the split
explicit and committed to reversing it. A local fix is not enough for the
reason RFC 0055 gives: each family is a new query statement kind, a
wire-visible answer shape, and a test-format amendment. The first two are
in the registry's RFC-required list (`docs/rfcs/README.md:18-19`); the
test-format amendment follows RFC 0045's Compatibility: fail-closed
extension, each new sentence decided by the first case that needs it
(`0045:820-827`).

## User and operational behavior

The statements, spelled once. A bracket marks an optional clause; `<mode>`
is `overwrite`, `append`, or `merge`; a raw block is `$$ … $$`. ***Page
state*** is the set of continuation fields a paged route answers beside its
items (`next_page_token`, `cursor`, `caught_up`); a control read carries it
in an optional `page` object on `ReadOutput`, skipped when the family has
none, so every existing read stays byte-identical. The same object carries
the header-level values a family's route answers beside its items
(`schema plan`'s `supported`, `snapshot`'s manifest and schema versions),
because those values belong to the answer and not to any row (Design,
Server dispatch).

| Statement | Door | Default | Answer |
|---|---|---|---|
| `load <mode> [on <branch>] [from <parent>] $$ rows $$` | `POST /mutate`, `omnigraph mutate` | branch `main` | `ChangeOutput`, counts filled, `outcome.kind = "loaded"` |
| `schema show` | `POST /query`, `omnigraph query` | none | `ReadOutput`, one row, columns `schema_source` and `accepted_schema` |
| `schema apply $$ schema $$ [allow data loss] [actor provenance on \| off]` | `POST /mutate`, `omnigraph mutate` | drops are soft; an omitted `actor provenance` clause preserves the accepted setting | `ChangeOutput`, `outcome.kind = "schema_applied"`; every deployed server answers the route's 409, since every one boots from a cluster |
| `schema plan $$ schema $$` | `omnigraph query --store` (embedded); the served door refuses it until a later decision | none | `ReadOutput`, one row per migration step, `supported` in `page` |
| `snapshot [of <branch>]` | `POST /query`, `omnigraph query` | branch `main` | `ReadOutput`, one row per dataset, header values in `page` |
| `commit list [on <branch>]` | `POST /query`, `omnigraph query` | branch `main` | `ReadOutput`, one row per commit, newest first |
| `commit show "<id>"` | `POST /query`, `omnigraph query` | none | `ReadOutput`, one row |
| `commit changes "<id>" [after page "<token>"] [limit <n>] [kind <k>] [type <T>] [op <o>]` | `POST /query`, `omnigraph query` | server page size | `ReadOutput`, one row per entity change, page state in `page` |
| `changes poll [on <branch>] [from now \| from cursor "<c>" \| from beginning \| after commit "<id>" \| after page "<token>"] [limit <n>] [kind <k>] [type <T>] [op <o>]` | `POST /query`, `omnigraph query` | branch `main`, start `now` | `ReadOutput`, one row per entity change, page state in `page` |
| `blob stat <node \| edge> <Type> "<id>" <property> [on <branch> \| at snapshot "<id>"]` | `POST /query`, `omnigraph query` | branch `main` | `ReadOutput`, one row |
| `queries list` | `POST /query`, `omnigraph query` (no embedded arm) | none | `ReadOutput`, one row per catalog entry |
| `optimize` | `omnigraph mutate --store` (embedded); `POST /mutate` in phase 5, as a job | none | `ChangeOutput`, `outcome.kind = "optimized"` embedded; the served answer kind is the mechanism RFC's |
| `rebuild full text indexes [on <branch>]` | `omnigraph mutate --store` (embedded); `POST /mutate` in phase 5, as a job | branch `main` | `ChangeOutput`, `outcome.kind = "rebuilt"` embedded; the served answer kind is the mechanism RFC's |
| `repair [confirm [force]]` | `omnigraph mutate --store` (embedded); `POST /mutate` in phase 5, as a job, `force` excluded | preview | `ChangeOutput`, `outcome.kind = "repaired"` embedded; the served answer kind is the mechanism RFC's |
| `cleanup keep <n> [older than <duration>] confirm` or `cleanup older than <duration> confirm` | `omnigraph mutate --store` (embedded); `POST /mutate` in phase 5, as a job | none | `ChangeOutput`, `outcome.kind = "cleaned"` embedded; the served answer kind is the mechanism RFC's |

The branch family keeps RFC 0055's rows. A branch name is an identifier or
a quoted string, quoted whenever the value leaves the identifier alphabet.
A commit id, a cursor, and a page token are all quoted in these rows and
in every example below; the grammar's `value_name` keeps `ident` for the
entity ids that can be one. A commit id always leaves the alphabet: it is
a 26-character uppercase ULID (`new_ulid`,
`crates/omnigraph/src/dst_ids.rs:55-62`, "every production identity mint in
this crate comes through here"), uppercase letters and digits only, which
no `ident` can be (`ident` starts with a lowercase letter or `_`,
`query.pest:111`). A cursor and a page token are base64url
(`crates/omnigraph/src/changes/token.rs:3, 343`), lowercase-led and with
or without `-`, so both are quoted by rule and not by alphabet:
`value_name` admits an `ident`, and an unquoted token is a parse error
whenever it carries `-`. An entity id and a snapshot id are quoted on the same rule,
which a numeric id such as `42` trips. `<duration>` is the verb's shape:
digits with an optional unit, one of `s`, `m`, `h`, `d`, `w`, a bare digit
string meaning seconds (`parse_duration_arg`,
`crates/omnigraph-cli/src/helpers.rs:623-650`); a composite such as `1h30m`
is refused by the verb and by the statement alike. The read door stays
read-only: every control read publishes no commit and changes no ref, and a
control write is refused there before any engine call, as RFC 0055 rules.

Postconditions, one per family. After `load` answers `loaded`, a read on
the branch returns the loaded rows, `outcome.branch_created` is true
exactly when `from` named a parent and the branch did not exist, and
`commit` is the branch head (one publication,
`crates/omnigraph/src/loader/mod.rs:261-278`). After `schema apply`
answers `schema_applied` with `applied: true`, `schema show` returns the
applied source and `outcome.steps` lists the migration steps
(`SchemaApplyOutput`, `crates/omnigraph-api-types/src/lib.rs:1013-1021`);
a schema identical to the accepted one answers `applied: false`, `steps:
[]`, and publishes nothing, so nothing about the graph changed and a
following `schema show` returns what it returned before. After `schema
plan` answers, the plan describes the migration the same source would
attempt; it is not a promise that a later `schema apply` succeeds, since
the accepted schema may move between the two. After `rebuild full text
indexes` answers `rebuilt`, a full-text read on that branch no longer
answers the 409 `full_text_index_rebuild_required`. After `optimize`
answers `optimized`, every dataset it committed is compacted to the
`published_dataset_version` the row reports and no row-level content
changed; after `repair` answers `repaired`, every dataset whose `action`
is `healed` or `forced` has its published version reconciled with storage,
and a dataset whose `action` is `refused` is unchanged. After `cleanup`
answers `cleaned`, the removed versions are gone from the object store for
every dataset whose `error` is null; a dataset with `error` set kept its
versions and is retried by the next `cleanup`. The statement is as
destructive as the verb and carries the same two-step consent (`confirm`
in the text, `confirm_destructive` in the CLI), and its CLI exit code is
the verb's: `repair` exits 1 when any dataset is `refused`
(`crates/omnigraph-cli/src/main.rs:1558-1565`), `cleanup` exits 0 with
per-dataset `error` set (`:1635-1654`, which prints the failed datasets
and returns). A denied `commit changes` answers the 404 `commit '<id>' not
found`, the answer an unknown commit gives, because a 403-versus-404 split
would be a graph-wide commit-existence oracle
(`crates/omnigraph-server/src/handlers.rs:2843-2853`); a denied `blob
stat` is redacted the same way (`redact_blob_api_error`, `:871`). A raw
block over the door's cap is rejected by the router's `DefaultBodyLimit`
layer before the `Json<QueryRequest>` extractor runs (`handlers.rs:651-655`),
so the answer is axum's own 413 rejection and not the `ErrorOutput`
envelope the handler bodies produce. A control read holds one accepted
view for its lifetime, as every read does (`docs/dev/invariants.md:31`).

Guarantee: a statement and its route produce the same engine effect, the
same Cedar decision, the same admission check, and the same error mapping
for every input, because they run one handler body (Design); they differ
only in envelope. Maintenance, which has no route, is governed by Design,
Maintenance.

Guarantee: a control read is authorized exactly as its route is, so for
one actor it returns exactly the rows the route returns; a scope-free
`read` (`schema show`, `commit show`) satisfies only a rule written for
any branch, as RFC 0055 states for `branch list`; `queries list` is `read`
on `main` (`handlers.rs:1634-1636`), not scope-free.

Consent on the served door. The text `confirm` is the whole consent for a
served `cleanup`: `confirm_destructive` is the CLI's prompt for a human
seat and has no server counterpart, as `POST /load` with `mode:
overwrite` has none today. For an agent, `cleanup … confirm` and `load
overwrite` are one-token destructive writes gated by `maintain` and
`change` alone, and a client that wants a second step supplies it.

From the CLI the statements arrive through the existing verbs and their
source flags, `--query <file>` and `-e`/`--query-string`
(`cli.rs:113-123, 146-156`):

```
omnigraph mutate -e 'load merge on b0 $$
{"type":"Person","data":{"name":"ada"}}
{"edge":"Knows","from":"ada","to":"bob"}
$$'
omnigraph mutate --query next_schema.gq          # schema apply $$ … $$ allow data loss
omnigraph query  -e 'commit list on b0' --format table
omnigraph query  -e 'commit changes "01JQ8Z3K7YB4N2VW5T6XH9DCFA" limit 50'
omnigraph query  -e 'changes poll from beginning limit 100'
omnigraph mutate -e 'rebuild full text indexes on b0' --store ./graph.omni
omnigraph mutate -e 'cleanup keep 5 older than 2w confirm' --store ./graph.omni --yes
```

Both verbs classify the source before sending, post a statement with no
request target, and fail locally on a stray `--branch`, `--snapshot`,
`--if-commit`, positional name, `--params`, or `--params-file`, as RFC
0055's CLI section rules. The classification happens inside the verb,
before `GraphClient::mutate` is called: today `Command::Mutate` hands the
source straight to `client.mutate`
(`crates/omnigraph-cli/src/main.rs:1237-1245`) and the only parse is inside
that method's embedded arm (`select_named_query`, `client.rs:841`), so the
parse moves up into the verb, which is what lets the consent prompt below
know what it is prompting for. `confirm_destructive`
(`crates/omnigraph-cli/src/helpers.rs:52`), which refuses a non-local
target without `--yes` or a TTY answer, runs on the statements whose
verbs run it: `load overwrite` (`main.rs:391-392`), `branch delete`
(`main.rs:496`), and `cleanup` (`main.rs:1609`). Before a family's door
phase ships, the verb refuses the statement locally, ahead of that prompt,
so a `cleanup` statement is never consented to and then refused by the
interim 400. The verb trees stay on their routes and engine calls; the
statement path shares their text renderers, their consent prompts, and the
`schema apply` verb's cluster-ownership refusal (`main.rs:925-943`,
`cluster_root_for_graph_uri`), which the embedded statement arm runs before
its engine call: a `schema apply` statement against a graph root inside a
cluster is refused with the verb's message on the embedded arm, as the
served arm answers the route's 409.

Refusals. Each is an HTTP 400 on the server, except the rows that name
their own status (413, 409, 404), and a non-zero exit with the same
message in the CLI, in the shape of RFC 0055's refusal table, which this
table inherits row by row (wrong door, request target, `name` and
`params`, expected head, deprecated route, the two parse errors) and
extends. RFC 0055 words those strings for its one family, so phase 1
spells them generally, once (a rewording when RFC 0055's phase 1 landed
first): "a control statement names its targets
itself", "a control statement takes no name and no parameters", "a control
statement takes no commit precondition", "control statements are not
served on deprecated routes; use POST /mutate or POST /query".

| Rule | Refusal |
|---|---|
| a control write at the read door | `statement 'load' is a control write; use POST /mutate` |
| a control read at the write door | `statement 'commit list' is a read; use POST /query` |
| a raw block larger than the door's body limit | 413 from the router's body-limit layer, as for any oversize body: `POST /mutate` and `POST /query` are capped by the router-wide `DefaultBodyLimit` at `DEFAULT_REQUEST_BODY_LIMIT_BYTES`, 1 MiB (`crates/omnigraph-server/src/lib.rs:164, 1963`); `/load` and `/ingest` carry a 32 MiB `INGEST_REQUEST_BODY_LIMIT_BYTES` layer (`lib.rs:165, 1905, 1917`) and `/load/ndjson` enforces the same cap inside its handler, since it reads the raw body (`collect_graph_batch_body`, `handlers.rs:1924-1948`, plus the Content-Length precheck `:2008-2026`); a bulk load keeps the route and the verb (Compatibility) |
| `schema apply` on a server | the route's 409, ``server-side schema apply is disabled for cluster-backed serving; update the cluster config, run `omnigraph cluster apply`, and restart the server.``, after the Cedar gate (`handlers.rs:1739-1749`); every `omnigraph-server` process boots from a cluster, so this fires on every deployed server (Compatibility) |
| a `schema apply` statement on the embedded arm against a graph root inside a cluster | the verb's message, ``A graph in a cluster evolves via `cluster apply``` (`main.rs:919-943`, `cluster_root_for_graph_uri`), before any engine call |
| `load` naming a branch that does not exist without `from` | the body's 404, `branch '<b>' not found; pass `from` to create it`, raised by `authorize_load_scope` before the Cedar check exactly as the route raises it (`handlers.rs:1791-1814`); the branch-existence disclosure that ordering carries is inherited, not added |
| `cleanup` without `confirm` | `cleanup is destructive; add confirm` (the verb prints its policy and exits 0 at the same point, `main.rs:1599-1604`, which is the behavioural authority; the statement is refused with a non-zero exit) |
| `repair force` without `confirm` | a parse error: `force` is grammatical only after `confirm`, as the flag requires it (`cli.rs:326`) |
| `repair confirm force` at `POST /mutate` | `repair force is break-glass; run the verb with --cluster or --store` (RFC 0011 Decision 11 keeps forced repair a direct verb); the grammar keeps `force` for the embedded arm |
| a statement of any family at either door before that family's door phase ships | `statement '<name>' is not served yet` (Rollout); for the four maintenance families the message adds `; run it embedded with --store` from phase 4, once that arm exists, since embedded is their standing form |
| `schema plan` at `POST /query` | `statement 'schema plan' is not served; run it embedded with --store` (Design, Server dispatch: it is embedded-only until a later decision) |
| a poll clause combining two of `from cursor`, `from beginning`/`after commit`, `after page` | a parse error: the grammar admits one position clause, so the body's mutual-exclusion 400, `cursor, start, and page_token are mutually exclusive` (`handlers.rs:3044-3049`), is unreachable from a statement |
| a cursor or page token under a different branch or filter set than the one that minted it | the engine's `change cursor rejected: <reason>` (`crates/omnigraph/src/error.rs:199`), mapped by the body, since "the canonical digest of this scope binds page tokens and feed cursors" (`crates/omnigraph/src/changes/model.rs:197-198`) |
| a raw block on a statement that takes none, or a missing one where the family requires it | a parse error from the compiler; an empty block, `$$$$`, is not a parse error, and the operation's own parser answers it |
| an integer that does not fit `u32` (`keep`) or `u64` (`limit`) | a parse error from the compiler, beside the name refusals |
| `affected:` in a `.gqt` expect on any control write other than `load` | `affected: is accepted on load only` (Design, Logic tests) |

Guarantee: no control statement is ever executed at a door other than its
own, and a request that reaches the engine carries exactly one source of
truth for every branch, commit, or payload it acts on.

A refusal is a rule applied to people, so its evasions and honest routes
follow. RFC 0055's evasion rows hold for every family; these are the rows
the new families add.

| Evasion | What stops it |
|---|---|
| run `cleanup` or `optimize` against a served graph through `POST /mutate` to skip the storage credential | before phase 5 the door refuses it; after phase 5 it is a job under `maintain`, the Cedar decision the operation lacks today (Design, Maintenance) |
| split a 32 MiB load into raw blocks under 1 MiB to ride `/mutate` | nothing stops it and nothing needs to: each block is one commit under `change` on the branch, as each `POST /load/ndjson` call is today; the cap bounds the request, not the actor |
| smuggle `.pg` text past the cluster control plane with `schema apply` on a cluster-backed server | the route's 409 fires after the Cedar gate, from the same handler body |
| put `$$` inside the payload to end the block early and append a second statement | the grammar: a file holds one statement, so text after the closing `$$` is a parse error; a payload that must contain `$$` is not expressible (Alternatives 6) |
| skip `confirm` on `repair force` or `cleanup` | `force` does not parse without `confirm`; `cleanup` without `confirm` is refused before any engine call |
| smuggle `.pg` past the cluster control plane with `--store` on the cluster's own graph root, `omnigraph mutate --store <cluster graph root> -e 'schema apply $$…$$'` | the `schema apply` verb's ownership check runs on the statement arm too (`main.rs:919-943`), so the embedded arm refuses it before any engine call |
| create a branch through `load <mode> on <new> from <parent>` holding `change` only | the body's first check is `BranchCreate` with `branch: Some(from)`, `target_branch: Some(branch)`, as `POST /load` does (Design, Server dispatch) |

| Honest route | Accepted |
|---|---|
| `POST /mutate` with `{"query": "load merge $$ … $$"}` and no `branch`; `POST /query` with `{"query": "commit list on b0"}` and no target | yes, the canonical doors |
| every existing route, with its own request type | yes, unchanged |
| `omnigraph mutate -e '…'`, `--query <file>`, `omnigraph query -e '…'`, with or without `--store`; every verb on its route, including `direct` for the maintenance verbs | yes, unchanged |
| a `.gqt` `--- mutate` step holding `load`, `schema apply`, or a maintenance statement; a `--- query` step holding any control read except `queries list`, which has no embedded arm | yes (Design, Logic tests) |
| `omnigraph cluster apply` for a cluster-managed graph's schema | yes, the only route for it (RFC 0004), unchanged |

Operationally: a control write is admission-gated per actor exactly as its
route is, after Cedar (`handlers.rs:1085-1087`); a control read is not, as
no read is (`handlers.rs:1130-1132`). One Cedar action, `maintain`, is
added; no route or configuration is added. Served maintenance is a
submitted job whose answer kind, job record, and any observation
statements over it belong to the RFC 0011 Decision 11 mechanism RFC; it
lands with that RFC (Rollout, phase 5).

## Design

### Grammar

`query.pest` (`crates/omnigraph-compiler/src/query/query.pest`, 121 lines
at this commit) gains the top-level alternative RFC 0055 adds as
`branch_stmt`, widened to one rule per family:

```
query_file     = { SOI ~ (control_stmt | query_decl*) ~ EOI }

control_stmt   = { branch_stmt | load_stmt | schema_stmt | snapshot_stmt | commit_stmt
                 | changes_stmt | blob_stmt | queries_stmt | maintain_stmt }

load_stmt      = { kw_load ~ load_mode ~ (kw_on ~ branch_name)? ~ (kw_from ~ branch_name)? ~ raw_block }
load_mode      = { kw_overwrite | kw_append | kw_merge }
schema_stmt    = { kw_schema ~ (kw_show
                 | kw_apply ~ raw_block ~ (kw_allow ~ kw_data ~ kw_loss)?
                                        ~ (kw_actor ~ kw_provenance ~ (kw_on | kw_off))?
                 | kw_plan ~ raw_block) }
snapshot_stmt  = { kw_snapshot ~ (kw_of ~ branch_name)? }
commit_stmt    = { kw_commit ~ (kw_list ~ (kw_on ~ branch_name)?
                 | kw_show ~ value_name
                 | kw_changes ~ value_name ~ (kw_after ~ kw_page ~ value_name)?
                                           ~ (kw_limit ~ integer)? ~ feed_filter*) }
changes_stmt   = { kw_changes ~ kw_poll ~ (kw_on ~ branch_name)?
                 ~ poll_start? ~ (kw_limit ~ integer)? ~ feed_filter* }
poll_start     = { kw_from ~ kw_now | kw_from ~ kw_cursor ~ value_name
                 | kw_from ~ kw_beginning | kw_after ~ kw_commit ~ value_name
                 | kw_after ~ kw_page ~ value_name }
feed_filter    = { kw_kind ~ ident | kw_type ~ type_name | kw_op ~ ident }
blob_stmt      = { kw_blob ~ kw_stat ~ (kw_node | kw_edge) ~ type_name ~ value_name ~ ident ~ read_target? }
read_target    = { kw_on ~ branch_name | kw_at ~ kw_snapshot ~ value_name }
queries_stmt   = { kw_queries ~ kw_list }
maintain_stmt  = { kw_optimize
                 | kw_rebuild ~ kw_full ~ kw_text ~ kw_indexes ~ (kw_on ~ branch_name)?
                 | kw_repair ~ (kw_confirm ~ kw_force?)?
                 | kw_cleanup ~ cleanup_policy ~ kw_confirm? }
cleanup_policy = { kw_keep ~ integer ~ (kw_older ~ kw_than ~ duration)? | kw_older ~ kw_than ~ duration }

branch_name    = { ident | string_lit }        // RFC 0055
value_name     = { ident | string_lit }        // a commit id, cursor, page token, entity id, snapshot id
duration       = @{ ASCII_DIGIT+ ~ ("w" | "d" | "h" | "m" | "s")? }
raw_block      = @{ "$$" ~ (!"$$" ~ ANY)* ~ "$$" }

kw_load        = @{ "load" ~ !(ASCII_ALPHANUMERIC | "_") }   // likewise every other kw_ rule
```

Three rules carry the design. First, every keyword is an atomic rule
closed by a word boundary, RFC 0055's rule, so `loadmerge` and `cleanup
keep5` are parse errors; nothing is reserved, `ident` stays any
lowercase-start word (`query.pest:111`), and every word here remains an
ordinary identifier inside a body (`limit` is already the literal of
`limit_clause`, `:82`). Second, the leading keyword disambiguates at the
start of the file, where today only `query` is legal (`:8, 11`), so no
file that parses today changes meaning. Third, the raw block is atomic
and disjoint from every existing token: `variable = @{ "$" ~ (ident_chars
| "_") }` (`:103-104`) requires a lowercase letter or underscore after
`$`, so `$$` can never begin a variable, and no other rule mentions `$`.
Atomicity protects the payload: pest inserts `WHITESPACE` and `COMMENT`
(`:3-6`) between the tokens of every non-atomic rule, and `LINE_COMMENT`
is `"//" ~ (!"\n" ~ ANY)*` (`:5`), so a non-atomic block would let a `//`
inside an NDJSON URL or a `.pg` comment eat the rest of the line. The
block's content is the bytes between the delimiters with the first and
last newline stripped: no escape, no trim, no comment. A payload
containing `$$` is not expressible (Alternatives 6); text after the
closing `$$` is a parse error. An empty block, `$$$$`, parses and is not a
missing one: the operation's own parser answers it, an empty NDJSON batch
for `load` and an empty `.pg` source for `schema apply`.

Clause order is fixed and each paging clause appears at most once: a
paging clause precedes every filter, so `commit changes "01J…" kind node
limit 5` is a parse error, and `limit 1 limit 2` or two `after page`
clauses are not expressible. Filters repeat, because the route's `kind`,
`type`, and `op` are multi-valued (`ChangeFeedQuery`,
`crates/omnigraph-api-types/src/lib.rs:553-572`). `kind` takes `node` or
`edge` (`EntityKindOutput`, `:55-58`) and `op` takes `insert`, `update`,
or `delete` (`ChangeOpOutput`, `:408-412`); any other word reaches the
body, which refuses it as the route does. `from now` spells the default
start, which the route reaches by omitting `start`
(`parse_change_feed_start`, `handlers.rs:2967-2972`), so the default has a
name a statement can write. `duration` follows the verb's parser, digits
with an optional unit and `w` among them (`parse_duration_arg`,
`crates/omnigraph-cli/src/helpers.rs:623-650`), so `cleanup older than 2w`
and `cleanup older than 3600` are the statement forms of flag values the
verb accepts; a composite such as `1h30m` is refused by both.

The payload's grammar belongs to the operation. A `load` block is the
strict envelope `POST /load/ndjson` and the `load` verb use, one node
envelope `{"type":…,"data":{…}}` or edge envelope
`{"edge":…,"from":…,"to":…,"data":{…}}` per nonblank line, refusing
duplicate members, unknown fields, and compatibility coercions
(`handlers.rs:1959`; `crates/omnigraph/src/loader/mod.rs:238-246`), not
the compatibility shape of `POST /load` and the `.gqt` seed (Unresolved
questions, Q5). A `schema apply` or `schema plan` block is `.pg` source,
parsed by the schema grammar (`crates/omnigraph-compiler/src/schema/schema.pest:8`)
as `SchemaApplyRequest.schema_source` is today. Names and ids follow RFC
0055's Grammar (`ident` or `string_lit`, decoded by
`decode_string_literal`, `crates/omnigraph-compiler/src/error.rs:60-90`,
and then refused by RFC 0055's name check when empty, whitespace-edged, or
carrying a control character). A
statement binds no `$vars`; `param_list` belongs to `query_decl`
(`query.pest:10-14, 35-36`). A file is a list of `query` declarations or
exactly one control statement.

### AST and classification

RFC 0055 turns `QueryFile` into an enum, `Queries(Vec<QueryDecl>)` and
`Branch(BranchStmt)`, and enumerates every `.queries` consumer that
becomes a `match`. This RFC widens the second variant:

```rust
pub enum QueryFile { Queries(Vec<QueryDecl>), Control(ControlStmt) }

pub enum ControlStmt {
    Branch(BranchStmt),                                                   // RFC 0055, unchanged
    Load { mode: LoadMode, branch: Option<String>, from: Option<String>, rows: String },
    SchemaShow,
    SchemaApply { source: String, allow_data_loss: bool, actor_provenance: Option<bool> },
    SchemaPlan { source: String },
    Snapshot { branch: Option<String> },
    CommitList { branch: Option<String> }, CommitShow { id: String },
    CommitChanges { id: String, page: PageClause, filter: FeedFilter },
    ChangesPoll { branch: Option<String>, start: PollStart, page: PageClause, filter: FeedFilter },
    BlobStat { entity: BlobEntity, type_name: String, id: String, property: String, target: Option<ReadTargetSpec> },
    QueriesList,
    Optimize, RebuildFullTextIndexes { branch: Option<String> },
    Repair { confirm: bool, force: bool },
    Cleanup { keep: Option<u32>, older_than: Option<Duration>, confirm: bool },
}

impl ControlStmt {
    pub fn is_write(&self) -> bool       // Branch(write), Load, SchemaApply, the four maintenance kinds
    pub fn statement_name(&self) -> &str // the leading words: "load", "schema apply", "commit changes"
    pub fn not_a_declaration_message(&self) -> String  // "<name> is a control statement, not a query declaration"
}
```

`Branch(BranchStmt)` keeps RFC 0055's type, `is_write`, and
`statement_name`; whichever phase 1 lands first introduces
`QueryFile::Control`, and the other renames or extends it (Rollout, phase
1). `not_a_declaration_message` is the one
method RFC 0055 does not need: its text names branches, and this RFC's
phase 1 carries the general string for every variant, the `Branch` one
included, replacing RFC 0055's branch-specific text when that phase landed
first.

Every consumer RFC 0055 lists matches on `QueryFile` once either phase 1
has landed, and its
`Control` arm is the arm it has for `Branch`: `find_named_query`
(`crates/omnigraph-compiler/src/query_input.rs:262`),
`select_named_query_decl` and `select_named_query` in the server
(`crates/omnigraph-server/src/handlers.rs:2512`, `:2531`),
`select_named_query` in the CLI
(`crates/omnigraph-cli/src/helpers.rs:846`), the logic-test runner, the
cluster loader's two sites, `lint_query_file`
(`crates/omnigraph-compiler/src/query/lint.rs:116`),
`QueryRegistry::from_specs` (`crates/omnigraph-server/src/queries.rs:103`),
and the `decl` test helper (`crates/omnigraph-gqt/src/tests.rs:515`).
There is no typechecker or lowerer arm:
`typecheck_query_decl`, `typecheck_query`
(`crates/omnigraph-compiler/src/query/typecheck.rs:98, 110`), and
`lower_query` (`crates/omnigraph-compiler/src/ir/lower.rs:34-38`) take a
`QueryDecl` and never see the file, as RFC 0055's Decision log records.
The arm refuses wherever the consumer wants a declaration, and dispatches
in the runner, the CLI, and the server. The compiler classifies and never
refuses a door.

### Server dispatch

RFC 0055's Server dispatch holds: `run_query` and `run_mutate` parse and
classify before any target resolution or Cedar check, so only the
statement's own action runs; the wrong-door, request-target, `name` and
`params`, expected-head, and deprecated-route refusals fire on
`QueryFile::Control` as on `Branch`; `ReadDispatch` gains one variant per
control-read family beside `BranchList`, and `Door` keys the same table.
Each handler body a statement reuses is split from its axum shell as RFC
0055 splits the branch handlers, so the `PolicyRequest`, the admission
check, the engine call, and the error mapping are one piece of code:

| Statement | Handler body (`handlers.rs`) | Cedar `PolicyRequest` | Engine call |
|---|---|---|---|
| `load` | `server_load_ndjson` `:1982` split into a body: the `main`/`merge` defaults, `authorize_load_scope` `:1791`, admission on `data.len()`, the engine call, `graph_batch_load_receipt_output` `:2045`. The media-type check (`:1996-2006`) and the Content-Length precheck (`:2008-2026`) stay in the axum shell, since a statement carries its payload in the JSON body | `Change`, `branch: Some(branch)`, `target_branch: None` (`:1830-1832`); when `from` is given and the branch is new, first `BranchCreate`, `branch: Some(from)`, `target_branch: Some(branch)` (`:1819-1821`); before both, the 404 for a missing branch with no `from` (`:1812`) | `db.load_graph_batch_as_with_receipt(&branch, from, rows, mode, actor)` (`:2042`; `loader/mod.rs:261-278`) |
| `schema show` | `server_schema_get` `:1668` | `Read`, `branch: None`, `target_branch: None` (`:1676-1678`) | `db.accepted_schema()` (`:1681-1685`), which answers `(String, SchemaIR)` |
| `schema apply` | `server_schema_apply` `:1717` | `SchemaApply`, `branch: None`, `target_branch: Some("main")` (`:1734-1736`); then the cluster-backed 409 (`:1739-1749`) | `db.apply_schema_as_with_catalog_check(source, SchemaApplyOptions { allow_data_loss, actor_provenance }, actor, validate_registry_against_catalog)` (`:1765-1774`), never `apply_schema_as`, which passes `\|_\| Ok(())` (`crates/omnigraph/src/db/omnigraph.rs:1070`): the registry check is part of the body, so a statement is refused for a schema that breaks a stored query exactly as the route is |
| `schema plan` | new body; no route exists, and the served door refuses it (Design, Server dispatch, below) | on the embedded arm no Cedar runs, as for every embedded statement; a later decision that serves it takes `schema show`'s pair, since a plan publishes nothing and reads only what `schema show` discloses | `db.plan_schema_with_options(source, SchemaApplyOptions::default())` (`omnigraph.rs:1024-1030`) |
| `snapshot` | `server_snapshot` `:527` | `Read`, `branch: Some(branch)`, `target_branch: None` (`:537-539`) | `db.snapshot_of(ReadTarget::branch(branch))` (`omnigraph.rs:1815`) and `db.internal_schema_version_of(ReadTarget::branch(branch))` (`handlers.rs:548-551`) |
| `commit list` | `server_commit_list` `:2421` | `Read`, `branch: Some(branch)`, `target_branch: None` (`:2434-2436`) | `db.list_commits(Some(branch))` (`omnigraph.rs:3606`) |
| `commit show` | `server_commit_show` `:2478` | `Read`, `branch: None`, `target_branch: None` (`:2487-2489`) | `db.get_commit(id)` (`omnigraph.rs:3584`) |
| `commit changes` | `server_commit_changes` `:2814` | `Read`, `branch: Some(branch)`, the branch resolved from the commit before the check (`:2837-2845`); a denial collapses to the 404 an unknown commit gives (`:2843-2853`) | the body's own paging |
| `changes poll` | `server_changes_feed` `:3026` | `Read`, `branch: Some(branch)`, `target_branch: None` (`:3038-3040`) | the body's own paging |
| `blob stat` | `server_blob_head` `:814` through `read_blob_for_delivery` `:837` | `Read` on the policy branch through `resolve_authorized_read_target` (`:842`, `:1205-1207`; a snapshot resolves to its branch first, `:1193`) | the body's own |
| `queries list` | `server_list_queries` `:1626` | `Read`, `branch: Some("main")`, `target_branch: None` (`:1634-1636`) | the registry |
| the four maintenance statements | Design, Maintenance | Design, Maintenance | Design, Maintenance |

A control read fills `ReadOutput { query_name, target, row_count,
columns, rows, graph_commit_id }`
(`crates/omnigraph-api-types/src/lib.rs:285-300`, where `rows` is a
`Box<RawValue>` since
[#627](https://github.com/ModernRelay/omnigraph/pull/627) and serializes
exactly as it did before), plus the optional `page` this RFC adds (below),
with `query_name` the statement's leading words,
`target` the branch or snapshot it read (`{ branch: null, snapshot: null }`
for the scope-free kinds, the value RFC 0055 introduces for `branch list`),
and rows projected per family from the route's response type, which is
unchanged. A row is the route type's serde JSON, one object per row, with
nulls and absences exactly as that type writes them: `native_dataset_branch`
serializes as `null`, while `before`, `after`, `size`, `etag`, `uri`,
`description`, and `parent_commit_id` are absent keys when unset
(`api-types:114`, `:469-472`, `:772-782`, `:919-931`, `:479-482`). The one
exception is the `blob stat` row, which drops `BlobStatOutput`'s
`selector` and spells its `target` as `resolved_target`, for the reason
the projection table gives.

| Family | One row per | Row identity key | Row order | Columns |
|---|---|---|---|---|
| `schema show` | the graph | the graph; one row always | one row | `schema_source`, `accepted_schema` (object, absent on older servers) (`SchemaOutput`, `:1024-1033`) |
| `schema plan` | migration step | position in the plan | plan order, the planner's | `kind` plus the union of every variant's field names, a row omitting the keys its variant lacks, which is what serde's `tag = "kind"` writes (`SchemaMigrationStep`, twelve variants, `crates/omnigraph-compiler/src/catalog/schema_plan.rs:54-155`) |
| `snapshot` | dataset | `entity_kind`, `type_name` | dataset order, the manifest's | `entity_kind`, `type_name`, `dataset_path`, `published_dataset_version`, `native_dataset_branch`, `entity_count` (`SnapshotDatasetOutput`, `:109-116`) |
| `commit list`, `commit show` | commit | `graph_commit_id` | newest first (`omnigraph.rs:3599-3606`); one row for `commit show` | `graph_commit_id`, `graph_branch`, `graph_manifest_version`, `parent_commit_id`, `merged_parent_commit_id`, `actor_id`, `created_at` (`CommitOutput`, `:387-397`) |
| `commit changes` | entity change | `graph_commit_id`, `kind`, `type`, `id`, `op`, since one entity carries insert, update, and delete rows in one commit (`:404-405`) | the frozen per-entity rank, insert before update before delete (`:404-405`) | `kind`, `type` (object), `id`, `op`, `before` (object), `after` (object) (`EntityChangeOutput`, `:464-473`), plus the cause's `graph_commit_id`, `parent_commit_id`, `merged_parent_commit_id`, `authored_branch`, `actor_id`, `authored_at` (`ChangeCauseOutput`, `:477-492`) |
| `changes poll` | entity change | the `commit changes` key | first-parent block order, then the frozen per-entity rank | the `commit changes` columns, the cause taken from the change's block (`ChangeBlockOutput`, `:507-510`) |
| `blob stat` | the cell | the four statement words | one row | `kind`, `size`, `etag`, `uri`, `resolved_target` (object: `branch?`, `snapshot?`, `resolved_snapshot`, `BlobResolvedTargetOutput`, `:749-755`) (`BlobStatOutput`, `:772-782`). The row column is named `resolved_target`, not `target`, because the envelope's `target` is a `ReadTargetOutput`; the statement's own four words are not repeated as a `selector` column |
| `queries list` | catalog entry | `name` | byte order of `name`, the registry's `BTreeMap` (`crates/omnigraph-server/src/queries.rs:65`) | `name`, `tool_name`, `description`, `instruction`, `mutation`, `params` (list) (`QueryCatalogEntry`, `:919-931`) |

The envelope beside the rows, per family:

| Family | `target` | `graph_commit_id` | `page` |
|---|---|---|---|
| `schema show`, `schema plan`, `commit show` | `{ branch: null, snapshot: null }` | absent, the read pins no branch head | absent for the first two; `schema plan` carries `supported` (`SchemaMigrationPlan`, `schema_plan.rs:48-51`) |
| `snapshot` | `{ branch, snapshot: null }` | the branch head the snapshot was taken at | `graph_branch`, `graph_manifest_version`, `internal_schema_version` (`SnapshotOutput`, `:119-126`) |
| `commit list` | `{ branch, snapshot: null }` | the branch head | absent |
| `commit changes` | `{ branch, snapshot: null }`, the branch resolved from the commit | the branch head | `next_page_token` when a further page exists (`CommitChangesOutput`, `:496-503`) |
| `changes poll` | `{ branch, snapshot: null }` | the branch head | `next_page_token`, `cursor`, `caught_up` as the route answers them (`ChangeFeedOutput`, `:514-528`) |
| `blob stat` | the resolved read target, branch or snapshot | the branch head when the target is a branch | absent |
| `queries list` | `{ branch: "main", snapshot: null }`, the scope the read is authorized on | absent, the registry is not a branch read | absent |

Page state travels in `page`, an optional object on `ReadOutput` skipped
when the family has none (Alternatives 7 becomes the design; Unresolved
questions, Q3). This is what lets an empty page still carry state: a
terminal poll answers zero blocks and still returns the cursor the client
must keep, because `change_feed_output` maps an at-block-boundary result to
`(None, Some(cursor), Some(caught_up))` unconditionally (`api-types:1496-1511`;
`crates/omnigraph/src/changes/model.rs:262-271`), and the default start
`now` (`handlers.rs:3055-3058`, "a missing cursor is never an implicit
beginning") answers zero blocks plus that cursor on a client's first poll.
An empty page may still carry `columns`. Under RFC 0051's Arrow `Accept`,
a family whose projection has an object or list column (`schema show`,
`commit changes`, `changes poll`, `blob stat`, `queries list`, `schema
plan`) answers 406 with the `ErrorOutput` body: those columns have no
stated Arrow type, and giving them one is that RFC's extension, not this
one's. That is a second 406 cause this RFC adds beside the one RFC 0051
defines, an `Accept` list naming neither form
(`docs/rfcs/0051-json-output-via-arrow.md:78`). The same 406 covers a
family carrying `page`, whatever its columns are: RFC 0051 travels the
envelope's fields as response headers and gives `page` none, so an Arrow
answer would drop the page state, and `omnigraph query --format arrow`
carries no `page` for the same reason. Both causes hold until RFC 0051
gives `page` a header spelling.

A control write fills `ChangeOutput { branch, query_name, affected_nodes,
affected_edges, actor_id, commit }` (`:329-336`) and the `outcome` field
RFC 0055 adds, tagged by `kind` in `snake_case`; the branch family keeps
its three kinds. `load` is the one control write with counts:
`affected_nodes` and `affected_edges` are the sums of `entities_loaded`
over `nodes` and over `edges` (`GraphBatchDeclarationOutput`,
`:362-365`), and `commit` is the receipt's commit; every other control
write reports both counts as `0`, as RFC 0055 rules for the branch family.

`branch` is a required `String` on `ChangeOutput`, so every kind states
it: the statement's branch for `load` and `rebuild full text indexes`,
`main` for `schema apply` (the route's target branch,
`handlers.rs:1734-1736`) and for `optimize`, `repair`, and `cleanup`,
which act on every dataset of the graph rather than on a branch. On
`optimized`, `repaired`, and `cleaned` that `main` is a placeholder and
not a scope, and a client reads the scope from `outcome.kind`. `commit`
is the head after the effect where one graph commit exists and `null`
otherwise: `loaded` carries the receipt's commit; `schema_applied` carries
the head of `main` after the apply, read as the first entry of
`list_commits(None)`, which is newest first (`omnigraph.rs:3599-3606`),
and `null` when `applied` is false; `rebuilt` carries
`FullTextIndexRebuildResult.graph_commit_id` and is `null` when nothing
needed rebuilding (`table_ops.rs:46-52`); `optimized`, `repaired`, and
`cleaned` are `null`, since they publish manifest versions and not one
graph commit.

| `kind` | Fields | Source |
|---|---|---|
| `loaded` | `mode`, `base_branch`, `branch_created`, `total_entities`, `nodes`, `edges` | `GraphBatchLoadOutput` (`:369-384`) minus `branch`, `actor_id`, `commit`, which `ChangeOutput` carries. `mode` is lower-case `overwrite`, `append`, or `merge` (`LoadMode`, `loader/mod.rs:66-75`). `entities_loaded` counts payload rows, not new entities (`loaded_count = batch.num_rows()`, `loader/mod.rs:728, 744, 784, 799`), so a `merge` that upserts an existing key counts; a declared type with no row in the payload gets no entry at all, since the map is written only for types the payload names |
| `schema_applied` | `applied`, `step_count`, `graph_manifest_version`, `steps` | `SchemaApplyOutput` (`:1013-1021`) minus `uri`, the graph the caller addressed, and minus `supported`, which is a constant `true` on the wire: both `SchemaApplyResult` constructors set it (`schema_apply.rs:301-307`, `:1262-1264`) and an unsupported plan is the route's error, not an answer (`plan_schema_for_apply` returns `Err` when `!plan.supported`, `:172-179`) |
| `optimized` | `datasets`, each `type_key`, `fragments_removed`, `fragments_added`, `committed`, `skipped`, `published_dataset_version`, `lance_head_version`, `pending_indexes` | `DatasetOptimizeStats` (`crates/omnigraph/src/db/omnigraph/optimize.rs:105-131`), mirrored into `omnigraph-api-types` as a wire twin, since the engine type is `#[non_exhaustive]` (`:106`). `skipped` is `null` or `drift_needs_repair` (`SkipReason::as_str`, `optimize.rs:70-87`); a `pending_indexes` element is `PendingIndex { type_key, property, reason }` (`table_ops.rs:1661-1665`), the engine struct the twin mirrors, where the verb's `--json` render drops `type_key` (`main.rs:1388-1391`); there is no `error` field, which is `cleaned`'s |
| `rebuilt` | `rebuilt_indexes`, each `type_key`, `property` | `FullTextIndexRebuildResult` (`table_ops.rs:46-52`) and `RebuiltFullTextIndex` (`:54-59`); `rebuilt_indexes` may be empty, and then `commit` is `null` |
| `repaired` | `confirm`, `force`, `graph_manifest_version`, `datasets`, each `type_key`, `published_dataset_version`, `lance_head_version`, `classification`, `action`, `operations`, `error` | `RepairStats` (`crates/omnigraph/src/db/omnigraph/repair.rs:110-116`) and `DatasetRepairStats` (`:95-107`), both `#[non_exhaustive]` (`:96`, `:111`), so mirrored as a wire twin exactly as `optimized` is. `classification` is one of `no_drift`, `verified_maintenance`, `suspicious`, `unverifiable` (`:26-49`); `action` is one of `no_op`, `preview`, `healed`, `forced`, `refused` (`:60-84`). `confirm` and `force` are echoed as the verb's `--json` echoes them (`main.rs:1503-1518`), because a preview and a `confirm` run over zero datasets, or over datasets that are all `no_op`, are otherwise byte-identical |
| `cleaned` | `keep_versions`, `older_than_secs`, `datasets`, each `type_key`, `bytes_removed`, `old_versions_removed`, `error` | `DatasetCleanupStats` (`optimize.rs:171-177`), with the policy echoed as the verb's `--json` echoes it (`main.rs:1619-1630`) |

Errors map exactly as the route maps them, through the shared body: a
loader refusal is the 400 `POST /load/ndjson` answers; an unsupported
migration is `POST /schema/apply`'s 400, because the engine returns an
error rather than an answer carrying `supported: false`
(`schema_apply.rs:172-179`); a `schema apply` on a cluster-backed server
is the 409 in the refusal table.

`schema plan` is embedded-only in this RFC. RFC 0011's capability table
marks the verb `direct` ("no server form exists", `0011:278`) and its
Decision 11 excludes it by name (`0011:533-535`); serving it would change a
recorded capability word, which this RFC declines to do in passing. The
verb keeps `direct`, RFC 0011's capability table is unchanged, and the
served door answers the `schema plan` row of the refusal table. Whether a
served `schema plan` is worth a capability change is left to a later
decision (Unresolved questions, Q4); on a cluster-managed graph the
authoritative preview would remain `cluster plan`'s, per RFC 0011
Decision 11.

### CLI

The verbs are unchanged. `omnigraph query` and `omnigraph mutate` classify
the source of `-e`/`--query-string` and `--query <file>` before sending,
as RFC 0055's CLI section rules, and post a `QueryFile::Control` with no
request target to `POST /query` or `POST /mutate`. The embedded arm
(`--store`, a URI) dispatches a control statement to the engine call the
verb's embedded arm makes today (`crates/omnigraph-cli/src/client.rs:370,
423, 454, 500, 567, 722-723, 1157-1166, 1338`; `main.rs:880, 882, 919-943,
1376, 1431-1432, 1495-1496, 1618`, `cleanup` on a `mut` handle). No Cedar
runs in that arm for any statement kind, because none runs there today:
every embedded arm opens the handle bare through `Omnigraph::open` or
`Omnigraph::open_read_only` (`open_embedded`, `client.rs:318-323`;
`main.rs:880` for `schema plan`), and neither constructor installs a policy
(`omnigraph.rs:625-626`; only `with_policy`, `:865-868`, does), so
`enforce` returns `Ok` (`:929-931`). `queries list` has no embedded arm
(the verb is `control`, `planes.rs:265`) and is refused with `--store` by
the verb's scope check (`planes.rs:403-411`); the statement's own refusal
is `queries list needs a server`. Until phase 5 the maintenance statements'
embedded form is primary: `omnigraph mutate -e 'optimize' --store
./graph.omni` is the statement spelling of `omnigraph optimize
./graph.omni`. The text renderer is the verbs': a `ChangeOutput` carrying
`outcome` prints the verb's human line for that kind; a control read prints
through the six `ReadOutputFormat` variants
(`crates/omnigraph-cli/src/read_format.rs:11-19`). The verbs render no GQ text of their
own, which is the shape `docs/dev/invariants.md:108-109` rejects for SQL,
"ad-hoc SQL or `IN (...)` string generation where structured expressions or
SIP apply", read here as the analogy it is.

Capability words classify verbs, not statements (`command_capability`,
`planes.rs:227-237`, derived from `command_plane`, `:243-282`), and a
statement rides `query`'s and `mutate`'s `Any`. So the maintenance
operations gain a served spelling in phase 5 while their verbs keep
`direct`, and `flag_applies` keeps rejecting `--server` on a `Direct` verb
(`:152`). No verb's capability word changes in this RFC. A verb-level
served arm, `omnigraph optimize --server`, is the Decision 11 mechanism
RFC's `planes.rs` and `cli.rs` change, not this one's.

### Logic tests (RFC 0045 amendment)

RFC 0055's amendment to RFC 0045 carries every family: a control write is
a `--- mutate` step body, a control read a `--- query` step body, the
compiler classifies, the wrong kind is refused with RFC 0055's two
strings, a statement step refuses `branch: <name>` on its header and a
following `--- params`, and new expect words are blessed from a run. This
RFC adds four sentences and no directive:

1. `affected: nodes=<N> edges=<M>` is accepted on a `load` step, asserting
   the two `ChangeOutput` counts; on every other control write it stays
   refused (`affected: is accepted on load only`).
2. `outcome: <word>` is accepted on a `schema apply` step with `<word>`
   one of `applied`, asserting `outcome.applied` is true, and `unchanged`,
   asserting `outcome.applied` is false and `outcome.steps` is empty for a
   schema identical to the accepted one; on `branch merge` it keeps RFC
   0055's three words; on
   every other step it stays refused. There is no `unsupported` word: an
   unsupported migration is the engine's error, and a case pins it with
   `error: <substring>` (`schema_apply.rs:172-179`). `load` and the
   maintenance statements take `ok` or `error: <substring>`; their
   statistics are not a test subject, the rows a following `--- query`
   reads are. `ok` on `cleanup` and on `repair` accepts per-dataset
   `error` and `action: refused`, since the engine returns `Ok` with them
   set; a case that must see none reads the statistics through a following
   `--- query`.
3. A raw block inside a step body is one token: the runner's `--- ` line
   refusal (`0045:368-370`) and its `${` refusal (RFC 0055, Logic tests)
   apply to the block's text as to any body text, and neither NDJSON nor
   `.pg` needs either sequence.
4. A control-read step carries the shape section as every rows step does:
   [#635](https://github.com/ModernRelay/omnigraph/pull/635) made
   `--- expect shape` mandatory directly after every rows expect
   (`crates/omnigraph-gqt/src/lib.rs:742`; `0045:398-425`). Its lines are
   the family's column names with the `.pg` type of each column. A shape
   line takes a `.pg` property type
   (`crates/omnigraph-gqt/src/shape.rs:96-105`, `parse_type` round-trips
   through `parse_schema`), and `.pg` has no object or list type, so an
   object or list column is refused there and `schema show`, `commit
   changes`, `changes poll`, `blob stat`, `queries list`, and `schema
   plan` take `error:` only until RFC 0045 admits a nested type. The computed check
   against the compiler's inferred schema is skipped for a control read,
   which has no `QueryDecl` to infer one from.

`ordered` is accepted on every control read whose row order the projection
table states and `unordered` otherwise, as RFC 0055 exempts `branch list`
from `ordered_refusal` (`lib.rs:614`). `queries list` is refused inside a
step with `queries list needs a server`, the statement's refusal (the
verb's is the scope check's, `planes.rs:403-411`), because the
runner executes against an embedded handle and that family has no embedded
arm. On a statement step an `error:` substring pins the engine's message:
the runner runs the engine call and not the handler body, so the refusal
table's server strings, the load 404, the cluster-backed 409, and the `not
served yet` 400 are unreachable from a case. No case can learn a commit
id, because a statement body binds no `${}` and no step binds a value from
a prior step, so `commit show`, `commit changes`, `commit list`, and
`changes poll` have no green case under this RFC (a commit id and a
timestamp sit in every row of the last two); an RFC 0045 amendment that
binds a prior step's value is what would unblock them.

The seed section is unchanged and keeps the compatibility shape
(`load_jsonl`, `crates/omnigraph-gqt/src/lib.rs:1560`); a `load` step
carries the strict shape, so a case holds two row grammars, setup and
operation under test, until a later RFC 0045 amendment moves the seed
(Unresolved questions, Q5). The runner executes every statement against
its embedded handle with no policy (RFC 0055, Logic tests). `cleanup`
needs the handle mutably: `Omnigraph::cleanup` takes `&mut self`
(`omnigraph.rs:2929-2934`) through `cleanup_all_datasets(db: &mut
Omnigraph, ..)` (`optimize.rs:1159-1162`), and `execute_case` owns it that
way (`lib.rs:1573`), so `run_mutate_step`, which takes `&Omnigraph` today
(`lib.rs:1486-1487`), takes the handle mutably.

```
--- mutate
load merge $$
{"type":"Person","data":{"name":"ada"}}
$$

--- expect affected: nodes=1 edges=0

--- mutate
optimize

--- expect ok

--- query
query after_optimize() {
    match { $p: Person }
    return { $p.name }
}

--- expect unordered
{"p.name": "ada"}

--- expect shape
p.name: String
```

Guarantee: an older harness refuses a case using any of the four
sentences (`affected:` on a statement step, an unknown `outcome:` word, a
shape section on a statement step, a statement body as `does not parse`,
`lib.rs:790-795`) and never mis-runs
it, by the refusals RFC 0055 names.

### Maintenance

The four maintenance operations have no route today and, for three of
them, no Cedar action: `optimize` (`omnigraph.rs:2915`), `repair`
(`:2922`), and `cleanup` (`:2929`) take no actor and call no `enforce`,
while `rebuild_full_text_indices_on_as` (`:2888-2894`) "Requires `Change`
on the selected branch before any effects" through `db.enforce(Change,
Branch(public_branch), actor)` (`table_ops.rs:81-85`). RFC 0011 Decision
11 commits `optimize`, `cleanup`, and healthy-path `repair` to
"server/cluster-managed async jobs", "policy-gated, audited,
single-coordinator", run "never inline in serving", with the mechanism in
a follow-up RFC (`docs/rfcs/0011-cli-addressing-and-config.md:527-538`).
This RFC composes with that decision in three parts.

The statement is the language form, defined here for all four, and it
executes embedded now: the runner and `omnigraph mutate --store` call
`optimize`, `repair`, `cleanup`, and `rebuild_full_text_indices_on_as`
exactly as the verbs do. Embedded is where these operations run today, so
no operational boundary moves in phase 4.

Bare `repair` publishes nothing (`RepairOptions { confirm: false }`,
`main.rs:1495-1496`, and a drifted dataset takes `action: preview`, one
with no drift `no_op`, `repair.rs:223`, `:236`) and is
still a control write, because an operation's door is its committed form's
door: `repair` and `repair confirm` are one operation and enter together at
`POST /mutate`. `schema plan` is a control read
because it is an operation of its own with no committing form; `schema
apply` is a different operation. The rule keeps
one door per operation, at the cost that one statement at the write door
publishes nothing.

The action. `PolicyAction` (`crates/omnigraph-policy/src/lib.rs:16-74`)
gains `Maintain`, policy-file name `maintain`, resource kind `Graph`, no
branch and no target-branch scope (`uses_branch_scope`,
`uses_target_branch_scope`, `:92-101`, both false, so `validate()` refuses
a scoped rule naming it, `:352, :363`), and a Cedar schema line `action
"maintain" appliesTo { principal: Actor, resource: Graph, context:
RequestContext }` beside the ten that exist (`:845-855`). `Admin` is not
reused: it is reserved for policy-management surfaces and has no call site
outside tests (`:26-41`; the one use is an assertion,
`crates/omnigraph-server/src/data_tokens/tests.rs:338`). The engine gains
`optimize_as`, `repair_as`, and `cleanup_as`, each calling
`enforce(Maintain, ResourceScope::Graph, actor)` before any effect
(`ResourceScope::Graph`, `crates/omnigraph-policy/src/lib.rs:922-926`);
these are the engine's first `Graph`-scoped `enforce` calls, which is a
claim this RFC makes on purpose, since the scope exists today "if any ever
go through enforcement" (`:923-924`). The actor-less `optimize`, `repair`,
`cleanup` become their `actor: None` wrappers, the shape `apply_schema`
and `apply_schema_as` have (`omnigraph.rs:1040, 1064-1072`). `rebuild full
text indexes` keeps `Change` on the branch, RFC 0043's decision, and adds
no action. A `maintain` rule names the graph, not a branch, because
compaction, drift repair, and version cleanup act on every dataset of the
graph (`optimize.rs:105-131`, `repair.rs:95-116`). The action touches the
enum, `as_str`, the two scope predicates, `resource_kind`, `FromStr`
(`:161-179`), the Cedar schema string, the crate's tests, and the action
tables in `docs/user/operations/policy.md:11-21` and
`skills/omnigraph/references/server-policy.md:67-75`; no CLI edit
(`policy explain --action` derives from `PolicyAction` through
`ValueEnum`, `cli.rs:960-961`); no cluster edit.

The mechanism RFC inherits `maintain` (graph, unscoped) as the submit gate
and `Change` on the branch as `rebuild`'s, a two-action family by RFC
0043's decision, and may not rename or rescope either. `maintain` is also
not a grantable
data-token action: RFC 0053's grant grammar admits `read`, `export`,
`change`, `branch_create`, `branch_delete`, `branch_merge`,
`invoke_query`, and `graph_list` only (`0053:128-133`), and what that
costs a served maintenance job is stated in Compatibility, Policy.

The served form. On a server a maintenance statement is a job: `POST
/mutate` authorizes `PolicyRequest { action: Maintain, branch: None,
target_branch: None }` (`Change` on the branch for `rebuild`), admits the
write, and submits the job to the worker Decision 11's mechanism RFC
defines. That worker, the job record, the job routes, the served answer
kind, and any statement that observes a job are that RFC's; this one fixes
the statement's spelling, door, and action, and nothing about the answer.
Three constraints bind it. The job executes inside the single
mutation-capable writer process ("one mutation-capable writer process
unless an external fence proves exclusivity",
`docs/dev/control-plane.md:71`; `docs/user/clusters/index.md:162-164`),
never a second writer process unless that RFC supplies the external fence
those documents require; RFC 0035, a draft, names this seat `Write`
("Future Blob/maintenance writers join `Write`",
`docs/rfcs/0035-served-operation-ownership.md:226`). `repair confirm force` has no
served form at all: forced repair publishes suspicious or unverifiable
drift (`docs/user/operations/maintenance.md:106-109`), and RFC 0011
Decision 11 keeps `direct` "as break-glass (`repair` when the server is
down)" (`0011:527-531`), so a served `repair confirm force` is refused
(the row in the refusal table) while the grammar keeps `force` for the
embedded arm. And `cleanup` is `&mut self` (`omnigraph.rs:2929-2934`,
through `cleanup_all_datasets(db: &mut Omnigraph, ..)`,
`optimize.rs:1159-1162`) while the server holds `GraphHandle.engine:
Arc<Omnigraph>` with no `RwLock`
(`crates/omnigraph-server/src/registry.rs:44-46`), so a served job runs it
on a handle the worker owns and not on `GraphHandle.engine`; whether
`cleanup_as` becomes `&self` is the mechanism RFC's decision, not this
one's.

Until phase 5 the served door refuses all four statements with the interim
message and the maintenance verbs stay `direct` (`cli.rs:20-21`). `rebuild
full text indexes` stays in that refusal too, and not because it lacks a
Cedar action: RFC 0043 chose "direct graph storage, not a new HTTP
maintenance endpoint" (`docs/rfcs/0043-full-text-index-compatibility.md:49-50`)
for the reason RFC 0010 gave, an inline destructive surface, which a job is
not, and its quiescence rule, "stop the old serving/writing fleet ...
rebuild ... before resuming" (`0043:55-57`), becomes an obligation of the
job mechanism rather than of the statement. Until the mechanism RFC
accepts that obligation, `rebuild` is refused at the served door with the
other three. Phase 5 therefore adds `rebuild full text indexes` to
Decision 11's set, which names `optimize`, `cleanup`, and healthy-path
`repair` only, and that extension is the divergence recorded in the
precedent audit.

## Invariants

- 2, one graph-content publication door: a `load` or `schema apply`
  statement publishes through the handler body and engine entry its route
  publishes through.
- 4, a mutation publishes once: `load` stages every declaration and
  publishes one commit (`loader/mod.rs:261-278`), as the route does; a
  statement cannot share a file with a mutation body.
- 7, "Physical acceleration is derived state" (`invariants.md:54`), whose
  rule is that "expensive index work happens through explicit
  reconciliation, not inline on content writes" (`:57`): `rebuild full
  text indexes` and `optimize` are the explicit request, not a content
  write; no statement triggers index work as a side effect.
- 9, query semantics are typed structures: every statement is an AST
  variant, the door is chosen by matching on it, and the payload's
  grammar is the operation's own parser.
- 10, trust at the boundary, enforced at the engine: the server path
  authorizes with the route's own `PolicyRequest`; the three new `_as`
  entries apply the `maintain` gate whenever a policy is installed, which
  closes the one place a mutating engine entry lacked a gate; the embedded
  CLI and the runner install none today, unchanged.
- 13, "Evidence matches the boundary" (`invariants.md:88`), whose rule is
  "test the layer whose contract changed": grammar and AST in the
  compiler, dispatch and refusals in the server, the action in the policy
  crate, the format in the logic tests, parity rows per family (Evidence
  and tests).

Deny-list (`docs/dev/invariants.md:96-117`): no job queue "for state
derivable from accepted manifest state, where an idempotent reconciler
suffices" (`:101-102`), which is why this RFC adds no job queue; no
"synchronous vector or FTS rebuilds on a content-write path" (`:103`),
which a `rebuild` statement is not; no "ad-hoc SQL or `IN (...)` string
generation where structured expressions or SIP apply" (`:108-109`), the
same shape as a verb assembling GQ text, which no verb does here; no
side channel for query semantics (`:110`), which the file-level
classification honors. No known gap changes; the maintenance gap (three
entries with no gate) closes.

## Compatibility and reversibility

Wire. `outcome` gains new `kind` values; the field is skip-when-absent
(RFC 0055), so every existing mutation response is byte-identical.
`ReadOutput` gains one optional field, `page`, also skip-when-absent, so
every existing read response is byte-identical too. A row's nulls are the
route type's own serde output, keys present or absent exactly as that type
writes them (Design, Server dispatch), which is what a `.gqt` expect body
and every typed client pin. Under RFC 0051's Arrow `Accept` a family whose
projection carries an object or list column answers 406, and so does a
family carrying `page`; both causes are stated in Design, Server dispatch. Every route,
request type, and response type is unchanged. The deprecated `POST /read`
and `POST /change` refuse statements; `POST /ingest` is the deprecated
alias of `POST /load` (`lib.rs:1908-1912`) and carries a load body rather
than GQ, so it has no statement to refuse. Error precedence (a parse error
before a `Read` or `Change` denial on the inline routes) is RFC 0055's
change, owned in its Compatibility; this RFC adds no reorder.

Body limits. A raw block rides the door's limit: `POST /mutate` and `POST
/query` are capped by the router-wide `DefaultBodyLimit` at 1 MiB
(`lib.rs:164, 1963`); `/load` and `/ingest` carry a 32 MiB layer
(`lib.rs:165, 1905, 1917`); `/load/ndjson` carries no layer and enforces
the same 32 MiB cap inside `collect_graph_batch_body`
(`handlers.rs:1924-1948`), because it reads the raw request body. This is a stated
support boundary: an inline `load` is the agent's and the test's form; a
bulk load keeps `POST /load/ndjson` and `omnigraph load`. Raising the
`/mutate` cap for statements only would make the limit depend on the
body's content, which the router cannot see (Alternatives 3); a
server-side fetch is Alternatives 6.

Policy. `maintain` is additive on the wire and breaking in a bundle. A
bundle naming it is refused by an older CLI at `cluster validate` and
`cluster apply` (`policy_invalid`,
`crates/omnigraph-cluster/src/config.rs:975-998`), and an older server
that boots a published bundle naming it quarantines that graph
(`crates/omnigraph-server/src/lib.rs:2216-2218`, `:2144-2150`; startup
aborts entirely under `--require-all-graphs`, `:2154-2159`), so that graph
goes dark for every action and upgrade order is a constraint: upgrade
every server before applying a bundle that names `maintain`. The refusal
message is serde's, `unknown variant \`maintain\`, expected one of ...`,
because `PolicyAllowRule.actions` is a `Vec<PolicyAction>` deserialized by
the derive (`crates/omnigraph-policy/src/lib.rs:16-18, 195-202`); the
`unknown policy action` text of `FromStr` (`:161-179`) is the `policy
explain --action` path, not the bundle path. The graph-scoped `validate()`
rules (`:350-371`) are unchanged. A policy that grants `schema_apply`
today grants `schema apply` tomorrow; one that denies `change` on a branch
denies `load` on it.

Two gates, not one. RFC 0053 checks a credential's own action grant before
Cedar (`actor.permits_action`, `handlers.rs:407-412`), and a signed data
credential additionally requires an applied Cedar policy (`:413-417`). A
statement is subject to that ceiling exactly as its route is: `schema
apply` is refused for every data-token actor as `POST /schema/apply` is,
because the grant grammar refuses the whole token on `schema_apply`
(`0053:128-133`), and `maintain` is not a grantable action at all, so a
served maintenance job needs a static credential until RFC 0053 is
amended. This RFC does not amend that grant list. On the Cedar gate,
`maintain` is denied under DefaultDeny, where only `read` passes once
tokens are configured and no policy is applied
(`handlers.rs:455-463`), and under any applied bundle that does not name
it; under `--unauthenticated` it is open, exactly as `change` and
`schema_apply` are on such a server (`:419-424`), because that mode installs
no `PolicyEngine` at all. So a served maintenance job is exactly as
reachable as a mutation, in every server mode.

Cluster-backed serving. Every `omnigraph-server` process boots from a
cluster (`crates/omnigraph-server/src/settings.rs:231-233`) and
`config_path` is set for directory and URI boots alike (`settings.rs:206`;
`lib.rs:317-319`), while the 409 fires on `config_path.is_some()`
(`handlers.rs:1743`). So on every deployed server a `schema apply`
statement answers the route's 409 after the Cedar gate
(`handlers.rs:1739-1749`; `docs/user/operations/server.md:136`); its
served arm is exercised by the `AppState::new_single` constructors
(`lib.rs:402-435`, `config_path: None` at `:636`), which the server's
tests and `examples/bench_actor_isolation.rs` use, and is otherwise
unreachable. The inline
`.pg` body's working path is embedded: `omnigraph mutate --store` on a
standalone graph root, and the logic-test runner. A cluster-managed graph
keeps changing schema through `cluster apply` only ("Direct schema apply
and the server schema-apply endpoint refuse cluster-managed graphs",
`docs/user/schema/index.md:139-140`); the statement opens no second path
and adds no rule (Unresolved questions, Q4). RFC 0011's "Definitions are
named; payloads are passed" (`0011:54-56`) holds for cluster-managed
graphs by that 409: their schema stays a catalog definition. No statement
reaches `cluster.yaml`, the applied revision, or the graph registry.

Documentation. `docs/user/cli/reference.md:54` lists `schema apply` as
`direct`; the code classifies it `any` (`planes.rs:256-258`) with a served
arm posting `SchemaApplyRequest` to `POST /schema/apply`
(`client.rs:1142-1152`, which now also posts `actor_provenance`). The row
is corrected in phase 2. A `maintain` row lands in two action tables, not
one, `docs/user/operations/policy.md:11-21` and
`skills/omnigraph/references/server-policy.md:67-75`, so the phase-4 doc
edit touches both; they already disagree on one word, `graph_list` being
server-scoped in the first (`policy.md:23`) and cluster-scoped in the
second (`server-policy.md:77`), a pre-existing divergence this RFC
inherits by citing both and does not resolve.

Clients. Every client of `POST /mutate` becomes a load, schema, and (after
phase 5) maintenance client, as RFC 0055 says for branches: server-side
Cedar is the gate on both fronts, so no actor's permissions widen, and a
client that withheld these operations while exposing a `mutate` entry
point must classify the source it sends or accept the change. A client
whose `mutate` entry point always populates `ChangeRequest.branch`, which
a tool generated from the OpenAPI document does, cannot send any control
statement until it omits the request target for a statement body: the
request-target refusal is unconditional (RFC 0055), so such a client must
classify the source or expose a second, target-free entry point.

Logic tests: fail-closed per RFC 0045 (`0045:820-823`) and RFC 0055.
Storage: none. Reverting: remove the grammar rules and the `Control`
variants beyond `Branch` (the compiler enumerates every arm to delete),
the new `outcome` kinds, the `maintain` action, and the three `_as`
entries; statements in flight become parse errors; routes and verbs are
unaffected; `.gqt` cases using the families are refused and stay readable
as behavior records.

## Alternatives

1. **Do nothing.** Agents keep N routes times M clients; maintenance stays
   credential-gated with no Cedar decision and no audit; the loader,
   schema, and maintenance keep Rust-only regressions.
2. **Lifecycle on dedicated verbs, content in GQ.** Its strongest form:
   operations with different mandates (an operator's compaction, an
   agent's row edit) should be visible as different tools; the acceptance
   semantics of a schema change (review, approval, `allow_data_loss`) do
   not fit a statement that runs on arrival; and a control statement is
   non-parameterized, not storable, and not composable, a second spelling
   with less power than a stored query. Answers: mandate visibility lives
   in Cedar, where `maintain`, `schema_apply`, and `change` are three
   actions whichever door carries them; acceptance semantics are the
   control plane's, a cluster-managed schema changes only through `cluster
   apply` with its approval artifacts (RFC 0004) and the statement gets
   the route's 409 on every deployed server, while against a standalone
   graph root the embedded `schema apply` already runs on arrival;
   storability is a property of a reviewed declaration (RFC 0041), which a
   control statement is not by design (Alternatives 8). The cost of the
   Cedar answer is the one Clients names: a client that curates by tool
   must classify the source it sends or accept that its `mutate` tool
   carries control writes. What this alternative leaves unanswered is the
   Motivation's first cost: for an agent, verb-first is route-first. The
   evaluation in Evidence and tests tests this alternative as much as the
   RFC.
3. **One door for every statement** (out of scope; RFC 0055
   Alternatives 3 defers it). The constraint stands: a proxy that dispatches by
   route path without reading bodies must send every write to the writer,
   so control writes keep a mutation route path, and the body cap would
   have to depend on content. One door is a transport RFC of its own, and
   is out of scope here rather than open.
4. **The composition of three existing answers** (the simplest competitor
   overall, taken in its strongest form): per-operation directives in
   `.gqt` (`--- load`, `--- schema apply`, `--- optimize`, each a runner
   directive with its own argument grammar) for the test motive; RFC 0011
   Decision 11's job routes for the governance motive; the existing routes
   for agents, which RFC 0003 already wraps as typed MCP tools. Each part
   exists or is committed, and together they cover all three costs without
   a language change. What the composition does not give is one spelling:
   the runner owns a grammar the compiler does not, so every operation has
   two forms, one testable and one shippable, and a test never pins the
   text an agent runs; and the agent surface stays N routes times M
   clients, curated at the tool list rather than at the Cedar action. RFC
   0055 Alternatives 2 reaches the same verdict for branches. Against this
   composition the RFC's remaining claim is the one the Evidence
   evaluation tests, so the RFC stands or falls with it.
5. **Per-family dedicated routes taking `{"query": …}`** (RFC 0055
   Alternatives 8, generalized): `POST /load` accepting a `load` statement,
   `POST /schema` accepting `schema …`, a new `POST /maintain`. It removes
   the wrong-door, request-target, and interim maintenance refusals, at
   the cost of one route per family that every client must learn, the N
   times M surface again with GQ text inside it.
6. **Payload by reference, `load merge from <uri>`**, a server-side fetch.
   It lifts the 1 MiB boundary and admits payloads containing `$$`.
   Rejected for this RFC: a fetch is a new capability (credentials for the
   source, an external-source policy, a bounded reader) with no precedent
   in the server, and the inline form is what `-e` and `.gqt` need. A
   tagged block, `$tag$ … $tag$`, is the smaller answer to the `$$` case,
   reserved for a follow-up (Unresolved questions, Q2).
7. **Page state repeated as columns on every row**, which keeps
   `ReadOutput` at its current field list. Rejected, and this is the
   design's one envelope change: the columns ride on rows, so a page with
   no rows carries no state, and a terminal `changes poll` page carries
   exactly that shape, zero blocks plus the cursor the caller must keep
   (`change_feed_output`, `api-types:1496-1511`). A client polling from
   the default start `now` would never receive a first cursor and could
   never bootstrap, and a filtered poll that matches nothing would rescan
   forever. The optional `page` object is the smaller change: one
   skip-when-absent field, every existing read byte-identical, and the six
   `ReadOutputFormat` variants (`read_format.rs:11-19`) touched once
   (Unresolved questions, Q3).
8. **Parameterized or storable control statements**, `load merge $$ … $$
   on $branch` in the catalog, or a stored `cleanup`. Rejected: a stored
   query is a reviewed declaration invoked by name (RFC 0041), and a
   control statement names its targets literally by design (RFC 0055,
   Grammar); a `$branch` parameter reintroduces two sources for one fact,
   which the request-target refusal closes, and a stored `cleanup` is a
   reviewed way to destroy versions on demand.
9. **Served maintenance inline on `POST /mutate`**, compaction or cleanup
   running in the request. Rejected: it contradicts RFC 0011 Decision 11,
   "never inline in serving", and RFC 0010's reason for keeping
   maintenance off the wire applies to an inline form and not to a job.
10. **Maintenance out of scope entirely.** Rejected with the lean of
    Unresolved questions, Q1: without it the language covers content and
    lifecycle but not the operations an agent is told to request (the
    full-text 409) and a test cannot express (compaction before a read),
    and the governance motive is unmet.
11. **`export`, `changes baseline`, and `blob get` as statements.**
    Rejected: each answers with a byte stream or bytes (`handlers.rs:890,
    696`; the baseline stream's terminal record, `ChangeBaselineRecord`,
    emitted exactly once after every snapshot record,
    `crates/omnigraph-api-types/src/lib.rs:598-605`), a statement answers
    with rows, and an export is a read the language already expresses.
12. **No new Cedar action: `optimize`, `repair`, and `cleanup` under
    `change` on `main`**, as `rebuild full text indexes` already runs
    under `Change` on its branch (RFC 0043). This is the minus-one design
    for `maintain`, and it is cheaper: no enum variant, no Cedar schema
    line, no policy-doc edit, no bundle-upgrade order. The failing
    scenario: every row-writing actor holding `change` on `main` could
    then compact datasets, publish drift repairs, and destroy version
    history. `change` is the action granted to agents and to ordinary
    write clients; `maintain` is not, and that separation is the whole
    point of adding it.
13. **The payload as a string literal** rather than a raw block, using the
    existing `string_lit` and `decode_string_literal`. This is the
    minus-one design for the raw block: no new syntax at all. Rejected:
    every `"` and `\` in an NDJSON payload and every newline in a `.pg`
    source would have to be escaped, so `-e` strings and `.gqt` bodies
    stop being copy-pasteable from the files they came from, and the
    unescaped, byte-for-byte form is exactly what the atomicity argument
    above protects.

Out of scope, decided here rather than deferred: one door for every
statement is a transport RFC of its own (Alternatives 3), and `export`,
`changes baseline`, and `blob get` stay off the language (Alternatives
11). Neither is a question this RFC's acceptance turns on.

Precedent audit. In-repo: RFC 0055 is the pattern and every door,
refusal, and classification rule here is
its rule restated; RFC 0009's parity matrix takes every statement's two
arms as rows (`docs/rfcs/0009-unified-access-paths.md:84-91`); RFC 0011
Decision 11 is the maintenance direction this RFC composes with; RFC
0035's `Write` lane is where a served maintenance writer lands; RFC 0043
keeps rebuild explicit and `Change`-gated; RFC 0046 keeps `optimize` "the
only index reconciler" with no scheduler
(`docs/rfcs/0046-index-status.md:32-36`); and RFC 0054 is the most recent
accepted RFC touching the door this one restates, having changed four
surfaces this document mirrors: `SchemaApplyOptions.actor_provenance`
(`docs/rfcs/0054-default-actor-provenance.md:93-94`) and, in its
implementation ([#663](https://github.com/ModernRelay/omnigraph/pull/663),
`c253f111`), the `SetActorProvenance` step kind (`schema_plan.rs:57`),
`SchemaApplyRequest.actor_provenance` (`api-types:1009`), and
`SchemaOutput.accepted_schema` (`api-types:1032`). Two divergences from the
nearest pattern are named on purpose. The `maintain` action is added where
RFC 0055 adds none, justified in Design, Maintenance. And phase 5 puts
`rebuild full text indexes` behind a job, where RFC 0043 chose "direct
graph storage, not a new HTTP maintenance endpoint" (`0043:49-50`) and
Decision 11 names only the other three: 0043's objection is to an inline
destructive surface, which a job is not, and its quiescence rule
(`0043:55-57`) transfers to the job mechanism as an obligation, until
which `rebuild` stays refused at the served door. External, each per the project's
documentation and not verified against source: SQL puts `VACUUM`,
`REINDEX`, `COPY`, `CREATE TABLE`, and `EXPLAIN` in the language beside
`SELECT`; Dolt mirrors every CLI command as a `dolt_*` procedure
(`DOLT_GC`, `DOLT_BACKUP`), so a SQL client never needs a second protocol
([Version control in Dolt](https://www.dolthub.com/docs/sql-reference/version-control/));
Neo4j's Cypher carries `CREATE DATABASE`, `SHOW INDEXES`, and `LOAD CSV`;
Kuzu has `COPY FROM`; DuckDB has `COPY`, `CHECKPOINT`, and `PRAGMA`;
ClickHouse has `OPTIMIZE TABLE` and `SYSTEM`; PostgreSQL's dollar quoting
is the raw block's shape. The counter-shape: TerminusDB keeps lifecycle
in its CLI and API outside WOQL, and lakeFS and Neon expose branching
through API, CLI, and console only, the status quo this RFC leaves.

## Evidence and tests

Existing owners to extend:

- Compiler: parser tests beside `parse_query` for every statement, every
  default, every keyword boundary, a raw block with `//` and `/* */`
  inside it, `$$` inside a payload as a parse error of the trailing text,
  `force` without `confirm` refused, and a property named `load`,
  `commit`, or `schema` still parsing inside a body.
- Server: `crates/omnigraph-server/tests/data_routes.rs` for dispatch of
  every family, the refusal table, the body cap on a raw block, a
  cluster-backed `schema apply` answering the route's 409 body, and the
  CLI's exact request shape; `auth_policy.rs` for the per-family Cedar
  decision, `maintain` allowed and denied, a scoped `maintain` rule
  refused by `validate()`, and DefaultDeny refusing it; `openapi.rs` for
  the new `outcome` kinds.
- Policy: the crate's tests (`crates/omnigraph-policy/src/lib.rs:1029-1030`
  onward) for `maintain` in `as_str`, `FromStr`, `resource_kind`, and the
  Cedar schema.
- Engine: `optimize_as`, `repair_as`, `cleanup_as` with a policy
  installed, allow and deny; the existing maintenance tests keep their
  owners.
- CLI: `crates/omnigraph-cli/tests/cli_queries.rs` for `-e` statements per
  family on both arms and the three consent prompts; `parity_matrix.rs`
  (RFC 0009) gains one row per statement, verb against statement, both
  arms, `--json` diffed.
- Logic tests: `crates/omnigraph-gqt/tests/gq_logic_tests.rs` for the
  four amendment sentences, and first cases, one per family a case can
  express:
  `load_merge_upserts_by_key.gqt`, `schema_apply_add_property.gqt`,
  `schema_apply_drop_is_soft_without_allow_data_loss.gqt`,
  `optimize_preserves_rows.gqt`,
  `rebuild_full_text_after_analyzer_change.gqt`,
  `cleanup_keep_preserves_head.gqt`, `snapshot_lists_datasets.gqt`. The
  drop case is named for what happens, not for a refusal: without `allow
  data loss` the planner emits `DropMode::Soft` and only `allow_data_loss`
  promotes it to `Hard` (`schema_apply.rs:28-36, 124, 171`;
  `schema_plan.rs:117-127`), so the soft drop applies as a tombstone,
  answers `applied: true`, and a following `--- query` over the dropped
  type is refused as an unknown type, the `error:` substring the case
  pins, while the rows stay on the object store (a `schema show` step
  cannot pin it: its `accepted_schema` column takes `error:` only, Design,
  Logic tests). Thirteen cases are
  committed at this commit (`crates/omnigraph-gqt/cases/`), so none of the
  seven names collides. Every expect body is blessed from a run.

The agent-experience evaluation. The Motivation's first claim carries an
evidence obligation, which lives here, in Evidence: it is not a dependency
on another document, and phases 1 to 4 do not wait on it. Run by: the RFC
author, before phase 3 ships; its count table is a phase-3 deliverable.
The shape, fixed before the first run. At least three agent tasks against
a named development graph, each one loading rows, changing the schema,
reading history, and running a maintenance operation. Each task is run
twice, once with the statements and once with the routes and verbs only,
with the same model and the same prompt in both arms; the model is fixed
for the whole set and recorded by its exact id. Recorded per run: model
id, prompt, task, arm, the count of tool-selection mistakes (a wrong door,
a wrong route, a verb the agent cannot run) and the count of
error-handling mistakes (a refusal not acted on, a 409 not followed by the
action it names). The artifact is a transcript plus a count table checked
in beside the parity matrix
(`crates/omnigraph-cli/tests/parity_matrix.rs`), run by the same harness,
so every number in the Decision log is re-derivable from it. The
threshold: the statement arm makes no more mistakes than the route arm
across the task set. A result the other way withdraws the Motivation's
first claim, and the RFC is then carried, or not, by the governance and
test claims alone.

## Rollout

1. **Compiler families framework** (`omnigraph-compiler`,
   `omnigraph-cluster`, `omnigraph-server`, `omnigraph-cli`,
   `omnigraph-gqt`): `control_stmt` and every family rule, the raw block,
   `QueryFile::Control(ControlStmt)`, with RFC 0055's `Branch` variant
   moved under it when that RFC's phase 1 has landed first and introduced
   here otherwise, and the arms RFC 0055 enumerates, updated for the
   renamed variant:
   `find_named_query`, `select_named_query_decl`, `select_named_query`,
   the runner, the cluster loader's two sites, `lint_query_file`,
   `QueryRegistry::from_specs`, and the `decl` test helper. The
   rename from `Branch` to `Control` forces a match at every consumer
   above; in `omnigraph-server`,
   `omnigraph-cli`, and `omnigraph-gqt` the arm is a refusal until
   phases 2 to 4 replace it with dispatch. Phase 1 also rewords RFC 0055's
   four family-specific refusal strings into the general ones (User and
   operational behavior), and adds `not_a_declaration_message`. Neither
   this phase nor RFC 0055's phase 1 waits on the other: whichever lands
   first introduces `QueryFile::Control`, and the other extends it. Ships
   alone: nothing accepts a new statement yet;
   every `.gq` file and every RFC 0055 statement parses as before.
   Until its own door phase ships, a statement of any family, at either
   door, at a CLI verb, or in a runner step, is refused with `statement
   '<name>' is not served yet` (HTTP 400, exit 1); the four maintenance
   families keep that refusal at the served door through phase 4, its
   `; run it embedded with --store` suffix appended once phase 4 ships
   that arm, and `schema plan` is refused at the served door indefinitely
   with its own row's message (Design, Server dispatch).
   `implementation` advances to `in-progress`.
2. **`load` and `schema`** (`omnigraph-server`, `omnigraph-api-types`,
   `omnigraph-cli`, `omnigraph-gqt`): the handler-body splits, the
   `loaded` and `schema_applied` kinds, the `schema plan` body, the
   optional `page` object on `ReadOutput` and the CLI renderers that read
   it, which `schema plan`'s `supported` needs, the
   body-cap and cluster-backed refusals, both CLI arms, the `affected:`
   and `outcome:` sentences, the first four cases; `openapi.json`
   regenerated, `docs/user/cli/reference.md:54` corrected, the loader and
   schema user docs amended with the statement spellings. Ships alone.
3. **Inspection reads** (`snapshot`, `commit`, `changes poll`, `blob
   stat`, `queries list`): `ReadDispatch` variants, the row projections,
   both CLI arms, the parity rows, the cases. Ships alone;
   every read stays available on its route. No verb's capability word
   changes in phases 2 and 3.
4. **Maintenance, embedded** (`omnigraph-policy`, `omnigraph`,
   `omnigraph-cli`, `omnigraph-gqt`): the `maintain` action, the three
   `_as` entries, the four statements on the embedded arm and in the
   runner, the four `outcome` kinds, the served-door interim refusal, the
   policy docs and the agent-facing `skills/omnigraph/*` references. Ships
   alone: served maintenance stays unavailable, as it is today.
5. **Maintenance, served** (lands with the RFC 0011 Decision 11 mechanism
   RFC): the served arm submitting the job under `maintain`, and the
   interim refusal removed for the three statements that RFC covers plus
   `rebuild full text indexes` once it accepts RFC 0043's quiescence
   obligation. `repair confirm force` keeps its refusal permanently. What
   that RFC owns and what this one fixes is stated in Design,
   Maintenance. RFC 0011's "served-job + direct break-glass"
   (`0011:537-538`) is realized as the statement on `POST /mutate` beside
   the unchanged `direct` verb; a verb-level served arm (`omnigraph
   optimize --server`) is that RFC's `planes.rs` and `cli.rs` change, not
   this one's, so no verb's capability word changes here either. Ships
   when that RFC and its worker land. `implementation` advances to
   `complete`.

The RFC PR adds this file as `docs/rfcs/0056-gq-one-language.md`, its
registry row, and the next-number bump to `0057`, with
`scripts/check-docs.py` green.

## Unresolved questions

Each is the maintainers' decision; the body is written to the lean, and
each names the event that forces it. One door for every statement and the
three stream operations are not open questions: both are decided in
Alternatives (3 and 11) and recorded there as out of scope.

- Q1, maintenance in this RFC. Lean: statements and embedded execution
  now, the served form a job. Decided by: the maintainers. Forced by: the
  acceptance of the Decision 11 mechanism RFC, which phase 5 lands with.
  Otherwise: serve `rebuild` inline now, since it is
  already `Change`-gated (Alternatives 9 narrows to the other three); or
  maintenance out entirely (phases 4 and 5, the `maintain` action, the
  `_as` entries, and Alternatives 10 leave, and the governance motive is
  withdrawn).
- Q2, payload form. Lean: inline `$$ … $$` only. Decided by: the
  maintainers. Forced by: the first payload that must contain `$$`, or the
  first inline `load` over the 1 MiB door cap. Otherwise the `from <uri>`
  clause of Alternatives 6 enters `load_stmt` with a fetch capability
  section; a tagged block is the smaller extension for the `$$` case.
- Q3, page state. Lean: an optional `page` object on `ReadOutput`,
  skip-when-absent (Alternatives 7), carrying the continuation fields and
  the family's header-level values alike. Decided by: the maintainers.
  Forced by: phase 3, which ships the field. Otherwise page state repeats
  as columns and a terminal page carries none, which the lean exists to
  avoid.
- Q4, `schema plan` and `schema apply` against a server. Lean: `schema
  plan` stays embedded-only and RFC 0011's capability table is unchanged;
  a served `schema apply` gets the route's 409, with no statement-specific
  rule. Decided by: the maintainers. Forced by: an operator asking for a
  served plan, which needs RFC 0011's `direct` row changed. Otherwise a
  statement-specific rule for cluster-managed graphs is a second path to
  their schema, which the lean exists to avoid.
- Q5, `load` row grammar. Lean: the strict envelope. Decided by: the
  maintainers together with RFC 0045's owner. Forced by: an RFC 0045
  amendment moving the seed to the strict shape; nobody is named to write
  one, so absent that the lean stands at acceptance. Otherwise the block
  takes the seed's compatibility shape, one row grammar per file, and the
  statement diverges from the verb and `POST /load/ndjson`.

## Decision log

- 2026-09-06: draft r0 written.
