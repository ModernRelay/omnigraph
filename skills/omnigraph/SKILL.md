---
name: omnigraph
description: Operate OmniGraph graphs and deployments. Use for `.pg` schemas, `.gq` queries, OmniGraph CLI commands, `file://`/`s3://`/`az://` graph URIs, `cluster.yaml`, operator config, bearer-authenticated servers, graph-backed knowledge or memory, Blob values, embeddings, branches, commits, and change feeds. Apply especially before schema changes, bulk loads, and retries after uncertain remote outcomes.
license: MIT (see LICENSE at repo root)
compatibility: Covers the current OmniGraph CLI and server, using HTTP contract 0.14. Upgrade the CLI, server and HTTP integrations together; the storage formats `omnigraph version` reports are served, and a standalone v8, v9 or v13 graph is converted by the offline `omnigraph upgrade`, never on open.
metadata:
  author: ModernRelay
  version: "0.14.0"
  repository: https://github.com/ModernRelay/omnigraph
---

# Operating Omnigraph Locally

This skill captures the operational rules for working with a locally or remotely deployed Omnigraph. Follow them when authoring schema, writing queries, loading data, evolving schema, or automating graph operations.

Check `omnigraph version` and the command's `--help` before using these
instructions. This skill tracks the current CLI/server contract, including
`Omnigraph-Http-Api: 0.14`; the released 0.13 contract is unsupported. The engine
reports `internal-schema 16 (serves v14 to v16)`: the storage format it
writes, then the range it serves. New graphs use `__id`/`__src`/`__dst` system columns; a graph
that already spells them `id`/`src`/`dst` keeps its physical columns. A
standalone v8 or v9 graph (written by 0.11.x) or a v13 graph is converted in
place by the offline `omnigraph upgrade`; a v0.10 graph uses v6 and is first
taken to v9 with the 0.11 binary, or rebuilt. See
[upgrading past v0.11](references/migrations.md#upgrade-v011-to-storage-format-14).
Read [migration guidance](references/migrations.md) before replacing a
deployed binary.

An `omnigraph` binary that has the `skill` command carries the copy of this
skill written for it: `omnigraph skill export <dir> --force` writes it where
your agent loads skills, and `omnigraph skill show [<path>]` prints one file.
Prefer that copy when the installed CLI differs from the one this copy was
written for (see
[version-matched skill](references/commands.md#version-matched-skill)).

## GQ 2.1 traversal support

0.12.0 compiles GQ 2.1, which adds typed edge selections to traversal. A v0.11
binary cannot parse this syntax; do not send it to a deployment not yet on 0.12.
`$a (knows | likes) $b` selects named edge types; `$a * $b` selects compatible
types from the captured schema and requires declared endpoint node types.
Omitted bounds always mean `{1,1}`; recursive traversal needs finite bounds
such as `{1,3}`. `$a $e:(knows | likes) $b` binds one-hop concrete edges, exposing
`$e.@type` and `$e.@id`. Common properties must exist with compatible base types
on every member; nullability and enum domains widen.

`set traversal_work_limit = 1000000;` controls shared traversal row work in
statements using selections. Full selected-table sizes count before scans, even
for selective neighborhoods. A result limit cannot replace this cap. Exhaustion
terminates the query with an error; received rows do not become partial success.
Historical wildcard targets are refused. Stored-query validation can refuse a
schema addition that would invalidate a wildcard common-property reference.
See [traversal](../../docs/user/queries/traversal.md) and
[session settings](../../docs/user/queries/settings.md).

## The Seven Rules

1. **Lint before commit** — `omnigraph lint --schema schema.pg --query queries/foo.gq` validates both sides against each other. No running repo required.
2. **Plan before apply** — never run `schema apply` without a successful `schema plan` first. Apply is destructive; plan is free. (Cluster mode has the same rule with different verbs: `cluster plan` before `cluster apply` — the plan embeds the engine's real migration steps.)
3. **Branches are for data; apply is for schema** — review bulk data loads on a feature branch then merge. Schema changes go straight to `main`: in cluster mode edit the `.pg` and run `cluster apply` (a server target deploys without restart; a direct root target requires stopped serving; a direct `schema apply` **refuses** a cluster-managed graph); for the managed service commit, `cluster push --target managed`, `cluster plan --target managed --rev <rev>`, then `cluster apply --target managed --plan <run>`; `schema plan`/`apply` is for a non-cluster store.
4. **Pick the right write command** — `mutate` for edits (typechecked, parameterized); `load` for bulk JSONL, local **or** remote, with a **required** `--mode` (`merge` upsert · `append` strict-insert · `overwrite` replaces only the node/edge types represented in the batch). `load --from <base>` forks a review branch in one shot; bare `load` needs an existing target branch.
5. **Parameterize everything** — never string-interpolate values into `.gq` bodies or `--params`. Declare `$var: Type` and pass via `--params`.
6. **Expose agent reads as aliases** — aliases decouple a read operation name
   from its stored-query implementation. Aliases are read-only; invoke a served
   stored mutation with `omnigraph mutate <name> --target ...`.
7. **Treat a lost remote response as unknown** — an effectful data mutation or load returns its exact commit, but a proxy can return 504 after publication. Branch statements have different outcome/receipt semantics. On timeout, reconcile the intended effect and relevant history before replay. See `references/remote-ops.md`.

## Essentials: Queries, Mutations, Loads

The patterns below cover the daily 80% — enough to write correct `.gq` and JSONL without leaving this file. The long tail (multi-hop, negation, aggregations, hybrid search, every decorator) is in [`references/queries.md`](references/queries.md) and [`references/schema.md`](references/schema.md).

**Comments in `.pg` and `.gq` are `//`, never `#`** (the #1 parse error).

### Read query (`.gq`)

```gq
query get_signal($slug: String) {
    match {
        $s: Signal { slug: $slug }   // inline property filter goes in the match block
        $s formsPattern $p           // edge FormsPattern declared PascalCase, traversed lowerCamelCase
    }
    return { $s.slug, $s.name, $p.slug }
}
```

- **Parameterize, never interpolate.** Declare `$var: Type` in the signature; pass via `--params '{"slug":"sig-foo"}'`. An empty signature still needs parens: `query foo() { ... }`.
- **Edge traversal is lowerCamelCase** even though the schema declares edges PascalCase (`FormsPattern` → `formsPattern`).
- **System identity uses `@`.** Read `$s.@id` and `$e.@src`/`$e.@dst`;
  bare `id`, `src`, and `dst` name declared user properties. `return { $s }`
  returns an object containing `@id` and its non-Blob/non-Vector properties.
  `schema show --json` reports the graph's physical `system_columns`; do not
  substitute physical `__id` names into GQ.
- **List/sort** by appending `order { $s.stagingTimestamp desc } limit 50` after `return`. Without `order`, `limit n` may return any `n` valid rows (the subset can change between releases); order for stable pages.
- **`nearest` and `rrf` require a trailing `limit N`** — omitting it is a compile error. `bm25` does not require a limit, but use one to keep ranked output bounded. Ranking operators lead `order { }`, and still restrict rows (`bm25` keeps only text matches; `nearest` skips null vectors). Scope with `match`/filters first, then rank (`order { nearest($d.embedding, $q) } limit 10`). Project a score by repeating the leading key: `return { $d.slug, nearest($d.embedding, $q) as distance }`.

### Mutation (`.gq`)

There is **no top-level `mutation { }`** — every block is a named `query`; the verb (`insert`/`update`/`delete`) makes it a write. Dispatch with `omnigraph mutate` (not `query`). (A source may instead hold exactly one branch statement — `branch create|delete|merge|list`; see [`references/queries.md`](references/queries.md#branch-statements).)

```gq
query add_signal($slug: String, $name: String, $brief: String, $createdAt: DateTime) {
    insert Signal { slug: $slug, name: $name, brief: $brief,
                    stagingTimestamp: $createdAt, createdAt: $createdAt, updatedAt: $createdAt }
}
query link($from: String, $to: String) { insert FormsPattern { from: $from, to: $to } }
query retitle($slug: String, $t: String) { update Signal set { name: $t } where slug = $slug }
query remove($slug: String)              { delete Signal where slug = $slug }
```

- **Every non-nullable property must be supplied.** Lint normally reports T12
  when one is missing. A non-null Vector target carrying `@embed`
  is a known exception: source-only insert can lint successfully even though
  execution still requires the vector. Supply it explicitly; writes never
  embed automatically.
- A single mutation is insert/update-only **or** delete-only — never both (D₂ rule, refused at execution before any effect; `lint` does not catch it); split them.
- Edge inserts give logical `from`/`to` endpoint IDs; a propertyless edge needs only those, and there is no nested `data` block in GQ. An unkeyed edge insert is strict (inserting twice creates two edges). An edge type may declare `@key(@src, @dst[, prop…])` in its body when created; inserts then upsert by the derived id, and identical inserts on two branches converge on merge. Edges cannot be `update`d (T16).

### Bulk load (JSONL)

```jsonl
{"type":"Signal","data":{"slug":"sig-foo","name":"Foo","brief":"…","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}}
{"edge":"FormsPattern","from":"sig-foo","to":"pat-bar","data":{}}
```

```bash
omnigraph load --data seed.jsonl --mode merge --target $GRAPH                                  # --mode is REQUIRED (no default)
omnigraph load --data delta.jsonl --from main --branch review --mode merge --target $GRAPH     # fork a review branch in one shot
```

- `--mode`: `merge` (upsert by logical entity ID; keyed node and keyed edge IDs derive from their `@key` tuple — re-running a merge file duplicates unkeyed edges) · `append` (fails on ID collision) · `overwrite` (destructive, staged). `--from <base>` forks a missing `--branch`; bare `load` needs an existing branch. Works local **and** remote.
- **Date values**: use a calendar-day string (`YYYY-MM-DD`) for `Date` and an ISO timestamp to the millisecond for `DateTime` (`.123` or `.123000`; `.123456` is refused), in both `mutate --params` and JSONL. `load` also accepts integer epoch days for `Date`.

### Dispatching

```bash
omnigraph alias  signal sig-foo                  # operator alias → its bound stored read query
omnigraph query  get_signal --params '{"slug":"sig-foo"}'   # served stored query by name (verb asserts read vs write)
omnigraph query  -e 'query q() { match { $s: Signal } return { $s.slug } limit 5 }'   # ad-hoc/inline (or: --query f.gq <name>)
omnigraph mutate add_signal --query mutations.gq --params '{"slug":"sig-foo","name":"Foo","brief":"Example","createdAt":"2026-04-14T00:00:00Z"}'   # name positional; ad-hoc file source
omnigraph lint   --schema schema.pg --query queries/foo.gq    # after EVERY .gq/.pg edit (no server needed)
```

### `.gq` grammar

The non-obvious facts that bite, then the full grammar:

- **Scalar param types**: `String Bool I32 I64 U32 U64 F32 F64 DateTime Date Blob`. Modifiers: `T?` (optional), `[T]` (list), `Vector(N)`. There is **no `Int`** — use `I64`.
- **A read query needs `match` *and* `return`** (`order`/`limit` optional); a mutation has neither — only `insert`/`update`/`delete`.
- **`limit` takes an integer literal, not a param** — `limit 50`, never `limit $n`.
- **Variable-hop traversal**: `$p knows{1,3} $f` — bounds are **required to be finite** (`{1,}` is rejected: "unbounded traversal is disabled"). Hops are shortest-path distances; each `$_` is a distinct anonymous node; variables beginning `__` are reserved.
- **Undirected traversal**: `$p <knows> $f` matches the edge in either direction, deduplicated (a pair connected both ways appears once). Same-endpoint-type edges only (e.g. `Related: Issue -> Issue`) — asymmetric edges are rejected (T22). Composes with bounds (`$p <knows>{1,3} $f`) and correlated blocks (`not { }`, `exists { }`, `count { } > 2`).
- **Edge bindings**: an optional `$var:` prefix on the edge word — `$src $w:knows $dst`, undirected `$a $w:<related> $b` — binds the matched edge row, so edge properties work in filters (`$w.confidence = "asserted"`), projections (`return { $w.role }`), aggregates, and ordering. A bound traversal returns one row per edge (parallel edges stay distinct); binding a `{min,max}` multi-hop, rebinding a taken name, or projecting bare `$w` is rejected (T23).
- **Literals & calls**: `now()`, `date("2026-04-29")`, `datetime("…T00:00:00Z")`, list `[…]`.
`starts_with`, `contains`, `in`, `>=`, `<=`, `!=`, `>`, `<`, `=`, `is null`, `is not null`

Those are the complete **filter operators** (`$m.number in $numbers` tests membership in a list parameter or literal); conditions compose with `and`, `or`, `not`, parentheses and `is null` / `is not null`, and a filter keeps only rows whose expression is true (a comparison with a null operand is null). String predicates are exact and
case-sensitive. **Aggregates** are `count/sum/avg/min/max` (`count($f) as n`); `min`/`max` also accept String, Bool, Date, and DateTime. Alias aggregates and order by the alias.
- **Result column names must be distinct** (`T25`): alias projections that would collide.
- **Stored-query metadata**: `@description("…")` / `@instruction("…")` may follow the param list.
- **Casing**: type names uppercase-initial (`Signal`); idents/edges lowercase-initial (`formsPattern`); variables `$`-prefixed. `//` and `/* */` comments only.

The compiler grammar in `crates/omnigraph-compiler/src/query/query.pest` is the
single source of truth.

## CLI Reference (condensed)

Select one complete destination with `--target NAME|ADDRESS` or the operator
file's `defaults.target`. Every backend invocation prints the resolved target to
stderr; machine results remain on stdout. An unknown or unsupported target
refuses without fallback. Source files and current directories never select a
backend. `--config DIR` is source input only.

- Direct graph: `--target ./graph.omni` or a registered `kind: direct, scope: graph` entry.
- Direct cluster root: `--target ./cluster --graph knowledge`; registered root targets can set `default_graph`.
- Server: `--target production --graph knowledge` or an explicit HTTP/Unix address. A registered server target binds its endpoint and credential owner or `auth: anonymous`.
- Managed: a registered account or cluster target pins API, organization, authentication mechanism and, for clusters, ID and incarnation. Data access additionally requires its endpoint. `--graph` or `default_graph` is required for graph commands.
- Catalogue: `--target og_catalog` for `query` and `schema show`.

Bare target names resolve from the operator file; use `./` for relative inline
paths. `init --schema schema.pg ./graph.omni` is the sole positional backend
address convenience. `--as ACTOR` attributes supported direct operations;
served targets derive their actor from credentials and send `--as` as the
`Omnigraph-If-Actor` assertion, refusing a mismatch with `actor_mismatch` and
exit 4; managed targets refuse it. `--yes` supplies explicit
consent for destructive nonlocal operations. There is no quiet switch for the
destination line. Legacy `--server`, `--store`, `--cluster`, `--profile`,
`--managed`, `--direct`, graph `--uri`, `use` and `profile` commands are removed.

| Family | Operations and applicability |
|---|---|
| Data | `query`, `mutate`, `load --mode overwrite\|append\|merge`, `branch`, `commit`, `snapshot`, `export`, `blob`, `diff`, `changes`; direct or server targets |
| Managed data | `query`, `mutate`, `load`, `commit list/show`, `graphs list`; other wrappers do not acquire managed credentials |
| Direct schema | `schema plan`; `schema apply` and `schema upgrade-system-columns` require standalone graph targets |
| Direct maintenance | `optimize`, `repair`, `cleanup`, `rebuild-full-text-indexes`; `upgrade --target ADDRESS` is standalone and offline |
| Source-only | `lint --schema FILE --query FILE`, `cluster validate --config DIR`; no backend target |
| Root control | `queries list/validate`, `policy validate/test/explain`, `cluster observe/force-unlock/upgrade-ledger`; direct root targets |
| Deployment | `cluster plan/apply --config DIR` on a direct root or server; `cluster status` uses only the target |
| Managed account | `cluster create/list`, `organization members/invitations`; account target |
| Managed cluster | `cluster members/pull/push/plan/apply/status/history/cancel/token/delete/undo-delete`; complete cluster target |
| Local configuration | `target list/show/add/remove/import`, `skill export/show`, `version`, `embed`; `alias NAME` owns its target and graph |
| Authentication | `login --target NAME` / `logout --target NAME`; server static credentials or managed human-device session. `credential get --target NAME` exports a server token to a pipe |

`query` and `mutate` invoke stored queries by positional name, or accept
`--query FILE` / `-e SOURCE`. Use `--params` or `--params-file`, never interpolate
values into query source. Reads support `--format table|json|jsonl|csv|kv`;
`--json` preserves exact receipt/error structure. Inspect `commit` and
`graph_commit_id` after uncertain writes; do not infer safe retry from a timeout.

Managed source changes use `cluster push --config DIR --expected-revision REV`,
then `cluster plan --rev REV`, then `cluster apply --plan PREVIEW_ID`, all with
the same cluster target. Managed plan/apply reject local `--config`. Creation
returns identity; it does not select a new target or bind a folder. Human-device
and automation-env authentication never fall back to each other. Preserve
operator-home lifecycle recovery records; `target import` validates and migrates
legacy folder evidence before new lifecycle submissions.

For direct recovery, `cluster apply --target ROOT --deployment-id ID
--writers-stopped` has no `--config`: it reconciles an existing attempt. A fresh
apply with a caller ID requires explicit `--config`, and reusing an existing ID
only observes that attempt. Full command shapes and bounds live in
[commands](references/commands.md) and [cluster operations](references/cluster.md).

## Schema Design

Omnigraph schemas are ontologies, and when many agents share one graph the
schema is their coordinate system: it decides where a fact goes, what a stored
value means, and which paths agents reason along. Design for **convergence**:
two agents given the same input should write the same graph and read the same
meaning back. Decide conservatively what to model; once modeled, constrain
precisely, because tightening later is a rebuild (adding
`@key`/`@unique`/`@range`/`@check`, changing cardinality, and `T?` → `T` are
refused; only `@index` additions, nullable additions, enum widening, renames and
drops apply in place).

In short: Gruber's five criteria (clarity · coherence · extendibility · minimal
encoding bias · minimal commitment) · neither too loose (one meaning, many
encodings) nor too tight (a meaning with no encoding): tight where agents must
converge, open at declared extension points · write the questions as `.gq`
first ·
design identity first (`@key`, keyed edges, find before create) · kinds are
types, roles are edges · put a fact on what determines it · keep attribution,
justification, confidence and time on separate axes · provenance is structural
(`Claim` → fact, `Actor`, `Source`), never a free-text `source` field · layer
extracted and synthesized knowledge over raw spans, with lineage back to them,
keyed per fact (span + extractor + statement digest) · store decisions, compute
derived facts · narrow types and closed vocabularies · constraints in the
schema; rules it cannot express as `GraphPolicy` nodes agents lint the graph
against, including an orphan check per required link (`@card` never counts a
node written with no edge) · compose around shared identities (modules add
facets, never modify the core) from general primitives that combine · optimize
retrieval: links for recall, specific edges, filters and answer-sized units for
precision · write names, `@description` and `@instruction` for a reader without
context · never repurpose a field · measure convergence with two independent
encoders. Full text with Omnigraph
mappings: [`references/schema-design.md`](references/schema-design.md).

## Storage & Credentials

A graph's bytes live in one of three supported URI families:

- **Local filesystem** — a path or `file://` URI. The target selects the graph or cluster root explicitly; a source directory does not select storage. Local development needs no object store.
- **S3-compatible object storage** — AWS, Railway, Tigris, etc. (`s3://bucket/prefix`). Authenticate with the standard `AWS_*` environment contract.
- **Azure Blob storage preview** — `az://container/prefix`. Reads and Azurite qualification are available, but Azure is not production-supported; every write or maintenance process must be launched through the root-scoped `omnigraph-azure-admission` wrapper.

Keep development credentials in a git-ignored `.env.omni` and source it before CLI calls:

```bash
set -a && source .env.omni && set +a
```

Direct `init`/`load` and **`cluster apply`** write storage without an HTTP server. A served `load` is different: the CLI sends it to `omnigraph-server`, which performs the graph write. A direct `cluster apply` reaches the cluster ledger and graph datasets directly, so its host needs storage credentials (`cluster apply --target managed --plan` is executed by the Intent API). A serving process also needs read-write access for served data-plane writes. Validate with `curl http://127.0.0.1:8080/readyz` (HTTP 200, `ready: true`; `/healthz` only proves the process is alive), then `omnigraph snapshot --target <name> --graph <id> --json`.

## Project Layout

### Deployment & access

- **Cluster deployment — the only way to serve.** A `cluster.yaml` declares the
  whole deployment (graphs, schemas, stored queries, policies, optional S3/Azure
  `storage:` root); `omnigraph cluster apply` converges it and
  `omnigraph-server --cluster .` (or `--cluster s3://bucket/prefix`,
  config-free) serves it. A direct apply keeps its admission lock: once its
  writes have settled, release that exact lock with
  `cluster force-unlock <LOCK_ID>` before starting the server, and send later
  changes with `cluster apply --target <name|url>`.
  See `references/cluster.md`.
- **Direct / embedded access — no server.** Address a graph's storage directly
  with `--target <file://|s3://|az:// uri>`; `init` also accepts its creation URI positionally.
  There is **no single-graph server mode** — the server is cluster-only.

### The two config surfaces

The team owns `cluster.yaml` and referenced schemas, stored queries and policies.
The operator owns complete destinations, identity, presentation and personal
aliases in `~/.omnigraph/config.yaml` (`OMNIGRAPH_HOME` changes that directory).
Source configuration declares desired state; targets select where it applies.

```yaml
target_config_version: 1
operator:
  actor: act-andrew
defaults:
  output: table
  target: intel-dev
targets:
  intel-dev:
    kind: server
    endpoint: https://graph.example.com
    credential: intel-dev
    default_graph: spike
aliases:
  triage:
    target: intel-dev
    graph: spike
    query: weekly_triage
    args: [since]
```

Secrets stay outside configuration. `echo "$TOKEN" | omnigraph login --target
intel-dev` stores an endpoint-bound static token with mode `0600`;
`OMNIGRAPH_TOKEN_INTEL_DEV` overrides it. Managed sessions and data credentials
use separate OS-keychain entries. Named cloud targets pin provider destination
settings while standard provider chains supply storage credentials. Stored
relative paths resolve beside the operator file, whereas explicit inline paths
resolve from the working directory. Source/query paths are independent inputs.

Use `target import` for old operator files and matching lifecycle evidence.
It preserves the original file and refuses ambiguous bindings. Neither
`OMNIGRAPH_PROFILE` nor `.omnigraph/context` supplies a target. A target name
never falls back to a filesystem path, credential cache or discovered graph.

### What to commit

**Commit:** `schema.pg`, `queries/*.gq`, `cluster.yaml`, `seed.md`, `seed.jsonl`, and the project's `README.md` and `CLAUDE.md`.

**Ignore:** `.env.omni` (credentials), `.claude/` (local agent state), `*.omni/` (local graph artifacts), `__cluster/` and `graphs/` (cluster state + derived graph roots).

### Give agents a `CLAUDE.md`

A per-project `CLAUDE.md` tells coding agents where files live and what conventions matter. Without it, agents re-discover the same things every session.

## Common Gotchas

These are the traps most likely to bite. Scan this table before debugging any parse or runtime error.

| Trap | Symptom | Fix |
|------|---------|-----|
| `#` comments in `.pg` | `parse error: expected schema_file` | Use `//` |
| Standalone `enum Foo { ... }` block | `parse error: expected EOI or schema_decl` | Inline: `kind: enum(a, b)` |
| `[Category]` (list of enum) | compile error | Use `[String]`; lists must contain scalars |
| Assuming `@embed` must quote its source | unnecessary schema churn | `@embed(text)` and `@embed("text")` are both valid; quoted form is canonical |
| Bare `src`/`dst` in an edge constraint on a new graph | offline `lint` passes; `init`/`schema plan` fail `unknown property reference 'Knows.src'; the system field is '@src'` | Put `@unique(@src)` inside the edge body; `src` means a declared user property |
| `_`-prefixed property on a new graph | `property name '_x' is reserved for system columns (names starting with '_')` | Rename it; `from`/`to` are also reserved on edges (`is reserved on edge types`) |
| Expecting `@embed` to populate vectors during load | missing/stale vectors | `@embed` is metadata; run the offline `omnigraph embed ... --reembed-all` file pipeline, then load its output |
| `schema apply` with feature branches open | `schema apply requires a graph with only main` | Delete them (`merge --delete-branch` or `branch delete`); a merge alone leaves the branch live |
| `nearest(...)` / `rrf(...)` without `limit` | compile error | Add `limit N`; a BM25-only query may omit it, though bounded output is recommended |
| Adding non-nullable property without backfill | unsupported migration | Make optional → backfill; keep it optional (tightening `T?` → `T` is refused, OG-MF-106) |
| `omnigraph init --json` | `unexpected argument '--json' found` | `init` doesn't support `--json`; drop the flag |
| `omnigraph init` on an already-initialized URI | `graph already initialized or initialization metadata exists at '<uri>'` | Never overwrite it. `--force` only replaces orphan schema artifacts after proving there is no graph manifest |
| `schema apply` dropping a property/type | nothing is reclaimed at apply; older commits still read the dropped data | to reclaim the space, run `cleanup` with a retention that excludes the pre-drop commits; the data is then unrecoverable |
| Committing `.env.omni` | credential leak | Add `.env*` to `.gitignore` |
| Non-parameterized query values | typecheck surprise, injection risk | Declare `$param: Type` and pass via `--params` |
| Missing required field in `insert` | ``T12: insert for `X` must provide non-nullable property `Y` `` | Accept the param in the mutation signature |
| Mixing `delete` with `insert`/`update` in one query | lint passes; `mutate` fails "mixes inserts/updates and deletes" | Split into two mutations (D₂ rule) |
| `$p.id`, `{ id: $v }`, `where id =` from a v0.10 query | ``T6``/``T2``/``T11``: ``type `Person` has no property `id`; the system identity is `$p.@id` `` (a failing stored-query registry quarantines its graph) | Use `$p.@id`, `$e.@src`/`@dst`, `where @id =`; lint every `.gq` before restarting on v0.11 or later |
| Long-lived feature branches | merge conflicts, schema apply blocked | Merge promptly; delete when done |
| `mutation { ... }` wrapper in `.gq` | `parse error: expected query_file` at line 1 | Use `query <name>(...) { insert T { ... } }`; there is no top-level `mutation` keyword |
| `--config` on a data/schema command | `unexpected argument '--config' found` | Use it only for source-taking cluster commands; select destinations with `--target` |
| `--as` names another actor than the served write's token | `actor_mismatch` (exit 4) | Use the token of that actor, or drop `--as`; the bearer token selects the actor |
| Reading a large schema via stdout-capped tool | Truncated, garbled, or duplicated output | `omnigraph schema show --target <name> --graph <id> > /tmp/schema.pg`, then read the file in chunks |
| `omnigraph load` without `--mode` | `the following required arguments were not provided: --mode <MODE>` | Pass `--mode merge\|append\|overwrite` — there is no default (overwrite is destructive, so it is never implicit). Address direct storage or a served graph |
| Blind retry after 504 | duplicate unkeyed nodes/edges or a repeated effect | compare the intended branch/entity state first; retry only after proving the attempt did not land |
| Review branch left after a timed-out `load --from` | uncertain load result | Inspect its content and history; matching `main`'s head alone is not proof of abandonment or authorization to delete it |
| `omnigraph schema apply` / `init` on a cluster-managed graph | refused — bypasses the cluster ledger | Evolve cluster graphs via `omnigraph cluster apply --target ROOT --config .`; `schema apply`/`init` are for a non-cluster store |
| Assuming Blob-bearing data cannot compact | unnecessary skipped maintenance | Lance 13 Blob compaction is supported; `optimize` preserves null/empty/non-empty values |
| `@unique`/`@index` on a Blob column | schema parse/validation rejection | Blob properties cannot be keys, unique, or indexed |
| Full-text search on indexes built before Lance 11 (v0.9 era), including after `omnigraph upgrade` | `FullTextIndexRebuildRequired` | Stop mixed-version access and run `rebuild-full-text-indexes --branch <b>` on every live branch that needs text search; `upgrade` does not rebuild them |
| Reopening a v0.10/v6 graph with v0.12 | `__manifest is stamped at internal schema v6, but this omnigraph reads only v14 to v14` | Stop the old fleet; take a standalone graph to v9 with the 0.11 `upgrade`, then to 14 with the 0.12 `upgrade`, or rebuild by export, `init` and `load --mode overwrite` |
| Default `omnigraph upgrade` of the 0.11 binary on a branched graph or one with `_` properties | `the route ends at v9, which requires a graph with only main` (or `reserves property names starting with '_'`) | Delete branches / `@rename_from` first, or pass `--to-format 8` to both check and execution; the 0.12 `upgrade` converts v8 as well as v9 |
| Opening a v8 or v9 graph with 0.12 | refused: 0.12 reads format 14 only and names `omnigraph upgrade` | Stop every process using the graph, back up the whole root, then run the 0.12 `upgrade --check` and `upgrade` on a standalone graph; a cluster-managed graph is exported with 0.11.x and rebuilt. See [upgrading past v0.11](references/migrations.md#upgrade-v011-to-storage-format-14) |
| Using `data.id` for identity on a new graph | `unknown input field 'id': move data.id to the top-level 'id' field` | Put identity in top-level JSONL `id`; `data` contains user properties |
| Re-running a `--mode merge` load with unkeyed edges | duplicate edges | Supply stable top-level edge `id`s, or declare `@key(@src, @dst)` when creating the edge type |
| Assuming every JSON result cell has a key | absent keys for null values | Query/export JSON omits null cells; change images retain explicit nulls |
| `--format arrow` or `defaults.output: arrow` | `has no text rendering` after the query runs | 0.12.0 has no Arrow output; use `json` or `jsonl` |
| `..` or a symlink in a `cluster.yaml` relative path | `config_path_escape` / `config_path_symlink` | Keep referenced files inside the config directory with plain relative paths |
| Legacy folder/profile routing | `legacy_routing` | Import preserved configuration/custody, then choose an explicit complete target |
| Policy commands with both a graph and a `cluster` bundle applied | `matches 2 policy bundles` | Known 0.12.0 limitation of `policy validate/test/explain --graph`; inspect each bundle file directly |

## Deep Dives

For anything beyond the basics, load the relevant reference file. Each is self-contained — load only what you need.

| Reference | When to load |
|-----------|--------------|
| [`references/schema.md`](references/schema.md) | Editing `.pg` files, running `schema plan`/`apply`, renaming types, backfilling required fields |
| [`references/schema-design.md`](references/schema-design.md) | Designing or reviewing a schema: Gruber's criteria, design flow, identity, provenance, `GraphPolicy` rules, composition, and measuring convergence across agents |
| [`references/queries.md`](references/queries.md) | Writing or linting `.gq` files, search functions, aggregations, multi-hop patterns |
| [`references/data.md`](references/data.md) | Choosing between `mutate` and `load` (required `--mode`, `--from` to fork a review branch); branch review workflow; exact overwrite scope |
| [`references/blobs.md`](references/blobs.md) | Writing and reading managed/external Blob values, selectors/ranges, security and lifecycle boundaries |
| [`references/changes.md`](references/changes.md) | Exact read/write positions, conditional mutations, commit diffs, feed cursors, baselines, and retention gaps |
| [`references/remote-ops.md`](references/remote-ops.md) | Operating through `--target`: exact receipts, unknown 504 outcomes, conflict handling, and safe retry decisions |
| [`references/cluster.md`](references/cluster.md) | Cluster configuration, plan/apply, drift, and serving |
| [`references/search.md`](references/search.md) | Embeddings, `@embed`, vector/text ranking, scope-then-rank pattern |
| [`references/aliases.md`](references/aliases.md) | Defining aliases for agents, structured output, JSON args |
| [`references/stored-queries.md`](references/stored-queries.md) | Cluster stored-query registry: declaration, `queries validate/list --target`, served invocation, and `invoke_query` Cedar gating |
| [`references/server-policy.md`](references/server-policy.md) | Starting the HTTP server, routes, bearer auth, Cedar policy gating, multi-graph mode |
| [`references/commands.md`](references/commands.md) | Current command shapes, addressing, output, and maintenance |
| [`references/migrations.md`](references/migrations.md) | Upgrading v0.11 graphs to storage format 14; v0.11 storage/system-column upgrades and wire changes; older v0.9→v0.10 and pre-0.7 boundaries |
