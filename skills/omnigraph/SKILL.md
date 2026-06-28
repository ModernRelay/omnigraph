---
name: omnigraph
description: Store, retrieve, and query knowledge, memory, and relationships in an Omnigraph graph, and operate a local or remote Omnigraph deployment. Use when the user wants to capture or recall facts, notes, or entities, build or query a knowledge graph or agent memory, or run Omnigraph — and whenever you see Omnigraph CLI commands (omnigraph init/query/mutate/load/schema/lint/embed/branch/commit/login/profile/cluster), .pg schema or .gq query files, s3:// graph URIs, bearer-authed graph endpoints, 504 errors, or a cluster.yaml / omnigraph.yaml / ~/.omnigraph/config.yaml. Covers cluster-mode deployments (cluster.yaml plan/apply, omnigraph-server --cluster), the two config surfaces (cluster.yaml + ~/.omnigraph/config.yaml), schema evolution, query linting, data writes (mutate; load needs --mode/--from), branches, embeddings, Cedar policy, and remote ops. Especially important before schema apply (plan first), any load (--mode required), any .gq/.pg edit (lint after), or any remote write (verify via commit list).
license: MIT (see LICENSE at repo root)
compatibility: Requires omnigraph CLI >= 0.7.0 — the unified `load`, the two config surfaces (cluster.yaml + ~/.omnigraph/config.yaml), and cluster apply/serve all require 0.7.0.
metadata:
  author: ModernRelay
  version: "0.7.0"
  repository: https://github.com/ModernRelay/omnigraph
---

# Operating Omnigraph Locally

This skill captures the operational rules for working with a locally or remotely deployed Omnigraph. Follow them when authoring schema, writing queries, loading data, evolving schema, or automating graph operations.

## The Seven Rules

1. **Lint before commit** — `omnigraph lint --schema schema.pg --query queries/foo.gq` validates both sides against each other. No running repo required.
2. **Plan before apply** — never run `schema apply` without a successful `schema plan` first. Apply is destructive; plan is free. (Cluster mode has the same rule with different verbs: `cluster plan` before `cluster apply` — the plan embeds the engine's real migration steps.)
3. **Branches are for data; apply is for schema** — review bulk data loads on a feature branch then merge. Schema changes go straight to `main`: in cluster mode edit the `.pg` and run `cluster apply` (a direct `schema apply` **refuses** a cluster-managed graph); `schema plan`/`apply` is for a non-cluster store.
4. **Pick the right write command** — `mutate` for edits (typechecked, parameterized); `load` for bulk JSONL, local **or** remote, with a **required** `--mode` (`merge` upsert · `append` strict-insert · `overwrite` clean-slate). `load --from <base>` forks a review branch in one shot; bare `load` needs an existing target branch.
5. **Parameterize everything** — never string-interpolate values into `.gq` bodies or `--params`. Declare `$var: Type` and pass via `--params`.
6. **Expose agent operations as aliases** — not raw CLI invocations. Aliases decouple the operation name from the query implementation.
7. **Verify after every remote write** — compare `commit list --branch main` head before and after. The CLI's exit code is not authoritative on remote graphs; proxies can drop the response while the write commits server-side. See `references/remote-ops.md` for the verification ritual and how to recover from 504s.

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
- **List/sort** by appending `order { $s.stagingTimestamp desc } limit 50` after `return`.
- **Ranking ops (`nearest`/`bm25`/`rrf`) require a trailing `limit N`** — omitting it is a compile error. They live in `order { }`, not as filters. Scope with `match`/filters first, then rank (`order { nearest($d.embedding, $q) } limit 10`).

### Mutation (`.gq`)

There is **no top-level `mutation { }`** — every block is a named `query`; the verb (`insert`/`update`/`delete`) makes it a write. Dispatch with `omnigraph mutate` (not `query`).

```gq
query add_signal($slug: String, $name: String, $brief: String, $createdAt: DateTime) {
    insert Signal { slug: $slug, name: $name, brief: $brief,
                    stagingTimestamp: $createdAt, createdAt: $createdAt, updatedAt: $createdAt }
}
query link($from: String, $to: String) { insert FormsPattern { from: $from, to: $to } }
query retitle($slug: String, $t: String) { update Signal set { name: $t } where slug = $slug }
query remove($slug: String)              { delete Signal where slug = $slug }
```

- **Every non-nullable property must be supplied** or lint fails (`T12: insert for 'Signal' must provide non-nullable property 'X'`).
- A single mutation is insert/update-only **or** delete-only — never both (parse-time D₂ rule); split them.
- Edges have no `@key`: give `from`/`to` slugs; the property block is `{}` when the edge has none.

### Bulk load (JSONL)

```jsonl
{"type":"Signal","data":{"slug":"sig-foo","name":"Foo","brief":"…","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}}
{"edge":"FormsPattern","from":"sig-foo","to":"pat-bar","data":{}}
```

```bash
omnigraph load --data seed.jsonl --mode merge $GRAPH                                  # --mode is REQUIRED (no default)
omnigraph load --data delta.jsonl --from main --branch review --mode merge $GRAPH     # fork a review branch in one shot
```

- `--mode`: `merge` (upsert by `@key`) · `append` (fails on collision) · `overwrite` (destructive, staged). `--from <base>` forks a missing `--branch`; bare `load` needs an existing branch. Works local **and** remote.
- **Date footgun**: `mutate --params` takes ISO strings (`Date` `"2026-04-29"`, `DateTime` `"…T00:00:00Z"`); `load` JSONL takes **integer days since epoch** for `Date` (`20572`) but ISO for `DateTime`.

### Dispatching

```bash
omnigraph alias  signal sig-foo                  # operator alias → its bound stored query (read or write)
omnigraph query  get_signal --params '{"slug":"sig-foo"}'   # served stored query by name (verb asserts read vs write)
omnigraph query  -e 'query q() { match { $s: Signal } return { $s.slug } limit 5 }'   # ad-hoc/inline (or: --query f.gq <name>)
omnigraph mutate add_signal --query mutations.gq --params '{"slug":"sig-foo", ...}'   # name positional; ad-hoc file source
omnigraph lint   --schema schema.pg --query queries/foo.gq    # after EVERY .gq/.pg edit (no server needed)
```

### `.gq` grammar

The non-obvious facts that bite, then the full grammar:

- **Scalar param types**: `String Bool I32 I64 U32 U64 F32 F64 DateTime Date Blob`. Modifiers: `T?` (optional), `[T]` (list), `Vector(N)`. There is **no `Int`** — use `I64`.
- **A read query needs `match` *and* `return`** (`order`/`limit` optional); a mutation has neither — only `insert`/`update`/`delete`.
- **`limit` takes an integer literal, not a param** — `limit 50`, never `limit $n`.
- **Variable-hop traversal**: `$p knows{1,3} $f` (`{1,}` = unbounded).
- **Literals & calls**: `now()`, `date("2026-04-29")`, `datetime("…T00:00:00Z")`, list `[…]`.
- **Filters** `= != > < >= <= contains`; **aggregates** `count/sum/avg/min/max` (`count($f) as n`).
- **Stored-query metadata**: `@description("…")` / `@instruction("…")` may follow the param list.
- **Casing**: type names uppercase-initial (`Signal`); idents/edges lowercase-initial (`formsPattern`); variables `$`-prefixed. `//` and `/* */` comments only.

Authoritative PEG grammar (pest) for `.gq` files ("NanoGraph" is the legacy engine name):

```pest
// NanoGraph Query Grammar (.gq files)

WHITESPACE = _{ " " | "\t" | "\r" | "\n" }
COMMENT = _{ LINE_COMMENT | BLOCK_COMMENT }
LINE_COMMENT = _{ "//" ~ (!"\n" ~ ANY)* }
BLOCK_COMMENT = _{ "/*" ~ (!"*/" ~ ANY)* ~ "*/" }

query_file = { SOI ~ query_decl* ~ EOI }

query_decl = {
    "query" ~ ident ~ "(" ~ param_list? ~ ")" ~ query_annotation* ~ "{"
        ~ query_body
    ~ "}"
}
query_annotation = { description_annotation | instruction_annotation }
description_annotation = { "@description" ~ "(" ~ string_lit ~ ")" }
instruction_annotation = { "@instruction" ~ "(" ~ string_lit ~ ")" }

query_body = { read_query_body | mutation_body }
mutation_body = { mutation_stmt+ }
read_query_body = {
    match_clause
    ~ return_clause
    ~ order_clause?
    ~ limit_clause?
}

mutation_stmt = { insert_stmt | update_stmt | delete_stmt }
insert_stmt = { "insert" ~ type_name ~ "{" ~ mutation_assignment+ ~ "}" }
update_stmt = { "update" ~ type_name ~ "set" ~ "{" ~ mutation_assignment+ ~ "}" ~ "where" ~ mutation_predicate }
delete_stmt = { "delete" ~ type_name ~ "where" ~ mutation_predicate }
mutation_assignment = { ident ~ ":" ~ match_value ~ ","? }
mutation_predicate = { ident ~ comp_op ~ match_value }

param_list = { param ~ ("," ~ param)* }
param = { variable ~ ":" ~ type_ref }

type_ref = { (list_type | base_type | vector_type) ~ "?"? }
list_type = { "[" ~ base_type ~ "]" }
vector_type = { "Vector" ~ "(" ~ integer ~ ")" }
base_type = { "String" | "Blob" | "Bool" | "I32" | "I64" | "U32" | "U64" | "F32" | "F64" | "DateTime" | "Date" }

match_clause = { "match" ~ "{" ~ clause+ ~ "}" }

clause = { negation | binding | traversal | filter | text_search_clause }
text_search_clause = { search_call | fuzzy_call | match_text_call }

// Binding: $p: Person { name: "Alice" }
binding = { variable ~ ":" ~ type_name ~ ("{" ~ prop_match_list ~ "}")? }

prop_match_list = { prop_match ~ ("," ~ prop_match)* ~ ","? }
prop_match = { ident ~ ":" ~ match_value }
match_value = { literal | variable | now_call }

// Traversal: $p knows $f
traversal = { variable ~ edge_ident ~ traversal_bounds? ~ variable }
traversal_bounds = { "{" ~ integer ~ "," ~ integer? ~ "}" }

// Filter: $f.age > 25
filter = { expr ~ filter_op ~ expr }

// Negation: not { ... }
negation = { "not" ~ "{" ~ clause+ ~ "}" }

// Return clause — projections separated by commas or newlines
return_clause = { "return" ~ "{" ~ projection+ ~ "}" }
projection = { expr ~ ("as" ~ ident)? ~ ","? }

// Order clause
order_clause = { "order" ~ "{" ~ ordering ~ ("," ~ ordering)* ~ "}" }
ordering = { nearest_ordering | (expr ~ order_dir?) }
nearest_ordering = { "nearest" ~ "(" ~ prop_access ~ "," ~ expr ~ ")" }
order_dir = { "asc" | "desc" }

// Limit clause
limit_clause = { "limit" ~ integer }

// Expressions
expr = { now_call | nearest_ordering | search_call | fuzzy_call | match_text_call | bm25_call | rrf_call | agg_call | prop_access | variable | literal | ident }
now_call = { "now" ~ "(" ~ ")" }
search_call = { "search" ~ "(" ~ expr ~ "," ~ expr ~ ")" }
fuzzy_call = { "fuzzy" ~ "(" ~ expr ~ "," ~ expr ~ ("," ~ expr)? ~ ")" }
match_text_call = { "match_text" ~ "(" ~ expr ~ "," ~ expr ~ ")" }
bm25_call = { "bm25" ~ "(" ~ expr ~ "," ~ expr ~ ")" }
rank_expr = { nearest_ordering | bm25_call }
rrf_call = { "rrf" ~ "(" ~ rank_expr ~ "," ~ rank_expr ~ ("," ~ expr)? ~ ")" }

prop_access = { variable ~ "." ~ ident }

agg_call = { agg_func ~ "(" ~ expr ~ ")" }
agg_func = { "count" | "sum" | "avg" | "min" | "max" }

comp_op = { ">=" | "<=" | "!=" | ">" | "<" | "=" }
filter_op = { "contains" | comp_op }

// Terminals
variable = @{ "$" ~ (ident_chars | "_") }
ident_chars = @{ (ASCII_ALPHA_LOWER | "_") ~ (ASCII_ALPHANUMERIC | "_")* }

// Edge identifier — lowercase start, same as ident but used in traversal context
// Must not match keywords
edge_ident = @{ !("not" ~ !ASCII_ALPHANUMERIC) ~ (ASCII_ALPHA_LOWER | "_") ~ (ASCII_ALPHANUMERIC | "_")* }

type_name = @{ ASCII_ALPHA_UPPER ~ (ASCII_ALPHANUMERIC | "_")* }
ident = @{ (ASCII_ALPHA_LOWER | "_") ~ (ASCII_ALPHANUMERIC | "_")* }

literal = { list_lit | datetime_lit | date_lit | string_lit | float_lit | integer | bool_lit }
date_lit = { "date" ~ "(" ~ string_lit ~ ")" }
datetime_lit = { "datetime" ~ "(" ~ string_lit ~ ")" }
list_lit = { "[" ~ (literal ~ ("," ~ literal)*)? ~ "]" }
string_lit = @{ "\"" ~ string_char* ~ "\"" }
string_char = @{ !("\"" | "\\") ~ ANY | "\\" ~ ANY }
float_lit = @{ ASCII_DIGIT+ ~ "." ~ ASCII_DIGIT+ }
integer = @{ ASCII_DIGIT+ }
bool_lit = { "true" | "false" }
```

## CLI Reference (condensed)

Notation: `<x>` required · `[x]` optional · `<a|b>` choice · `…` repeatable.

**Global addressing flags**: `--as <actor>` (direct/`--store` writes only — a server resolves the actor from its token), `--server <name|url>`, `--cluster <dir|uri>` (cluster-managed storage, for maintenance), `--graph <id>` (selects the graph within a `--server` or `--cluster` scope), `--profile <name>` (`$OMNIGRAPH_PROFILE`), `--store <uri>`. Data commands also take a positional `file://`/`s3://` URI (`--config <dir>` is for `cluster` commands only). Output: `--json`, or reads take `--format <json|jsonl|csv|kv|table>`. **Write guards:** `--yes` skips the confirm prompt for a destructive write (`cleanup`, overwrite `load`, `branch delete`) against a non-local scope (it *refuses* without it when non-TTY or `--json`); `--quiet` suppresses the resolved-target echo.

**Data plane** — `any` (served via `--server`/`--profile`, or direct via `--store`/URI):
- `query` (alias `read`) `<name>` — a **served stored query** by name (via `--server`/`--profile`); or ad-hoc `[<name>] (--query <f.gq> | -e '<GQ>')` where `<name>` picks which query in the source. `[--params <json> | --params-file <p>] [--branch <b> | --snapshot <id>] [--format <fmt> | --json]`. No positional URI — address via `--server`/`--store`/`--profile`.
- `mutate` (alias `change`) — same shape (served stored mutation by `<name>`, or ad-hoc `--query`/`-e`); `[--params …] [--branch <b>] [--json]`. The verb asserts kind: `query`→read, `mutate`→write (400 on mismatch).
- `alias <name> [args…]` — invoke an operator alias's bound stored query (read or write); `[--params … | --params-file <p>] [--format <fmt> | --json]` (server/graph/query come from the binding)
- `load --data <f.jsonl> --mode <overwrite|append|merge> [--branch <b>] [--from <base>] [--json]` — `--mode` required; `--from` forks a missing `--branch`
- `snapshot [--branch <b>] [--json]`
- `export [--branch <b>] [--type <T>…] [--table <K>…]` (streams JSONL)
- `branch <create <name> [--from <base>] | list | delete <name> | merge <source> --into <target>> [--json]`
- `commit <list [--branch <b>] | show <commit_id>> [--json]`
- `schema <plan | apply> --schema <f.pg> [--allow-data-loss] [--json]` · `schema show` (alias `get`) — `apply` **refuses a cluster-managed graph** (evolve those via `cluster apply`)

**Served only** (needs `--server`/`--profile`): `graphs list [--json]`

**Direct / storage** — reject `--server`; address by positional URI or `--cluster <dir|s3> --graph <id>`:
- `init --schema <f.pg> <uri> [--force]`
- `lint --query <f.gq> [--schema <f.pg>] [<uri>] [--json]` — offline with `--schema`, graph-backed with a URI
- `optimize [--json]` · `repair [--confirm] [--force] [--json]` · `cleanup (--keep <N> | --older-than <7d>) --confirm [--json]`
- `queries <validate [<uri>] | list> [--json]`

**Control plane** — cluster (`--config <dir>`, default `.`):
- `cluster <validate | plan | apply | status | refresh | import> [--config <dir>] [--json]`
- `cluster approve <resource> --as <actor> [--config <dir>] [--json]` · `cluster force-unlock <lock_id> [--config <dir>] [--json]`

**Local** (no graph):
- `policy <validate | test --tests <f> | explain --actor <a> --action <act> [--branch <b> | --target-branch <b>]> --cluster <dir> [--graph <id>]`
- `embed --seed <embed.yaml> [--reembed_all | --clean | --select "<Type>:<field>=<value>"]`
- `login <server> [--token <t>]` (prefer piping the token on stdin) · `logout <server>` · `profile <list | show [<name>]>` · `version`

Pre-0.7.0 spellings (`read`/`change`/`ingest`, `--target`, positional `http://`) → [`references/migrations.md`](references/migrations.md).

## Five Ontology Design Criteria (Gruber 1993)

Omnigraph schemas are ontologies. The canonical design criteria from Gruber's *Toward Principles for the Design of Ontologies Used for Knowledge Sharing* (Int. J. Human-Computer Studies 43:907–928) apply directly when authoring `.pg` files.

1. **Clarity** — definitions should communicate intended meaning unambiguously and be independent of social or computational context. In Omnigraph: precise type names, narrow enums over `String`, `@check`/`@range` for stated invariants. A reviewer should understand the domain from the schema alone.
2. **Coherence** — inferences sanctioned by the schema must be consistent with the domain modeled. Gruber's trap: defining quantity as a `(magnitude, unit)` pair makes `6 feet ≠ 2 yards` even though they describe the same length. In Omnigraph: watch for `@card`, `@unique`, and edge directionality that let the schema distinguish things the domain treats as equal.
3. **Extendibility** — the schema should support specialization without revising existing definitions. In Omnigraph: prefer interfaces for shared shape, leave enums open where the domain genuinely admits more, model identifiers via mapping functions rather than baking units/formats into the entity.
4. **Minimal encoding bias** — representation choices made for notation or implementation convenience leak into the model. In Omnigraph: don't type dates as `String` because the source API returns strings; separate conceptual entities (a publication date, a person) from their surface encoding (a year integer, a name string) when both matter.
5. **Minimal ontological commitment** — make as few claims about the world as the use case requires. In Omnigraph: don't add required properties, closed enums, or `@card(1..1)` "in case"; tighten later via `schema plan`/`apply` when a real constraint emerges. Weaker schemas leave consumers room to specialize.

The criteria trade off against each other — Clarity wants tight definitions while Minimal Commitment wants weak ones. Gruber's resolution: *having decided a distinction is worth making, give it the tightest possible definition*. Decide what to model conservatively; once modeled, constrain precisely.

## Schema Authoring Principles

Twelve practical rules for `.pg` authoring — full text and examples in [`docs/omni-schema.md`](../../docs/omni-schema.md). In short: schema-is-the-contract · explicit identity via `@key` · model meaning not tables · strong intentional types · deliberate optionality · shared shape in interfaces · schema-level constraints (`@unique`/`@index`/`@range`/`@check`/`@card`) · search as a schema decision · edge semantics matter · reviewable schemas · intentional migrations (`@rename_from`) · domain clarity over ORM habits.

Design flow: entities → stable keys → relationships worth their own edge → enum candidates → uniqueness/bounds/cardinality → search needs → shared shape into interfaces → evolution plan.

## Provenance Is Structural (Multi-Agent Source of Truth)

When Omnigraph serves as canonical truth across multiple agents, every assertion must answer *who said it, when, based on what evidence*. This is the runtime guarantee Gruber's criteria don't cover — his agents shared vocabulary; ours additionally must share attribution. Provenance belongs in the schema, not in logs.

Without structural provenance, agents cannot reconcile contradictory assertions, retract facts when a source is discredited, replay graph state at a past timestamp, or distinguish high-evidence facts from speculation.

**In Omnigraph:** model provenance as a `Claim`-style interface (or a separate `Claim` node linked to each sourced fact) with required fields — `asserted_by: Actor`, `asserted_at: DateTime`, `evidence_source: Source`, optionally `confidence: F64`. Don't stash provenance into a free-text `source: String` or a `metadata: JSON` dump — structured provenance is queryable, indexable, and migratable; free-form is none of these.

## Storage & Credentials

A graph's bytes live in one of two backends:

- **Local filesystem** — a path or `file://` URI. In cluster mode `storage:` defaults to the config directory, so local dev needs no object store.
- **S3-compatible object storage** — AWS, Railway, Tigris, etc. (`s3://bucket/prefix`). Authenticate with the standard `AWS_*` environment contract; keep dev creds in a git-ignored `.env.omni` and source it before CLI calls:

```bash
set -a && source .env.omni && set +a
```

`init`, `load`, and **`cluster apply`** write storage directly (bypassing the server). `cluster apply` is a storage-direct control-plane command — it reaches the object store directly (the `__cluster/` ledger *and* each graph's Lance datasets, to create/migrate/delete them), never through a running server, so the host that runs it needs storage access (the `AWS_*` contract for an `s3://` cluster). That is by design: the control plane is declarative (config → cluster), not a runtime mutation API on the serving process. The server reads **cluster** state read-only at boot, but it is not read-only against storage overall — data-plane HTTP writes (`mutate`/`load`/`branch`) still go through the server to the graph datasets, so it needs read-write storage access. Validate with `curl http://127.0.0.1:8080/healthz`, then `omnigraph snapshot <graph-uri> --json`.

## Project Layout

### Deployment & access (omnigraph >= 0.7.0)

- **Cluster deployment — the only way to serve.** A `cluster.yaml` declares the
  whole deployment (graphs, schemas, stored queries, policies, optional S3
  `storage:` root); `omnigraph cluster apply` converges it and
  `omnigraph-server --cluster .` (or `--cluster s3://bucket/prefix`,
  config-free) serves it. See `references/cluster.md`.
- **Direct / embedded access — no server.** Address a graph's storage directly
  with `--store <file://|s3:// uri>` or a positional URI for one-off CLI ops.
  There is **no single-graph server mode** — the server is cluster-only.

### The two config surfaces (omnigraph >= 0.7.0)

Configuration has two single-owner homes (RFC-007/008), plus an
everything-explicit flag/env tier:

| Surface | Owner | Location | Declares |
|---|---|---|---|
| **Cluster config** | the team, in the repo | `cluster.yaml` + the `.pg`/`.gq`/policy files it references | what the system **is**: graphs, schemas, queries, policies, storage |
| **Operator config** | one person | `~/.omnigraph/config.yaml` (`$OMNIGRAPH_HOME` relocates it) | who **I** am: identity, named servers, output defaults, personal aliases |
| Flags / env | per invocation | — | everything, explicitly |

```yaml
# ~/.omnigraph/config.yaml — per operator, never committed
operator:
  actor: act-andrew          # default --as identity
servers:
  intel-dev:
    url: https://graph.example.com    # no tokens here, ever
defaults:
  output: table              # read-format default
  server: intel-dev          # default served scope (or `store: file://…/g.omni` for a local default — mutually exclusive)
  default_graph: spike       # graph within a server/cluster scope
profiles:                    # optional named scope bundles — pick with --profile <name>
  staging: { server: intel-staging, default_graph: spike }
aliases:                     # personal bindings to TEAM stored queries (see references/aliases.md)
  triage: { server: intel-dev, graph: spike, query: weekly_triage, args: [since] }
```

The operator config and credentials are **auto-discovered — no flag points at them**: the CLI reads `$OMNIGRAPH_HOME/config.yaml` (default `~/.omnigraph/config.yaml`), and an absent file is just an empty layer (zero-config). `$OMNIGRAPH_HOME` relocates the *directory* only, not a specific file. (`--config`/`$OMNIGRAPH_CONFIG` is a separate flag for the cluster / server config — not this.)

Credentials live outside config: `echo $TOKEN | omnigraph login intel-dev`
writes `~/.omnigraph/credentials` (`0600`); the matching token resolves via
`OMNIGRAPH_TOKEN_INTEL_DEV` or that file.

**Addressing a graph**: `--store <file://|s3:// uri>` or a positional URI for
direct storage; `--server <name|url>` (+ `--graph <id>`) for a served remote;
`--profile <name>` for a named bundle; else the operator `defaults`. A remote is
addressed with `--server` (a bare `http(s)://` URL is not a graph address). Run
data-plane commands from a graph's project folder so relative `queries/`,
`schema.pg`, and `.env.omni` paths resolve.

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
| `@embed(text)` without quotes | `unexpected constraint_name` | `@embed("text")` |
| `@unique(src)` on edge without body block | parse error | `@card(1..1) { @unique(src) }` |
| `load --mode merge` after `@embed` source change | stale embeddings | `omnigraph embed --reembed_all` or `load --mode overwrite` |
| `schema apply` with feature branches open | rejected | Merge or delete branches first |
| `nearest(...)` / `bm25(...)` / `rrf(...)` without `limit` | compile error | Add `limit N` |
| Adding non-nullable property without backfill | unsupported migration | Make optional → backfill → tighten in follow-up apply |
| `omnigraph init --json` | `unexpected argument --json` | `init` doesn't support `--json`; drop the flag |
| `omnigraph init` on an already-initialized URI | `AlreadyInitialized` error (v0.6.0+) | `--force` to re-init (skips the schema preflight; does **not** purge data) |
| `schema apply` dropping a property/type | soft-dropped or rejected (no data loss) | add `--allow-data-loss` to actually drop the column |
| Committing `.env.omni` | credential leak | Add `.env*` to `.gitignore` |
| Non-parameterized query values | typecheck surprise, injection risk | Declare `$param: Type` and pass via `--params` |
| Missing required field in `insert` | `T12: insert for 'X' must provide non-nullable property 'Y'` | Accept the param in the mutation signature |
| Long-lived feature branches | merge conflicts, schema apply blocked | Merge promptly; delete when done |
| `mutation { ... }` wrapper in `.gq` | `parse error: expected query_file` at line 1 | Use `query <name>(...) { insert T { ... } }`; there is no top-level `mutation` keyword |
| `--config` placed before subcommand | `unexpected argument --config` | Put `--config` **after** the subcommand (e.g. `omnigraph schema show --config X`) |
| Reading a large schema via stdout-capped tool | Truncated, garbled, or duplicated output | `omnigraph schema show > /tmp/schema.pg` first; then read the file with offset/limit |
| `omnigraph load` without `--mode` | error: `--mode` is required | Pass `--mode merge\|append\|overwrite` — there is no default (overwrite is destructive, so it is never implicit). `load` works against local and remote URIs |
| Blind retry after 504 | Duplicate Signal/Decision/Claim (append-only types lack `@key` dedup) | `commit list --branch main --json` first; head advanced means it landed; only retry if unchanged |
| `sync_branch()` mentioned in version-drift error | Searching for nonexistent CLI command | Server-internal directive in error text; just retry — the next call re-pins to the new head |
| Stale empty branches at `main`'s head | 504-orphaned forks from a timed-out `load --from`; eventually block writes | List branches, find ones at `main`'s `graph_commit_id`, `omnigraph branch delete --config X <name>` |
| `omnigraph schema apply` / `init` on a cluster-managed graph | refused — bypasses the cluster ledger | Evolve cluster graphs via `omnigraph cluster apply --config .`; `schema apply`/`init` are for a non-cluster store |
| `omnigraph optimize` against a table with a `Blob` property | table is **skipped**, not failed (Lance blob-v2 compaction bug) | Expected — `--json` reports it under `skipped`; non-blob tables still compact |
| `@unique` on a `[List]`/`Blob` column | `load` now errors loudly (was silently un-enforced before #160) | Use `@unique` only on scalar columns (and composite `@unique(a, b)`, now keyed as a true tuple) — uniqueness needs a type that reduces to a scalar key |

## Deep Dives

- `references/cluster.md` — cluster-mode declarative deployments: cluster.yaml, the validate/import/plan/apply loop, approval-gated deletes, `--cluster` serving, the two-file contract, recovery

For anything beyond the basics, load the relevant reference file. Each is self-contained — load only what you need.

| Reference | When to load |
|-----------|--------------|
| [`references/schema.md`](references/schema.md) | Editing `.pg` files, running `schema plan`/`apply`, renaming types, backfilling required fields |
| [`references/queries.md`](references/queries.md) | Writing or linting `.gq` files, search functions, aggregations, multi-hop patterns |
| [`references/data.md`](references/data.md) | Choosing between `mutate` and `load` (required `--mode`, `--from` to fork a review branch); branch review workflow; destructive ops |
| [`references/remote-ops.md`](references/remote-ops.md) | Operating against a remote/CloudFront-fronted graph: 504 verification ritual, version drift, fork-branch 504 fingerprints, append-only retry safety, operator `--server`/`login` targeting |
| [`references/search.md`](references/search.md) | Embeddings, `@embed`, vector/text ranking, scope-then-rank pattern |
| [`references/aliases.md`](references/aliases.md) | Defining aliases for agents, structured output, JSON args |
| [`references/stored-queries.md`](references/stored-queries.md) | Server-side stored-query registry: declared in `cluster.yaml`, `omnigraph queries validate/list`, `GET /graphs/{id}/queries` + `POST /graphs/{id}/queries/{name}`, `invoke_query` Cedar gating |
| [`references/server-policy.md`](references/server-policy.md) | Starting the HTTP server, routes, bearer auth, Cedar policy gating, multi-graph mode |
| [`references/commands.md`](references/commands.md) | `snapshot`, `export`, `commit list/show`, addressing & resolution |
| [`references/migrations.md`](references/migrations.md) | Migrating a pre-0.7.0 setup, or you hit an old config/command/flag/route/error and need its current form |
