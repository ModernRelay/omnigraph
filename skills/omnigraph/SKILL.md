---
name: omnigraph
description: Operate OmniGraph graphs and deployments. Use for `.pg` schemas, `.gq` queries, OmniGraph CLI commands, `file://`/`s3://`/`az://` graph URIs, `cluster.yaml`, operator config, bearer-authenticated servers, graph-backed knowledge or memory, Blob values, embeddings, branches, commits, and change feeds. Apply especially before schema changes, bulk loads, and retries after uncertain remote outcomes.
license: MIT (see LICENSE at repo root)
compatibility: Validated against OmniGraph CLI and server 0.10.x. The CLI, server, and client bindings must be upgraded together.
metadata:
  author: ModernRelay
  version: "0.10.0"
  repository: https://github.com/ModernRelay/omnigraph
---

# Operating Omnigraph Locally

This skill captures the operational rules for working with a locally or remotely deployed Omnigraph. Follow them when authoring schema, writing queries, loading data, evolving schema, or automating graph operations.

## The Seven Rules

1. **Lint before commit** — `omnigraph lint --schema schema.pg --query queries/foo.gq` validates both sides against each other. No running repo required.
2. **Plan before apply** — never run `schema apply` without a successful `schema plan` first. Apply is destructive; plan is free. (Cluster mode has the same rule with different verbs: `cluster plan` before `cluster apply` — the plan embeds the engine's real migration steps.)
3. **Branches are for data; apply is for schema** — review bulk data loads on a feature branch then merge. Schema changes go straight to `main`: in cluster mode edit the `.pg` and run `cluster apply` (a direct `schema apply` **refuses** a cluster-managed graph); `schema plan`/`apply` is for a non-cluster store.
4. **Pick the right write command** — `mutate` for edits (typechecked, parameterized); `load` for bulk JSONL, local **or** remote, with a **required** `--mode` (`merge` upsert · `append` strict-insert · `overwrite` replaces only the node/edge types represented in the batch). `load --from <base>` forks a review branch in one shot; bare `load` needs an existing target branch.
5. **Parameterize everything** — never string-interpolate values into `.gq` bodies or `--params`. Declare `$var: Type` and pass via `--params`.
6. **Expose agent reads as aliases** — aliases decouple a read operation name
   from its stored-query implementation. Aliases are read-only; invoke a served
   stored mutation with `omnigraph mutate <name> --server ...`.
7. **Treat a lost remote response as unknown** — a successful JSON write response contains the exact commit published by that attempt, but a proxy can return 504 after publication. On timeout, verify the branch head or intended entity effect before retrying. See `references/remote-ops.md`.

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
- **`nearest` and `rrf` require a trailing `limit N`** — omitting it is a compile error. `bm25` does not require a limit, but use one to keep ranked output bounded. Ranking operators live in `order { }`, not as filters. Scope with `match`/filters first, then rank (`order { nearest($d.embedding, $q) } limit 10`).

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

- **Every non-nullable property must be supplied.** Lint normally reports T12
  when one is missing. In v0.10, a non-null Vector target carrying `@embed`
  is a known exception: source-only insert can lint successfully even though
  execution still requires the vector. Supply it explicitly; writes never
  embed automatically.
- A single mutation is insert/update-only **or** delete-only — never both (parse-time D₂ rule); split them.
- Edges have no `@key`: give logical `from`/`to` endpoint IDs. A propertyless edge needs only `from` and `to`; there is no nested `data` block in GQ.

### Bulk load (JSONL)

```jsonl
{"type":"Signal","data":{"slug":"sig-foo","name":"Foo","brief":"…","stagingTimestamp":"2026-04-14T00:00:00Z","createdAt":"2026-04-14T00:00:00Z","updatedAt":"2026-04-14T00:00:00Z"}}
{"edge":"FormsPattern","from":"sig-foo","to":"pat-bar","data":{}}
```

```bash
omnigraph load --data seed.jsonl --mode merge $GRAPH                                  # --mode is REQUIRED (no default)
omnigraph load --data delta.jsonl --from main --branch review --mode merge $GRAPH     # fork a review branch in one shot
```

- `--mode`: `merge` (upsert by logical entity ID; keyed node IDs derive from their `@key` tuple) · `append` (fails on ID collision) · `overwrite` (destructive, staged). `--from <base>` forks a missing `--branch`; bare `load` needs an existing branch. Works local **and** remote.
- **Date values**: `mutate --params` takes ISO strings. `load` accepts ISO `Date` strings (recommended) or integer epoch days, and ISO `DateTime` strings.

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
- **Variable-hop traversal**: `$p knows{1,3} $f` — bounds are **required to be finite** (`{1,}` is rejected: "unbounded traversal is disabled").
- **Undirected traversal**: `$p <knows> $f` matches the edge in either direction, deduplicated (a pair connected both ways appears once). Same-endpoint-type edges only (e.g. `Related: Issue -> Issue`) — asymmetric edges are rejected (T22). Composes with bounds (`$p <knows>{1,3} $f`) and `not { }`.
- **Edge bindings**: an optional `$var:` prefix on the edge word — `$src $w:knows $dst`, undirected `$a $w:<related> $b` — binds the matched edge row, so edge properties work in filters (`$w.confidence = "asserted"`), projections (`return { $w.role }`), aggregates, and ordering. A bound traversal returns one row per edge (parallel edges stay distinct); binding a `{min,max}` multi-hop, rebinding a taken name, or projecting bare `$w` is rejected (T23).
- **Result columns**: each `return { }` entry is one column, named by its alias or its expression (`$s.slug` → `s.slug`); two entries that would produce one column name are rejected (T25) — alias them apart.
- **Literals & calls**: `now()`, `date("2026-04-29")`, `datetime("…T00:00:00Z")`, list `[…]`.
`starts_with`, `contains`, `>=`, `<=`, `!=`, `>`, `<`, `=`

Those are the complete **filter operators**; String predicates are exact and
case-sensitive. **Aggregates** are `count/sum/avg/min/max` (`count($f) as n`).
- **Stored-query metadata**: `@description("…")` / `@instruction("…")` may follow the param list.
- **Casing**: type names uppercase-initial (`Signal`); idents/edges lowercase-initial (`formsPattern`); variables `$`-prefixed. `//` and `/* */` comments only.

The compiler grammar in `crates/omnigraph-compiler/src/query/query.pest` is the
single source of truth.

## CLI Reference (condensed)

Notation: `<x>` required · `[x]` optional · `<a|b>` choice · `…` repeatable.

**Global addressing flags**: `--as <actor>` (direct-engine writes and actor-bound cluster operations; remote writes derive the actor from the bearer token), `--server <name|url>`, `--cluster <dir|uri>` (cluster-managed storage, primarily for maintenance), `--graph <id>` (selects within a `--server` or `--cluster` scope), `--profile <name>` (`$OMNIGRAPH_PROFILE`), `--store <uri>`. Commands with an open positional slot also accept `file://`, `s3://`, or preview `az://` directly. `--config <dir>` belongs only to `cluster` subcommands. Output: `--json`, or read queries take `--format <json|jsonl|csv|kv|table>`. **Write guards:** `--yes` skips non-local confirmation for destructive writes; `--quiet` suppresses the resolved-target echo.

**Data plane** — `any` (served via `--server`/`--profile`, or direct via `--store`/URI):
- `query` (alias `read`) `<name>` — a **served stored query** by name (via `--server`/`--profile`); or ad-hoc `[<name>] (--query <f.gq> | -e '<GQ>')` where `<name>` picks which query in the source. `[--params <json> | --params-file <p>] [--branch <b> | --snapshot <id>] [--format <fmt> | --json]`. No positional URI — address via `--server`/`--store`/`--profile`.
- `mutate` (alias `change`) — same shape (served stored mutation by `<name>`, or ad-hoc `--query`/`-e`); `[--params …] [--branch <b>] [--if-commit <graph_commit_id>] [--json]`. The verb asserts kind; a failed precondition has no effect and exits 4.
- `load --data <f.jsonl> --mode <overwrite|append|merge> [--branch <b>] [--from <base>] [--json]` — `--mode` required; `--from` forks a missing `--branch`; overwrite replaces only represented types
- `blob <get|stat> <node|edge> <TYPE> <ID> <PROPERTY>` — dedicated Blob-cell reads; `get` supports ranges/`--out`, `stat` returns metadata
- `snapshot [--branch <b>] [--json]`
- `export [--branch <b>] [--type <T>…]` (streams JSONL)
- `branch <create <name> [--from <base>] | list | delete <name> | merge <source> --into <target> [--delete-branch]> [--json]`
- `commit <list [--branch <b>] | show <commit_id> | changes <commit_id> [filters…]> [--json]`
- `changes <poll [--start now|beginning|after:<id> | --cursor <c>] | baseline --out <snapshot.jsonl>> [filters…] [--json]`
- `schema apply --schema <f.pg> [--allow-data-loss] [--json]` · `schema show` (alias `get`) — `apply` **refuses a cluster-managed graph** (evolve those via `cluster apply`)

**Served only** (needs `--server`/`--profile`): `graphs list [--json]`

**Direct / storage** — reject `--server`. `init` requires its positional URI;
`schema plan` uses a positional URI or `--store`; lint and maintenance also
accept `--cluster <dir|file://|s3://|az://> --graph <id>`:
- `init --schema <f.pg> <uri> [--force]`
- `schema plan --schema <f.pg> [--allow-data-loss] [--json]`
- `lint --query <f.gq> [--schema <f.pg>] [<uri>] [--json]` — offline with `--schema`, graph-backed with a URI
- `optimize [--json]` · `repair [--confirm] [--force] [--json]` · `cleanup [--keep <N>] [--older-than <7d>] --confirm [--json]` (at least one retention option; both may be combined)
- `rebuild-full-text-indexes [--branch <b>] [--json]` — replace full-text indexes on one branch with default English analysis; custom tokenizer settings are replaced. Stop overlapping writers and retain a whole-store backup for upgrades. `--as` records attribution; direct access does not load server policy. See [maintenance commands](references/commands.md#rebuild-full-text-indexes--explicit-analyzer-upgrade).

**Control plane**:
- `cluster <validate | plan | apply | status | refresh | import> [--config <dir>] [--json]`
- `cluster approve <resource> --as <actor> [--config <dir>] [--json]` · `cluster force-unlock <lock_id> [--config <dir>] [--json]`
- `policy <validate | test --tests <f> | explain --actor <a> --action <act> [--branch <b> | --target-branch <b>]> --cluster <dir|uri> [--graph <id>]`
- `queries <validate | list> --cluster <dir|uri> [--graph <id>] [--json]`

**Local** (no graph):
- `alias <name> [args…]` — invoke an operator alias's bound stored read query; `[--params … | --params-file <p>] [--format <fmt> | --json]` (server/graph/query come from the binding)
- `embed (--seed <embed.yaml> | --input <raw.jsonl> --output <out.jsonl> --spec <spec.json>) [--reembed-all | --clean] [--type <T>…] [--select "<Type>:<field>=<value>"]`
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

Twelve practical rules for `.pg` authoring — full text and examples in the bundled [`references/schema.md`](references/schema.md). In short: schema-is-the-contract · explicit identity via `@key` · model meaning not tables · strong intentional types · deliberate optionality · shared shape in interfaces · schema-level constraints (`@unique`/`@index`/`@range`/`@check`/`@card`) · search as a schema decision · edge semantics matter · reviewable schemas · intentional migrations (`@rename_from`) · domain clarity over ORM habits.

Design flow: entities → stable keys → relationships worth their own edge → enum candidates → uniqueness/bounds/cardinality → search needs → shared shape into interfaces → evolution plan.

## Provenance Is Structural (Multi-Agent Source of Truth)

When Omnigraph serves as canonical truth across multiple agents, every assertion must answer *who said it, when, based on what evidence*. This is the runtime guarantee Gruber's criteria don't cover — his agents shared vocabulary; ours additionally must share attribution. Provenance belongs in the schema, not in logs.

Without structural provenance, agents cannot reconcile contradictory assertions, retract facts when a source is discredited, replay graph state at a past timestamp, or distinguish high-evidence facts from speculation.

**In Omnigraph:** model provenance as a `Claim` node linked by typed edges to
the asserted fact, an `Actor`, and a `Source`. Keep scalar facts such as
`asserted_at: DateTime` and optional `confidence: F64` on `Claim`; properties
cannot be node-typed. Don't stash provenance into a free-text `source: String`
or a metadata dump—structural provenance is queryable and migratable;
free-form provenance is neither.

## Storage & Credentials

A graph's bytes live in one of three supported URI families:

- **Local filesystem** — a path or `file://` URI. In cluster mode `storage:` defaults to the config directory, so local dev needs no object store.
- **S3-compatible object storage** — AWS, Railway, Tigris, etc. (`s3://bucket/prefix`). Authenticate with the standard `AWS_*` environment contract.
- **Azure Blob storage preview** — `az://container/prefix`. Reads and Azurite qualification are available, but Azure is not production-supported; every write or maintenance process must be launched through the root-scoped `omnigraph-azure-admission` wrapper.

Keep development credentials in a git-ignored `.env.omni` and source it before CLI calls:

```bash
set -a && source .env.omni && set +a
```

Direct `init`/`load` and **`cluster apply`** write storage without an HTTP server. A served `load` is different: the CLI sends it to `omnigraph-server`, which performs the graph write. `cluster apply` reaches the cluster ledger and graph datasets directly, so its host needs storage credentials. A serving process also needs read-write access for served data-plane writes. Validate with `curl http://127.0.0.1:8080/healthz`, then `omnigraph snapshot --server <name> --graph <id> --json`.

## Project Layout

### Deployment & access

- **Cluster deployment — the only way to serve.** A `cluster.yaml` declares the
  whole deployment (graphs, schemas, stored queries, policies, optional S3/Azure
  `storage:` root); `omnigraph cluster apply` converges it and
  `omnigraph-server --cluster .` (or `--cluster s3://bucket/prefix`,
  config-free) serves it. See `references/cluster.md`.
- **Direct / embedded access — no server.** Address a graph's storage directly
  with `--store <file://|s3://|az:// uri>` or a positional URI for one-off CLI ops.
  There is **no single-graph server mode** — the server is cluster-only.

### The two config surfaces

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

The operator config and credentials are **auto-discovered — no flag points at them**: the CLI reads `$OMNIGRAPH_HOME/config.yaml` (default `~/.omnigraph/config.yaml`), and an absent file is just an empty layer (zero-config). `$OMNIGRAPH_HOME` relocates the *directory* only, not a specific file. Only `cluster` subcommands take `--config`.

Credentials live outside config: `echo $TOKEN | omnigraph login intel-dev`
writes `~/.omnigraph/credentials` (`0600`); the matching token resolves via
`OMNIGRAPH_TOKEN_INTEL_DEV` or that file.

**Addressing a graph**: `--store <file://|s3://|az:// uri>` or a positional URI for
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
| Assuming `@embed` must quote its source | unnecessary schema churn | `@embed(text)` and `@embed("text")` are both valid; quoted form is canonical |
| `@unique(src)` on edge without body block | parse error | `@card(1..1) { @unique(src) }` |
| Expecting `@embed` to populate vectors during load | missing/stale vectors | `@embed` is metadata; run the offline `omnigraph embed ... --reembed-all` file pipeline, then load its output |
| `schema apply` with feature branches open | rejected | Merge or delete branches first |
| `nearest(...)` / `rrf(...)` without `limit` | compile error | Add `limit N`; a BM25-only query may omit it, though bounded output is recommended |
| Adding non-nullable property without backfill | unsupported migration | Make optional → backfill; keep it optional (tightening `T?` → `T` is refused, OG-MF-106) |
| `omnigraph init --json` | `unexpected argument --json` | `init` doesn't support `--json`; drop the flag |
| `omnigraph init` on an already-initialized URI | `AlreadyInitialized` error | Never overwrite it. `--force` only replaces orphan schema artifacts after proving there is no graph manifest |
| `schema apply` dropping a property/type | soft-dropped by default (no physical data loss) | use `--allow-data-loss` on both plan and apply to preview and execute a hard drop |
| Committing `.env.omni` | credential leak | Add `.env*` to `.gitignore` |
| Non-parameterized query values | typecheck surprise, injection risk | Declare `$param: Type` and pass via `--params` |
| Missing required field in `insert` | `T12: insert for 'X' must provide non-nullable property 'Y'` | Accept the param in the mutation signature |
| Long-lived feature branches | merge conflicts, schema apply blocked | Merge promptly; delete when done |
| `mutation { ... }` wrapper in `.gq` | `parse error: expected query_file` at line 1 | Use `query <name>(...) { insert T { ... } }`; there is no top-level `mutation` keyword |
| `--config` on a data/schema command | `unexpected argument --config` | Only `cluster` subcommands accept it; use `--server`/`--graph`, `--store`, or `--profile` elsewhere |
| Reading a large schema via stdout-capped tool | Truncated, garbled, or duplicated output | `omnigraph schema show --server <name> --graph <id> > /tmp/schema.pg`, then read the file in chunks |
| `omnigraph load` without `--mode` | error: `--mode` is required | Pass `--mode merge\|append\|overwrite` — there is no default (overwrite is destructive, so it is never implicit). Address direct storage or a served graph |
| Blind retry after 504 | duplicate unkeyed nodes/edges or a repeated effect | compare the intended branch/entity state first; retry only after proving the attempt did not land |
| Stale empty branches at `main`'s head | 504-orphaned forks from a timed-out `load --from`; eventually block writes | List branches, find ones at `main`'s `graph_commit_id`, `omnigraph branch delete <name> --store <graph-uri>` |
| `omnigraph schema apply` / `init` on a cluster-managed graph | refused — bypasses the cluster ledger | Evolve cluster graphs via `omnigraph cluster apply --config .`; `schema apply`/`init` are for a non-cluster store |
| Assuming Blob-bearing data cannot compact | unnecessary skipped maintenance | Lance 11 Blob compaction is supported; `optimize` preserves null/empty/non-empty values |
| `@unique`/`@index` on a Blob column | schema parse/validation rejection | Blob properties cannot be keys, unique, or indexed |
| Full-text search after upgrading an old store to 0.10 | explicit rebuild-required error | stop mixed-version access and run `rebuild-full-text-indexes` on every live branch that needs text search |

## Deep Dives

For anything beyond the basics, load the relevant reference file. Each is self-contained — load only what you need.

| Reference | When to load |
|-----------|--------------|
| [`references/schema.md`](references/schema.md) | Editing `.pg` files, running `schema plan`/`apply`, renaming types, backfilling required fields |
| [`references/queries.md`](references/queries.md) | Writing or linting `.gq` files, search functions, aggregations, multi-hop patterns |
| [`references/data.md`](references/data.md) | Choosing between `mutate` and `load` (required `--mode`, `--from` to fork a review branch); branch review workflow; exact overwrite scope |
| [`references/blobs.md`](references/blobs.md) | Writing and reading managed/external Blob values, selectors/ranges, security and lifecycle boundaries |
| [`references/changes.md`](references/changes.md) | Exact read/write positions, conditional mutations, commit diffs, feed cursors, baselines, and retention gaps |
| [`references/remote-ops.md`](references/remote-ops.md) | Operating through `--server`: exact receipts, unknown 504 outcomes, conflict handling, and safe retry decisions |
| [`references/search.md`](references/search.md) | Embeddings, `@embed`, vector/text ranking, scope-then-rank pattern |
| [`references/aliases.md`](references/aliases.md) | Defining aliases for agents, structured output, JSON args |
| [`references/stored-queries.md`](references/stored-queries.md) | Cluster stored-query registry: declaration, `queries validate/list --cluster`, served invocation, and `invoke_query` Cedar gating |
| [`references/server-policy.md`](references/server-policy.md) | Starting the HTTP server, routes, bearer auth, Cedar policy gating, multi-graph mode |
| [`references/commands.md`](references/commands.md) | Current command shapes, addressing, output, and maintenance |
| [`references/migrations.md`](references/migrations.md) | Pre-0.7 vocabulary and the coordinated v0.9→v0.10 upgrade boundary |
