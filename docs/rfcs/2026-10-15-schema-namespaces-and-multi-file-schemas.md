---
rfc: "2026-10-15-schema-namespaces-and-multi-file-schemas"
title: "Schema namespaces and multi-file schemas"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-15
updated: 2026-10-18
discussion: https://github.com/ModernRelay/omnigraph/issues/838
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Schema namespaces and multi-file schemas

## Summary

This RFC makes two decisions. They can ship separately, and the second depends
on the first.

**Multi-file schemas.** A graph's schema may be written as several `.pg` files.
The files carry no meaning: there is no `import`, and a file's name or directory
never decides a type's name. Composition is declared once, in `cluster.yaml` or
on the command line, and the loader joins the files in a fixed order into one
canonical source text. Plan, apply, the stored contract, the contract digest and
the cluster ledger all see that one text, as they see one file today. No storage
format, IR or ledger field changes. A schema given as one file keeps
byte-identical source and digests, and a single directory path keeps the shape
of every bundle a server receives. The list form is new `cluster.yaml` syntax
that servers without this change refuse (see Compatibility). Errors name the
file and line. This replaces the `import` design that issue #838 proposed (see
[Relation to issue #838](#relation-to-issue-838)).

**Namespaces.** A declaration may sit in a namespace block,
`namespace crm { node Contact { … } }`, which gives it the canonical name
`crm::Contact`.

- The namespace is part of the name and never part of identity. `StableTypeId`
  stays the identity.
- Moving a type into, out of or between namespaces is a rename. It goes through
  the existing `RenameType` and metadata-only `RenameTable` path, and it must be
  declared with `@rename_from`, never inferred.
- Queries write qualified names, or make a namespace's names available
  unqualified with `use crm;`.
- An unqualified name resolves only in the root namespace and the namespaces a
  query `use`s, with no search order. Two matches are a type error, never a
  silent choice.
- Every wire surface carries the canonical string (`"type": "crm::Contact"`) in
  the field that carries a type name today, with no new field.
- A graph that declares no namespace keeps byte-identical IR, `schema_ir_hash`,
  table keys and wire output.

What does not change: data paths, Lance schemas and index names, the
one-publication schema apply, its main-only restriction, and identity by stable
id.

## Motivation

- **Schemas outgrow one file.** One production knowledge graph declares 76
  node types and 426 edge types in a single `.pg` file. Templates that people
  copy into a graph arrive as files of their own, and several teams may own
  parts of one graph.
- **A schema is one file today, everywhere.**
  - `parse_schema(input: &str)` parses one string
    (`crates/omnigraph-compiler/src/schema/parser.rs`).
  - Every production call site passes one file: `init`, `schema plan`,
    `schema apply`, `lint`, and cluster `validate`/`plan`/`apply`.
  - `GraphConfig.schema` is one `PathBuf` (`crates/omnigraph-cluster/src/types.rs`).
  - Queries can already be a file, a directory or a list (`QueriesDecl` in
    `crates/omnigraph-cluster/src/config.rs`).
- **Names are flat.** The `.pg` grammar has
  `type_name = @{ ASCII_ALPHA_UPPER ~ (ASCII_ALPHANUMERIC | "_")* }`. The `.gq`
  rule is the same, plus the `@` prefix of the
  [Engine catalogue](2026-10-16-engine-catalogue.md) types. Two templates that
  each declare `Event` or `OwnedBy` therefore cannot coexist. Edge names are
  also unique after case folding across the whole graph (`normalize_edge_name`
  in `catalog/mod.rs`).
- **Workarounds don't scale.** A prefix convention such as `Crm_Contact` works
  today and costs nothing. It gives no scope to a query, to a future per-type
  policy rule, or to an author reading a 400-type schema.

An issue is not enough: both halves add a grammar surface, and namespaces add a
spelling that is persisted in `__manifest` table keys and in immutable
`__history` extents.

### Relation to issue #838

Issue #838 (accepted) asks for `import` statements in `.pg` files. This RFC
replaces that design: composition is declared once in configuration, and
namespaces, not imports, give names a scope.

- **Kept from #838:**
  - one schema split across files;
  - a core file shared by several graphs of one cluster, as a list entry;
  - errors that give a file and line;
  - a server that never opens caller paths, since the bundle carries the
    composed source.
- **Dropped:**
  - named, selective imports;
  - a `.pg` file that can be checked alone. An editor or a one-file `lint`
    cannot resolve `core::Named` in `crm.pg` without the declaration that
    composes the files. `omnigraph lint --schema <dir>` checks a directory;
  - sharing a core file across clusters, because `check_config_path` refuses
    `..`.
- **Decided differently:** #838 includes a definition reached through several
  paths once. Without imports there is no diamond: a file reaches the
  composition only by being named. A file named twice, as in
  `[schema/core.pg, schema/]`, is an error naming both entries, not a silent
  deduplication.

## User and operational behavior

### Multi-file schemas

```yaml
graphs:
  main:
    schema: schema/                    # every *.pg file directly inside, in path order
  crm:
    schema: [schema/core.pg, schema/crm/]   # files and directories, in this order
```

```text
omnigraph schema plan --schema schema/ graph.omni
```

Discovery copies the `queries:` rules with one difference, so one rule covers
both.

- **What a path may name.** A single path may be a file or a directory, and a
  list may mix both.
  - A directory contributes its top-level `*.pg` files, with no recursion.
  - Each directory's files are sorted by path. A list keeps its own order.
  - A file named twice, directly or through a directory, is an error naming
    both entries.
- **Refusals and caps.**
  - Symlinks and `..` components are refused (`check_config_path`).
  - Reads go through `SourceCapture`, with the same per-file cap and the same
    discovery cap as queries.
  - The composed source, banners included, is one bundle source, and
    `validate_bundle` refuses any source above `MAX_CONFIG_SOURCE_BYTES`
    (1 MiB). A schema therefore stays within 1 MiB, as one file does today;
    the 8 MiB total cap does not raise that limit.
  - **The one difference:** a directory with no `.pg` file is an error, while
    an empty query directory is the warning `query_dir_empty`. A graph has to
    have a schema.
- **Composition.**
  - The composed source is each file's bytes in order, each ending with exactly
    one newline.
  - With two or more files, each file is preceded by a banner line
    `// file: <path>`. With exactly one file there is no banner, whatever the
    form of the declaration. Every existing graph therefore keeps its digest,
    and so does a directory that holds one file.
  - `<path>` is the path discovery already records for a query file,
    `declared.join(file_name)` (`config.rs`), relative to the `cluster.yaml`
    directory and written with `/`. One file has one banner in every form:
    `schema: schema/`, `schema: [schema/]` and `[schema/a.pg, schema/b.pg]`
    compose the same bytes.
  - On the command line (`--schema <dir>`), `<path>` is the directory's final
    component joined with the file name. The same files then compose the same
    bytes from any working directory, and the same bytes as a cluster that
    declares that directory beside its `cluster.yaml`. A graph is applied
    either standalone or through its cluster, never both (`schema apply`
    refuses a cluster graph), so the two never compete for one contract.
  - Banners become part of `source_hash` for every multi-file graph, so their
    format is a compatibility surface: changing it later gives each such graph
    a new contract with no schema steps.
- **Validate and plan output.** `ResourceSummary.path` in `cluster validate`
  and `plan --json` holds the declared path for a single path. For a list it
  is absent, and a new optional `paths` field lists the composed files in
  order. Both are additive to that output.
- **What sees the composed text.** It is what `schema plan`, `schema apply`,
  the contract's `source`, `source_hash`, the `schema.<graph>` resource digest
  and `schema show` see.
- **Moving text between files changes the contract.** Moving a declaration from
  one file to another changes the composed bytes. That publishes a new contract
  with no schema steps, exactly as editing a comment does today.
- **Errors name the file.**
  - A syntax error reports `schema/crm.pg:12:5`.
  - A semantic error reports the file of the declaration it concerns.
  - A name declared twice is an error naming both files.

### Namespaces in `.pg`

```pg
namespace core {
  interface Named { name: String }
  node Project { slug: String @key  name: String }
}

namespace crm {
  node Contact implements core::Named {
    email: String @key
    name: String
  }
  node Deal { title: String @key }
  edge OwnedBy: Deal -> Contact          // crm::Deal -> crm::Contact
  edge WorksOn: Contact -> core::Project
}

node Note { body: String }               // the root namespace: today's schemas
```

- **Paths.** A namespace path is one or more lowercase segments
  `[a-z][a-z0-9_]*` joined by `::`, as in `namespace acme::crm { … }`. The path
  is flat: `acme::crm` is one namespace, and `acme` gains nothing from it. A
  segment may not be one of the `.gq` reserved words (`and`, `or`, `not`, `is`,
  `null`, `in`).
- **Blocks.**
  - Blocks do not nest.
  - Blocks with the same path merge, across files too.
  - A file may hold several blocks.
- **The root namespace.** A declaration outside every block is in the root
  namespace, where every existing schema already sits.
- **Reserved names.** `omnigraph`, `std` and segments that start with `_` are
  reserved for future built-ins.
- **`namespace` is a keyword only at the top level.** It starts a declaration
  only where `node`, `edge` and `interface` can, so a property named
  `namespace` stays valid. Type names start with an uppercase letter, so no
  existing type name can collide with it.
- **Kinds.** Node and edge names stay separate per kind, as today, inside each
  namespace. The interface rule of
  [Polymorphic types](https://github.com/ModernRelay/omnigraph/pull/884) (a new
  schema may not give an interface the name of a node or edge type) applies to
  canonical names, so `crm::Event` as an interface and `core::Event` as a node
  may coexist.
- **Edge names.** They are unique per namespace after folding the case of the
  final segment, so `crm::OwnedBy` and `hr::OwnedBy` coexist, while
  `crm::OwnedBy` and `crm::Ownedby` collide.
- **Resolving names inside a schema.**
  - An unqualified reference in namespace `N` resolves to `N::X` or to the root
    `X`. If both exist it is an error naming both; if neither exists the type is
    unknown.
  - A qualified reference resolves exactly. A reference at root level resolves
    only in the root.
  - Adding a type can therefore make a schema ambiguous. It can never silently
    change what an existing reference means.

### Namespaces in `.gq`

```gq
use crm;

query open_deals() {
  match {
    $d: Deal
    $d ownedBy $c
    $c worksOn $p
    $p: core::Project
  }
  return { $d.title, $c.email, $p.slug }
}
```

- **`use` lines.** They come at the top of a query text and apply to every query
  in it. A stored query's payload is its whole file, so the lines are stored
  with it.
- **`use` is not a reserved word.**
  - It is recognized only in the leading-statement position where `set …;`
    lines already sit (`query_file` in `query.pest`), before the first query
    declaration.
  - It does not join the reserved words that edge, property and alias names
    exclude (`and`, `or`, `not`, `is`, `null`, `in`). An edge named `Use`, a
    property `use` and an alias `as use` therefore keep working, and no
    existing query text changes meaning.
- **Name resolution.**
  - A qualified name resolves exactly.
  - An unqualified name must match exactly one type among the root namespace and
    the `use`d namespaces. More than one match is a type error listing the
    candidates; none is unknown.
  - There is no search order, no session-level default and no environment
    setting.
- **Where every form applies.** Bindings (`$c: crm::Contact`), traversals
  (`$d crm::ownedBy $c`, alternations, undirected and bound edges), `insert`,
  `update`, `delete` and `load csv … into node:crm::Contact` all take qualified
  names.
- **Telling a binding from an edge.** The parser decides by the case of the
  final segment, as it decides by the case of the whole name today.
- **Edges named by reserved words.** Today such an edge is written as a string,
  `$i "in" $b` (`query.pest`).
  - The string may hold a qualified name: `$i "crm::in" $b`.
  - The bare form `$i crm::in $b` is accepted too. After `::` the final segment
    is never read as a reserved word, because the `::` already marks a name.
  - Namespace segments themselves are never reserved words (see Paths).
- **Engine catalogue types.** The built-in types of the
  [Engine catalogue](2026-10-16-engine-catalogue.md) (`@Operator`, `@Setting`
  and the rest) keep their `@` spelling and belong to no namespace. A qualified
  name never carries `@`, so `crm::@Operator` is a syntax error, and `use` never
  affects how an `@` name resolves.
- **Breakage surfaces at plan time.** Stored queries are type-checked at
  `cluster plan`, at apply and at boot. A schema change that makes a stored
  query ambiguous is reported at plan time and never changes the query's meaning
  at run time.

### Wire and tools

- **Canonical names everywhere.** Every external surface uses the canonical name
  in the field it uses today:
  - load and export envelopes (`{"type": "crm::Contact"}`,
    `{"edge": "crm::OwnedBy"}`);
  - change-feed `type=` filters, blob selectors and `export --type`;
  - `$x.@type` values and every HTTP and MCP `type_name` field.
- **No `use` outside queries.** Load envelopes and filters have no `use`. They
  take canonical names: exact for nodes, and with the final segment
  case-folded for edges, as today.
- **Table keys.**
  - The persisted `__manifest` table key becomes `node:crm::Contact`.
  - Every reader strips the `node:`/`edge:` prefix and treats the rest as opaque.
  - The key stays one component of the `lance-namespace` table id;
    `table_id_to_key` keeps refusing multi-component ids.
- **Parsers to fix.**
  - The CLI `embed --select Type:field=value` selector, which splits on the
    first `:` (`crates/omnigraph-cli/src/embed.rs`).
  - The GQT `expect plan` scan and expand selectors, which do the same, and its
    `type` and `cast` claims, which split at the last `:` and so cut
    `crm::Contact` in two (`crates/omnigraph-gqt-core/src/plan.rs`).
  - The GQT `expect shape` type-name check (`crates/omnigraph-gqt-core/src/shape.rs`).
  - The GQT load-plan table keys, whose `type_name` check accepts only letters,
    digits and `_` (`crates/omnigraph-gqt-core/src/load_plan.rs`).
  - Under [Integer node identity](https://github.com/ModernRelay/omnigraph/pull/884),
    the `Type:number` handle parser, which must split at the last `:`.
- **Policy.**
  - A namespace confers no permission, as `implements` confers none under
    Polymorphic types. Otherwise a `schema_apply` holder could widen an
    actor's view by moving a type into a namespace.
  - Per-type visibility
    ([Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951))
    stays decided per concrete type.
  - Name resolution for a restricted actor runs on its view catalog, as Actor
    views specifies. An ambiguous name's candidate list and the
    unknown-namespace error never name a hidden type or namespace.
  - A rule written over a namespace pattern (`crm::*`) would be sugar, expanded
    to `StableTypeId`s when the policy is applied. Schema plan would report any
    move that changes the set.
  - No type name is a Cedar entity today: the policy engine builds only actor,
    action, group, graph, server and cluster entities. If per-type policy lands,
    type names become Cedar entity ids, never entity types, because Cedar
    already uses `::` for its entity types (`Omnigraph::Actor`).

### Moving existing types

```pg
namespace crm {
  node Contact @rename_from("Contact") { … }   // was the root Contact
}
```

- **What a move does.**
  - It plans as `RenameType`, and apply publishes a metadata-only `RenameTable`
    in the same manifest commit.
  - Data paths (`nodes/{type_id:016x}-{incarnation:016x}`), Lance field names
    and index names do not move.
  - Old commits keep `node:Contact` in their immutable history extents.
    Historical reads rebind by identity (`bind_catalog_aliases`), so they keep
    working.
- **Hints are canonical names.** A `@rename_from` hint is always the canonical
  name of an accepted declaration. The in-namespace reference rule never
  applies to it:
  - `@rename_from("Contact")` always means the root `Contact`;
  - `@rename_from("crm::Contact")` moves a type out of `crm`, or to another
    namespace.

  The resolver already looks a hint up as an accepted name. Its
  `IgnoredExactMatchRenameHint` diagnostic still applies when the new
  canonical name already exists.
- **A probable move without a hint fails closed.** A move without
  `@rename_from` plans today as `DropType` plus `AddType`, and a populated drop
  is accepted silently (issue #984, currently undecided). Namespaces make that
  mistake easy, so this RFC owns a narrow refusal:
  - **What is refused.** A plan that drops a populated declaration and adds one
    of the same kind with the same final segment in another namespace (root
    `X` to `crm::X`, `crm::X` to `hr::X`, or back to the root), with no
    `@rename_from`.
  - **The refusal.** A new migration code, allocated at implementation, whose
    message names the two declarations and suggests `@rename_from`.
  - **Dropping and re-creating on purpose.** Split it into two applies: drop
    in one, add in the next.
  - **Why it is safe.** The refusal infers no identity and changes nothing it
    accepts, so invariant 6 holds. Ordinary populated drops keep their current
    behavior, which #984 decides separately.
  - **No existing graph is affected.** The pattern needs a qualified name, so
    no plan that runs today can trigger it.
- **Interfaces wait.** Interfaces cannot be renamed today, so existing
  interfaces cannot move until
  [Polymorphic types](https://github.com/ModernRelay/omnigraph/pull/884) lands
  interface `@rename_from`. New interfaces may be declared in namespaces from
  the start.

## Design

### Multi-file: compose before hashing

Downstream of the loaders, everything identifies a schema by
`sha256(source bytes)`:
- the contract digest's `source_hash`;
- the cluster resource digest `schema.<graph>`;
- the invariant `resources[schema] == contract.source_hash` in
  `crates/omnigraph-cluster/src/deployment.rs`;
- drift detection in `observe` and at server boot.

Composing the files into one canonical text before that hash keeps every one of
those consumers unchanged. Keeping file boundaries after apply would instead need
a new `SchemaContractRow` field, a storage stamp bump with an upgrade route, a
new archive magic for `__history/schemas/` and a new `schema_content_hash`. File
layout is presentation, so this RFC does not pay that cost. The user's repository
remains the authority for files; the graph remains the authority for the accepted
text.

**One parse path.** The loader parses the composed text with the same function
that validates a stored contract on every open (`validate_schema_contract_row`
in `crates/omnigraph/src/db/schema_state.rs`). Applied and reopened, it
therefore gives the same answer. A source map from composed byte ranges to
`(file, line)` turns spans into file positions, the approach Gel's CLI takes.

Today only syntax errors carry a span (`ParseDiagnostic` in
`crates/omnigraph-compiler/src/error.rs`). Declarations gain a span, skipped by
serde, so catalog and identity errors can name the declaration's file.

**One loader.** A single compiler-crate function resolves a `SchemaDecl` into
the composed text and its source map. The CLI (`init`, `schema plan`,
`schema apply`, `lint`) and the cluster config share it.
- `GraphConfig.schema` becomes an untagged `SchemaDecl { Path, Many }`,
  mirroring `QueriesDecl`.
- The single-path form serializes as today, so `config_digest` and
  `validate_bundle`'s canonical-serialization check are unchanged.
- Bundles keep carrying one composed source, so remote `cluster plan` and
  `cluster apply` need no wire change.

### Namespaces: part of the name, not of identity

Both places a namespace could live were weighed against the code:

- **As a separate IR field (rejected).**
  - The resolver matches accepted declarations by `(kind, name)` and then by
    hint (`resolve` in `crates/omnigraph-compiler/src/catalog/schema_ir.rs`).
    It would match `crm::Contact` against `Contact` by name and keep the id,
    but the planner would see a change it cannot classify and refuse it
    (`schema_plan.rs`, the unclassified-change refusal).
  - IR uniqueness and sort order are by name. Two namespaces declaring `Contact`
    would need new uniqueness and sort keys, a namespace copy in every
    `TypeRefIR`, `PropertyRefIR` and `SystemFieldRefIR`, a new planner step and
    a feature name.
  - Every wire surface would still need one string per type, so the field would
    add structure that no consumer reads.
- **As part of the canonical name (chosen).**
  - `NodeIR.name` holds `crm::Contact`.
  - Uniqueness, sort order, references, the manifest's `table_key`, the rename
    machinery and every wire field work as they do now.
  - The namespace of a name is everything before its last `::`, so it is
    derived, never stored. A namespace has no object of its own: it exists while
    a declaration names it, and there is nothing to create or drop. Gel's
    module-rename and empty-module problems have no counterpart.

**Feature name.** The IR's `required_features` derives `namespaces` whenever a
declaration name contains `::`. An older binary then refuses the graph by
unknown feature name, a clear reason, instead of failing to re-parse the stored
source or reporting a hash mismatch. A graph that declares no namespace derives
no feature, so its IR and hash stay byte-identical, the same posture as
`system-columns` and `edge-keys`.

**Delimiter `::`.**
- `.` is property access in `.gq` (`$p.name`) and the nested-field path
  separator in Lance, which also refuses `.` in top-level field names
  (`lance-core` `Schema::validate`). Protobuf's dotted partial names produced
  its first-component lookup trap, and Postgres needs `OPERATOR(schema.op)` to
  disambiguate.
- `/` collides with `//` comments and with URL paths.
- `__` is the system-column prefix.
- `:` alone already marks bindings, parameters, property matches and `node:`
  load targets.
- `::` appears in neither grammar today. It is what Gel, the closest prior art,
  uses, as do Rust and Cedar, and `node:crm::Contact` still parses with every
  existing `strip_prefix`.

**Resolution without a search path.** Postgres' `search_path` lets a type added
early on the path capture names that resolved elsewhere (CVE-2018-1058). GraphQL
declined namespaces partly because "adding a field can break queries". Gel
resolves within one active module and never searches others.

The rule here goes one step stricter. A name either resolves to exactly one
declaration among the root and the `use`d namespaces, or it is an error.

The compiler resolves every name to its canonical name. The IR carries that
canonical name (`IROp::NodeScan { type_name }`,
`IROp::Expand { src_type, dst_type }`), and the planner and engine find tables
by it (`node:{type_name}`), not by `StableTypeId`. Four existing mechanisms keep
that safe, and the fifth arrives with Polymorphic types:
1. **One snapshot per query** (invariant 3), so the name lookup and the catalog
   agree for the whole run.
2. **Stored queries are type-checked again** at `cluster plan`
   (`validate_query_source`), at apply and at server load, so a schema change
   that breaks one is caught before it serves.
3. **The resolver gives an exact `(kind, name)` match priority over a hint**
   (`resolve` in `catalog/schema_ir.rs`). One apply therefore cannot release a
   name and reuse it with the same kind: `node Beta @rename_from("Alpha")` plus
   a new `node Alpha` is refused as "consumed by more than one desired
   declaration".
4. **The exactly-one rule above.**
5. **Registry pinning** by kind and `StableTypeId`, which
   [Polymorphic types](https://github.com/ModernRelay/omnigraph/pull/884)
   adds for stored queries.

A later schema change can produce a loud plan-time error, but not a silent
rebinding of a stored query. Carrying ids in the query IR would change the
planner and engine as well as the compiler; it is outside this RFC.

**Files and namespaces stay independent.** Protobuf made packages independent of
files and the ecosystem pushed them back together with lint (Buf
`PACKAGE_DIRECTORY_MATCH`), while Go and TypeScript fix "directory or file =
module". Here, names are an API that is persisted in table keys and history, so
tying them to file layout would turn moving a file into a migration. Gel shows
that declared namespaces with concatenated files hold up. An optional lint can
warn when a namespace spans directories or a file holds several namespaces.

Block syntax is also what keeps concatenation safe. A file-scoped header
(`namespace crm;` at the top) would leak into the next file once files are
joined, unless the loader rewrote the text, and then the stored source would no
longer be the files' bytes.

### Interaction with polymorphic types

[Polymorphic types](https://github.com/ModernRelay/omnigraph/pull/884) (draft)
makes interfaces and inline unions bindable, traversable and usable as edge
endpoints. The two RFCs are independent; neither requires the other to ship.
Where they meet, this is how they compose.

- **One rule for type references.** Every place that RFC accepts a type name
  takes a qualified one, resolved by the rules above:
  - edge endpoints and unions (`edge HeldBy: Holding -> crm::Contact | hr::Employee`);
  - `implements` lists and interface inheritance
    (`namespace crm { interface Party implements core::Entity { … } }`);
  - abstract bindings and unions in queries (`$x: core::Entity`, `$x: crm::Contact | Note`);
  - type tests (`$x is crm::Contact`);
  - typed endpoints (`to: crm::Contact($id)`);
  - edge alternations (`(crm::ownedBy | hr::ownedBy)`).

  In a union, each member resolves on its own. The grammar has one
  `qualified_type_name = (segment "::")* type_name` rule that both RFCs use, so
  they never grow two spellings. The name avoids `type_ref`, which both grammars
  already use for property and parameter types.
- **Interfaces span namespaces.** An interface's member set is its concrete
  implementors in any namespace. `$x: core::Named` reads `crm::Contact` and
  `hr::Employee` alike. Namespaces never filter membership; a query narrows
  with type tests or a union.
- **Moves keep ids; names change.**
  - Moving a type is a rename, so it keeps its `StableTypeId`, the type half of
    every packed endpoint under the integer identity format that RFC builds on.
    No edge row changes.
  - `$x.@type`, `$e.@src_type` and `$e.@dst_type` return the new qualified
    name, while `$x.@type_id` stays the same. Clients that cache or route by
    type should key on `@type_id`, as that RFC advises.
  - A move also respells integer-identity handles. The `@id` of every unkeyed
    node and every edge of the moved type, and `@src` and `@dst` where they
    render an unkeyed endpoint, change from `Note:10` to `crm::Note:10`, and
    the old spelling is not an alias. A client that keeps handles re-reads
    them after a move.
  - Ordering ties break on the packed reference, not on the name, so a move
    does not reorder results.
- **Interface moves wait for interface renames.** Moving an existing
  interface into a namespace needs interface `@rename_from`, which that RFC
  adds. New interfaces may be declared in namespaces from the start.
- **Registry pinning covers re-resolution.** That RFC records, for each stored
  query, the kind and `StableTypeId` of every type and edge name it resolves
  (bindings, traversal edges, type tests, typed endpoints, and mutation and
  load targets), and refuses when one later resolves to a different kind or
  id. Its migration table refuses reusing a released name in one apply only
  as a different kind, so a same-kind re-add, such as moving `OwnedBy` into
  `crm` and adding a new root `OwnedBy`, is caught by pinning alone. Together
  with the exactly-one-match rule:
  - A move keeps every stored query that still resolves the name, for example
    through `use crm;`, because the id is unchanged.
  - A query that named the old unqualified `Contact` without `use crm;` fails
    to typecheck at plan time.
  - A new root `Contact` added in the same apply makes `use crm;` queries
    ambiguous, and registry pinning refuses queries whose `Contact` would now
    bind a different id.

  No path changes a stored query's meaning silently. Ad-hoc query text is
  resolved again on every run, so the same-kind re-add above retargets it;
  only stored queries are protected.
- **Released names.** That RFC refuses reusing a name released by a rename or
  drop as a different kind in the same apply. A move into a namespace releases
  the root name, so the same rule applies to it.
- **Catalog route.** The structured route that RFC adds lists canonical names
  next to each `type_id`. A shell groups types by namespace by splitting on the
  last `::`; the route needs no namespace object, because namespaces have none.
- **Change-feed filters.** That RFC accepts an interface name as a feed or
  export filter, expands it per commit, and refuses with
  `ChangeScopeMembershipChanged` when membership changes. A namespace filter
  (`type=crm::*`) would reuse that mechanism unchanged. It is left as an
  unresolved question here.
- **Policy.** Visibility stays per concrete type for both. Neither
  `implements` nor a namespace grants or removes it (see Wire and tools).

### Lance

Type names never reach Lance:
- Data paths are id-derived (`table_path_for_identity` in
  `crates/omnigraph-catalog/src/lib.rs`).
- Lance field names are property names plus system columns.
- Index names default to `{column}_idx` (OmniGraph passes `name: None`).

Namespaces are type-level, so no Lance schema, file or index changes, and
property names keep refusing `.`.

Lance's own namespace specification is a hierarchical catalog of tables:
- identifiers are lists of names joined by `$`;
- its directory layout names table directories `<hash>_<object_id>`;
- it offers per-table and batch table operations.

Using it for type namespaces would derive storage locations from names, against
invariant 6. It would also put a second catalog beside `__manifest`, which the
deny-list rejects. Its plausible fit is graph discovery across a cluster, as a
derived read-only view, which belongs to the cross-graph reference work, not to
this RFC. OmniGraph keeps using `lance-namespace` only for its request models,
with a one-component id.

## Invariants

- **2 and 4, one publication.** A move is a set of `RenameType` steps that one
  schema apply publishes in one `__manifest` CAS, together with its
  `RenameTable` rows.
- **6, identity from stable ids.** A namespace is part of a name. Moves are
  renames that must be declared; nothing infers identity from a name, a
  namespace, a file or a path.
- **8, loud failures.** These are typed refusals:
  - ambiguous names and unknown namespaces;
  - duplicate declarations across files, and a file named twice;
  - reserved names;
  - a probable move without `@rename_from`.
- **9, typed semantics.** `use` and qualified names are AST structures.
  Resolution finishes in the compiler against the query's snapshot. The IR and
  execution receive only resolved canonical names, never a relative or
  unresolved one.
- **12, one source of truth.** No namespace registry exists, and the composed
  source is the only stored schema text. File layout is not persisted; the
  repository owns it.
- **13, evidence.** See below.

## Compatibility and reversibility

- **Multi-file schemas.**
  - **No format changes.** There is no storage stamp, IR or ledger field
    change, and a single-file schema keeps byte-identical source and digests.
  - **The `cluster.yaml` change.** It gains an untagged alternative, a minor
    change under the compatibility-surfaces RFC.
  - **The single-path form is safe across versions.** A file or one directory
    stays a string in `config_semantics`, the JSON of the whole
    `RawClusterConfig` that a deployment bundle carries to the server
    (`PlanRequest`, `DeploymentRequest`) and stores as `deployment/{digest}.json`.
    A server reads only the composed source from the bundle, so any version
    accepts it.
  - **The list form is a skew rule.** It is a JSON array where an older server
    expects a path. `validate_bundle` re-parses `config_semantics` into
    `RawClusterConfig`, so an older server refuses the bundle with
    `deployment_input_invalid`, and an older binary refuses a stored bundle
    that holds one. This fails closed. Use the list form only once every server
    and binary that receives or reads the cluster's bundles has this change.
  - **CLI JSON.** `cluster validate` and `plan --json` gain the optional
    `ResourceSummary.paths` field, an additive change to that surface.
- **Namespaces.**
  - PG and GQ grammars gain a block, a `use` line and qualified names, all minor
    changes. Neither adds a reserved word, so every accepted schema and query
    text parses as before.
  - The `namespaces` IR feature makes older binaries refuse a graph that uses it.
  - Wire shapes do not change, but values may contain `::`. A client that
    validates type names against `^[A-Z]\w*$` must widen its pattern.
    `openapi.json` declares no pattern for these fields.
- **Reverting.** Moving types back to the root is a rename. Removing the feature
  requires renaming every type back first, and history keeps the qualified keys
  it recorded.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| A prefix convention (`Crm_Contact`) | Works today with no engine change, and stays the baseline until a need appears. It gives no query scope, no policy scope, and edge names still collide across the graph. |
| `import "./x.pg"` between files (issue #838) | With declared namespaces, imports add cycle and order rules and nothing else. Protobuf shows file imports leak file structure to callers (`import public`, Buf `IMPORT_NO_PUBLIC`). Composition is declared once, in configuration. The cost of that choice: a `.pg` file does not describe itself, so an editor or a one-file `lint` needs the composing declaration to resolve a name from another file. |
| File or directory = namespace (Go, TypeScript) | Ties persisted names to file layout: moving or renaming a file becomes a schema migration, and templates copied into different folders get different type names. |
| File-scoped `namespace crm;` header | Breaks joining files by concatenation, unless the loader rewrites the text, so the stored source would no longer be the files' bytes. Possible later as sugar the composer expands. |
| Namespace as a separate IR field | New uniqueness and sort keys, a namespace copy in every IR reference type and a new planner step, while every wire surface still needs one string. |
| Search path, or unqualified names that are unique anywhere | Shadowing and capture (CVE-2018-1058). Adding a type changes or breaks existing queries silently. |
| Lance namespaces for type namespaces | Derives storage from names and adds a second catalog beside `__manifest`. |
| Persist file boundaries in the contract | Needs a stamp bump, an upgrade route and a new history archive format, for display value only. Deferred until a user needs `schema show` to return files. |

## Evidence and tests

Facts this design rests on, checked at `fe69c039`:
- **The manifest key is a renameable alias.** `table_key` is a `Utf8` field of
  the `__manifest` `table` row and of `__history` extents
  (`crates/omnigraph-catalog/src/record.rs`, `history.rs`). A rename rewrites
  it through `RenameTable` without moving the data path
  (`crates/omnigraph-catalog/src/lib.rs`, `publisher.rs`).
- **Change-feed cursors.** They carry a digest of the caller's type-name filter
  (`crates/omnigraph/src/changes/token.rs`). A cursor that filtered on
  `Contact` does not validate for a filter on `crm::Contact`, which is already
  true for any rename.
- **Graph-index stamps.** Its persisted stamps carry type and edge names
  (`graph_index/persist.rs`). A mismatch rebuilds the derived index.
- **Declaration order.** It does not matter within a file: interfaces are
  collected before resolution, and edge endpoints are checked after every node
  is known. Joining files therefore cannot change meaning.

Tests extend the existing owners in [testing](../dev/testing.md):
- **Compiler.**
  - Schema parser tests: blocks, merged blocks, reserved and malformed paths,
    and qualified references.
  - Catalog tests: edge folding per namespace, and kind separation.
  - `schema_ir` tests: the `namespaces` feature, byte-identical IR without it,
    and a move into a namespace producing `RenameType`.
  - Query parser and typecheck tests: `use`, ambiguity, unknown namespaces, and
    the binding/edge choice by final segment.
- **Cluster.**
  - Config tests that mirror the `QueriesDecl` tests: directory, list, symlink
    and `..` refusal, caps, an empty directory, a file named twice, sorted
    order, and an unchanged digest for the single-path form.
  - `schema: schema/`, `schema: [schema/]` and the explicit file list compose
    identical bytes, and the composed 1 MiB bound refuses one byte more.
  - The list form sent through `validate_bundle`, pinning the skew rule.
- **Refusal and compatibility.**
  - `crates/omnigraph/src/db/catalog_tests.rs` already proves that an unknown
    feature name refuses open before any effect. Adding `namespaces` there pins
    the older-binary refusal; the envelope gate in `schema_state.rs` runs before
    the source is parsed.
  - A probable move without `@rename_from` is refused with its code, while an
    ordinary populated drop plans as it does today.
  - `.gqt` cases under `crates/omnigraph-gqt/cases/` for the ambiguity,
    unknown-namespace and reserved-segment errors.
  - A regression row proving that a property named `namespace` or `use`, an
    edge `Use` and the alias `as use` keep parsing. All four work today.
- **CLI.**
  - `--schema <dir>` for every command that takes a schema.
  - Errors with `file:line`.
  - The `embed --select` selector with a qualified type.
- **Engine.**
  - `tests/schema_apply.rs`: a move keeps the table identity, data files, row
    ids and indexes, and publishes once.
  - Historical reads across a move.
  - Export and load round-trip with qualified names.
  - A change-feed cursor across a rename.
- **GQT.** Cases with a namespaced schema and queries, plus `expect shape` and
  `expect plan` claims that spell qualified names.
- **Composition.**
  - Composing the files gives the same schema shape as each file parsed alone and
    merged, for every order of the files.
  - Recompiling the stored composed text gives the shape the apply produced.

## Rollout

1. **Multi-file schemas.**
   - The shared loader and source map, `SchemaDecl` in `cluster.yaml`, and
     `--schema <dir>`.
   - No format change.
2. **Namespaces.**
   - `.pg` blocks and the `namespaces` IR feature.
   - The refusal of a probable move without `@rename_from`, in the same change
     as the blocks.
   - `.gq` qualified names and `use`.
   - Qualified names on every wire surface.
   - The selector and GQT parser fixes.
3. **Moving interfaces.** Follows interface `@rename_from` in Polymorphic
   types. Abstract bindings, unions and polymorphic endpoints take qualified
   names whenever both RFCs have shipped, in either order.
4. **Later, each on demand.**
   - Namespace-scoped per-type policy rules.
   - A layout lint.
   - A helper that writes `@rename_from` hints for a bulk move.
   - A stamp-gated `schema show --files`.

## Unresolved questions

- **Where `use` binds.** It applies per query text here. Gel binds per
  statement (`with module`). Is a per-query clause better for stored queries
  that share a file?
- **Recursive discovery.** It matches `queries:` (top level only) here. Should
  both recurse together?
- **Bulk moves.** Are per-type `@rename_from` hints enough, or is a block-level
  form wanted, such as Terraform's `moved` blocks?
- **Case of namespace segments.** Lowercase keeps the binding/edge choice
  trivial. Is that too strict for organizations' naming?
- **Namespace filters.** Should change-feed and export filters accept
  `crm::*`, reusing the per-commit expansion and membership refusal that
  Polymorphic types defines for interface filters?

## Decision log

2026-10-15: drafted from a code investigation at `fe69c039` covering:
- the compiler grammar, parser, catalog, SchemaIR identity and schema plan;
- every surface where a type name crosses a boundary;
- the schema loading and contract path;
- `__manifest` and `__history` persistence, and Lance 11.0.0 (field names,
  index names, the namespace specification and crate).

Prior art read: Gel modules, Prisma multi-file schemas, PostgreSQL schemas and
`search_path`, Protocol Buffers and Buf, GraphQL, ISO GQL, Neo4j graph types,
Rust, TypeScript and Go modules, Terraform `moved` blocks, dbt and LinkML.

2026-10-16: added the interaction with Polymorphic types (draft, PR #884):
one type-reference rule for unions, `implements`, type tests and typed
endpoints; interfaces spanning namespaces; moves keeping `StableTypeId` and
`@type_id`; registry pinning as the guard against re-resolution; namespaces
conferring no visibility. Also stated that `use` and `namespace` are positional
keywords, not reserved words, so existing names keep parsing.

2026-10-18: aligned with Polymorphic types and Integer node identity (PR
#884): registry pinning covers every resolved type and edge name, and a
same-kind re-add in one apply is caught by pinning alone; ad-hoc text is not
protected. A move respells integer-identity handles, which parse at their
last `:`. Added the GQT load-plan and `type`/`cast` parsers to the fix list,
view-catalog name resolution from Actor views, and the block form of a
namespaced interface declaration.

2026-10-18: answered the first review:
- linked issue #838 as the discussion and recorded which of its needs this
  design keeps, drops or decides differently;
- replaced the #984 prerequisite with a narrow refusal this RFC owns, a
  probable move without `@rename_from`;
- restated the safety argument on the mechanisms that exist, since the query
  IR carries canonical names rather than `StableTypeId`;
- stated the list-form skew rule for `config_semantics` and the new
  `ResourceSummary.paths`;
- fixed one banner path rule, the composed 1 MiB bound, the error for a file
  named twice, canonical rename hints, reserved-word segments, quoted
  qualified edges and the `qualified_type_name` rule name;
- added refusal and compatibility tests.
