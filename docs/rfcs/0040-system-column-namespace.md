---
rfc: "0040"
title: "System column namespace"
track: public
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-08-23
updated: 2026-09-09
discussion: https://github.com/ModernRelay/omnigraph/issues/529
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0040: System column namespace

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

OmniGraph reserves the leading-underscore property namespace for the system
and releases `id`, `src`, and `dst` to user schemas. The implicit stored
columns, the columns the engine adds to every table without a declaration,
are spelled `__id`, `__src`, `__dst` on newly created graphs. Today the
engine hardcodes those spellings; this RFC replaces every such assumption
with per-graph resolution by role, keyed on the graph's ***vintage***, the
spelling generation recorded in its stored schema authority. That is what
lets every existing graph keep its spellings and stay readable with no
migration. The query language gains
a ***meta-field namespace***: `$p.@id` (edges: `$e.@src`, `$e.@dst`) reads
the system identity on every graph. `$p.id` refers only to a declared user
property, on every vintage, exactly as it does today, so no meaning is ever
ambiguous. Every wire surface carries system identity in a fixed logical
envelope, the same shape on both vintages; physical spellings stay in the
bucket. The RFC also ends the schema IR's version scalar at 5: an
`ir_version` 5 IR carries a set of feature names, and every later schema
feature adds a name, never a version (Design).
At the release boundary, existing graphs and queries see no behavior change
beyond a hint added to one compiler error; the two payload changes existing
clients see are the export envelope, which carries the identity beside the
type instead of inside `data`, and the `@id` member of a projected node
object (User and operational behavior). The
`_`-prefix rejection applies to newly created graphs' schema admission.
Each graph adopts the new spellings at its own explicit
upgrade, defined below; history written before the upgrade stays readable
after it (Design).

## Motivation

A user property named `id` collides with the implicit physical id column.
Issue #529 reports the visible half: `node Grp { id: String @key }` fails
with "@key must reference declared properties" although the property is
declared. The silent half is worse: without `@key` the schema is accepted
and the table carries two `id` columns. Edge properties named `src`/`dst`
hit both failure modes. A companion patch (tracked on issue #529; Rollout
step 1), to be submitted alongside this RFC, will reserve the three names
with a clear error. Three liabilities remain:

1. Porting friction is permanent: a natural key literally named `id` is the
   default in relational tables, REST payloads, and CSV exports.
2. Per-name reservation lists: the compiler already carries one for Lance's
   `_rowid` family, with an audit obligation on every Lance version bump.
3. The names sit in the user's namespace. ArangoDB (`_id`, `_from`, `_to`),
   MongoDB (`_id`), Elasticsearch, and Lance all chose a reserved prefix;
   SQLite/DuckDB `rowid` shadowing is a documented footgun; Neo4j's one
   accessor rename took a multi-year deprecation cycle. Pre-1.0 is the
   cheap moment.

## User and operational behavior

### Schema language

New schema admission rejects property names starting with `_`: "property
name '_x' is reserved for system columns". The five Lance-name reservations
collapse into this rule. The companion patch's three-name reservation does
not: on pre-RFC graphs it survives as the upgrade trigger (below). On new
graphs `id`, `src`, `dst` are ordinary names:

```
node Grp {
    id: String @key      // legal on graphs created after this RFC
    name: String
}
```

### Query language: the meta-field namespace

System fields are read through `@`-prefixed meta-fields, a namespace user
properties can never enter:

```
return { $b.@id, $b.email }
```

`$x.@id` is a binding's system identity; `$e.@src`/`$e.@dst` are edge
endpoints; in mutation predicates, which carry no binding, the bare form
serves: `delete Person where @id = "..."`. These resolve by role, so they
work on every graph. The rule in one sentence: user things are bare names,
system things are `@name` in the language and `__name` in the bucket.

On any single graph, `id` has exactly one meaning:

| | `$p.id` / `where id = …` | `$p.@id` / `where @id = …` |
|---|---|---|
| Old-vintage graph | the unknown-property error, as today | system identity |
| New-vintage graph | a user property, or the unknown-property error | system identity |

The bare spellings never reached the system columns: the compiler resolves
`$p.id`, and a bare `id` in a mutation predicate, against declared
properties only (`resolve_expr_type` and `typecheck_mutation_predicate` in
`crates/omnigraph-compiler/src/query/typecheck.rs`), and a valid
old-vintage graph cannot contain a user `id` property (the companion patch
will enforce it at admission; this RFC's schema-authority validation
enforces it for old graphs; the already-corrupt case is refused, see
Design). Two logical spellings do reach system fields today, and both
resolve by role on both vintages: a bare binding
in a projection (`return { $p }`) yields the node object under the key `p`,
with its identity today under the object member `id` (`node_object_fields`
in `crates/omnigraph-compiler/src/catalog/mod.rs`), a member a declared
property `id` on a new-vintage graph would take, so from this release the
identity member is `@id` on both vintages, the one logical spelling this
RFC changes; and edge mutations address endpoints as `from`/`to` (`insert
Knows { from: …, to: … }`, `delete Knows where from = …`), unchanged. Where
no property `id` is
declared, the compiler's error gains a hint at every unknown-property site
(T2, T6, T11, T15) on both vintages: "type `Person` has no property `id`;
the system identity is `$p.@id`" after a binding, and the same error naming
`@id` in a mutation predicate or assignment. No query is ever silently
reinterpreted, at the release boundary or at the upgrade (Design).

### What does not change: user properties

A declared property such as `since: Date?` is untouched on every vintage:
column name, type, access (`$e.since`), constraints, wire field. The RFC
moves three engine-owned columns and reserves one prefix; the only change
user properties see is a gain: on new graphs they may be named `id`, `src`,
or `dst`.

### Wire surfaces

Every surface that carries system identity or endpoints uses a ***logical
envelope***, a payload shape whose identity and endpoint slots are named by
role and are identical on both vintages, so no client ever reads a storage
spelling. The physical spellings appear only in the bucket. The matrix, one
row per surface:

| Surface | Identity | Endpoints | User properties | Today |
|---|---|---|---|---|
| Query results | the projection's name: `return { $p }` yields the node object under `p` with its identity under the member `@id` (today `id`); `$p.@id` under `p.@id`, by the rule that puts `$p.name` under `p.name`, or its `as` alias | `$e.@src`/`$e.@dst`, likewise | under the property name | object key logical, identity member `id` |
| Mutations | `where @id = …` | `from`/`to` | bare names | `from`/`to` already logical |
| Load and export (JSONL: CLI `load`/`export`, `POST /load/ndjson`, `POST /export`) | `id` beside `type` or `edge`, outside `data` | `from`/`to` beside `edge`, as today | `data` holds user properties only | `data.id` was the identity slot |
| Change feed (RFC 0030) | `id` on the change | `before`/`after` `.endpoints.from`/`.to` | `properties`, user keys verbatim | already logical |
| Blob selectors (RFC 0033) | `id` in the selector | none | `property` | already logical |
| Merge conflicts | `entity_id` on the conflict | none | none | already logical |
| Key conflicts (RFC 0023) | `entity_id` on the conflict | none | none | already logical |
| Schema introspection (`GET /schema`) | `system_columns: {id, src, dst}` (final spelling at implementation), an optional field carrying the graph's physical spellings for tools that read the bucket directly | none | none | this RFC |

The result key is one guarantee: a projection's JSON key is its `as` alias,
else the binding's name for a bare binding, else `<binding>.<field>` with the
field spelled exactly as written.

The envelope is one guarantee: a line carrying its identity beside
`type`/`edge` and no `data.id` loads to the same rows on every vintage of
every binary from this release. Load accepts the legacy shape, `data.id` as
the identity, on old-vintage graphs only, where no user property can be named
`id`, so every existing export file keeps loading into a graph that has not
upgraded; once a graph upgrades, its own pre-release export files are refused
at `data.id`, so the restore artifact is an export taken after the release,
or the old file with `data.id` moved beside `type`/`edge`. On a new-vintage
graph `data.id` is the user property `id` when the schema declares one and an
unknown input field otherwise, under today's strict-field rule. The envelope
`id` keeps every rule `data.id` has today: a string, equal to the canonical
key value on a keyed type, else the identity as written; a non-string `id` is
refused for nodes and edges alike; on an old-vintage graph a line carrying
both `id` and `data.id` is refused. The `data.src` and `data.dst`
reservation, which holds those names for structural state, applies on
old-vintage graphs only. Export emits the new envelope on
every vintage from this release; an export written by this release loads
into any binary of this release or later and never into an older one: its
strict loader refuses the file at the first envelope carrying `id`
("unknown top-level graph batch field 'id'") before any effect, and its
lenient loader ignores the key, deriving keyed identities from the key and
minting fresh ones for unkeyed rows, whose edge references then dangle. The
introspection field exists for raw-storage tools; no other API payload
carries a spelling, and the spellings cannot appear in the `.pg` source,
since system columns are undeclarable.

### Existing graphs

Nothing changes at the release boundary. A graph changes only at its
***upgrade***, the explicit operation that renames its system columns, run
by the owner (mechanism in Design). A schema apply that declares a freed name on a
pre-upgrade graph is refused with an error naming the upgrade as the fix;
the upgrade never runs implicitly. Stored queries, application queries, and
dashboards need no change: no query text spells a system column, because the
bare spellings never resolved to one (Query language), and meta-fields and
`from`/`to` resolve by role on both vintages, so every query valid before
the upgrade is valid after it, and every payload keeps the shape it had
before the upgrade (Wire surfaces; the export envelope changes at the
release, not at the upgrade). What the owner accepts is the preflight in
Design: a property named `_x`, or `__id`, `__src`, `__dst`, must be renamed
before the upgrade runs, and a graph carrying a non-main branch must merge or
delete it first. After it, the graph behaves like a new one.

## Design

### Prefix reservation, keyed on vintage

Admission rejects `_`-leading property names as a semantic check after
parse (a grammar rejection would give an unhelpful error). Schema authority
validation repeats it, and on both paths the rule is vintage-keyed: new
graphs get the prefix rule; old graphs keep the historical rules, the exact
five Lance names (a legally declared `_row_id` stays readable) plus the
three-name collision rule, whose error names the upgrade as the fix.
Brand-new graphs always admit under the new rule.

### `ir_version` ends at 5

The accepted schema IR gains a ***feature set***, a set of feature names
stamped as exactly the capabilities the graph requires; a binary refuses
an IR carrying any name it does not know, the same fail-closed refusal
`ir_version` gives today, per name instead of per number. `ir_version` 5
marks an IR as carrying the set and is the scalar's last value: 2 and 4
keep the meanings RFC 0044's merged text assigns them (2 the base, 4 the
edge-key number, implemented in #593), 3 stays burned (RFC 0054's
Compatibility boundary: refused before effects, never reinterpreted), and
every schema feature after this RFC adds a name, never a version.

From this RFC's release the stamping rule is total. An accept whose
derived set is empty stamps `ir_version` 2, exactly today's base; an
accept requiring any name stamps 5 with the set; 4 is never emitted
again; 5 with an empty set is refused as malformed. Stamps therefore
move in both directions, as under RFC 0044's re-stamp rule, which this
subsumes: an old-spelling graph whose last keyed edge type is removed
returns to 2 and to the reach of every older binary. The set exists only
at 5: an `ir_version` 2 or 4 IR carrying a set field is refused, an
absent set serializes to exactly today's IR bytes, and a binary too old
to know the set refuses on the number before ever reading it. On load, a 4
IR is accepted exactly when it declares an edge key and carries no set
(RFC 0044's derivation check, kept for the number it minted), and a 2 IR
exactly when it declares no edge key and carries no set; the stamping rule
never emits 4, the load rule never refuses it.

This RFC mints two names. `system-columns` marks the new spellings; it
is recorded at graph creation or added by the upgrade, never by an
ordinary apply. `edge-keys` is the spelling of RFC 0044's capability on
set-carrying graphs. The set is derived, not authored: apart from
`system-columns`, membership is recomputed from the schema's
declarations at every accept and again by `validate_schema_ir` on load,
so a hand-authored IR whose set mismatches its declarations is refused.
An old-spelling graph that later requires a new capability moves to
`ir_version` 5 carrying its required names, without `system-columns`;
its spellings do not change, because the vintage keys on
`system-columns` membership, never on the number. This is what a single
counting scalar cannot express: old-spelling graphs are permanent and
keep gaining capabilities, so the spelling fact cannot be a position in
an ordering.

Future names follow one rule: a name is minted in the Design section of
the RFC that owns its capability, spelled kebab-case, one name per
capability, and registered in the RFC registry's index
(`docs/rfcs/README.md`) beside the owning RFC's row, so a duplicate
claim is caught where number collisions are caught today.

Two pieces of RFC 0044's merged text are superseded from this RFC's
acceptance, with the same explicitness as the 0028 amendment in
Invariants: its highest-required-version stamping rule (subsumed by the
total rule above, direction preserved) and its deferral of the
cross-feature scheme to a dedicated versioning RFC. The numbers 2 and 4
and their meanings are untouched, and 3 stays refused. The amendment to
RFC 0044's text (minting
`edge-keys`, retiring the deferral and its pointers to "RFC 0040's
unresolved question 1", which in the merged text tracked the constraint
spelling, never the joint numbering (settled in Design; Decision log
2026-09-01)) lands with or before RFC 0044's
acceptance; until
then, RFC 0044's deferral text stands and this section is the proposed
resolution it defers to. RFC 0044's pointers to "RFC 0040's unresolved
question 1" resolve to this section until that amendment lands.

### Per-graph role resolution

The compiler models system columns as roles (`SystemFieldRole::Id`/`Src`/
`Dst`) and, per RFC 0028, treats user column names as spellings over stable
identities. This RFC extends that to system columns (amending 0028 once,
see Invariants): no code path may assume a system column's spelling. The
vintage is concretely the `system-columns` membership in the graph's
accepted schema IR: absent, including on every graph at `ir_version` 2
or 4, the vintage is old; present, new. New graphs record `ir_version` 5
with `system-columns` in the set. Ordinary schema applies never change
`system-columns` membership (a required change to the resolver, which
today stamps `required_ir_version`, recomputed from the declarations
alone on every resolution, and must instead preserve `system-columns`
while recomputing the derived members), so no unrelated apply can flip
spellings; only the
upgrade adds the name. One graph is always
internally uniform, and a graph with no schema apply keeps its IR bytes
and hash identical to today's. The catalog, a projection of the IR, is the
one resolution point, and builds its Arrow schemas with the vintage's
spelling. The reroute worklist is the grep
`git grep -E '"(id|src|dst)"' -- crates/omnigraph/src` at `a99907b4`: 205
matching lines across 21 files, 160 across the 19 files outside the
crate's two in-tree test modules; each is rerouted through the resolution
point by this RFC. For a read pinned to a past version, the resolution
point consults the pinned image instead (Historical reads).

### Name resolution and coexistence

Meta-fields resolve by role. Bare names resolve against declared properties
only, on both vintages, as today; the bare binding in a projection and the
edge endpoints `from`/`to` resolve by role (Query language). Nothing
resolves by inspecting physical spellings. Inside the reserved namespace,
single underscore belongs to the substrate (`_rowid`), double to OmniGraph
(`__manifest`, `__graph_index`, now `__id`).

### Historical reads resolve through the pinned dataset

A read pinned to a manifest version (`snapshot_at`, and the change feed's
commit-era images) binds the current accepted catalog to that version's
dataset images by immutable table identity (`Snapshot::bind_catalog_aliases`
in `crates/omnigraph/src/db/manifest.rs`), and RFC 0030 already requires an
old entity image to be decoded with its commit-era physical schema. This
RFC adds the rule for system columns: on a pinned dataset image, each
system role's spelling is resolved from that image's Lance schema by
***stable field ID***, the per-column identifier Lance assigns at column
creation and preserves across a rename, never from the current catalog's
spelling. Concretely, the resolution point reads the role's field ID from
the Lance schema of the table's manifest-selected dataset version (the
catalog's Arrow schema carries no field IDs) and looks that ID up in the
pinned image's
schema; the name found there is the spelling to scan. The lookup lives in
the engine beside `bind_catalog_aliases`: the live table's Lance schema
supplies the role's field ID, the pinned `SnapshotDataset::schema()` supplies
the spelling, and the compiler's `Catalog` never sees a field ID. The change
feed's image hoist (`emitted_image` in
`crates/omnigraph/src/changes/enumerate.rs`) and the graph index's endpoint
projection (`crates/omnigraph/src/graph_index/mod.rs`) are two such scans.
Within one table incarnation the system roles' field IDs are invariant:
`alter_columns` preserves them, and every `Overwrite` re-derives them from a
schema that always places `id`, `src`, `dst` first. So pre-upgrade versions
spell `id`, post-upgrade versions `__id`, and both decode correctly under one
catalog. Across incarnations nothing changes: a pinned version whose table
identity the current catalog no longer holds is unreadable under it, as today
(`bind_catalog_aliases`).
History written before the upgrade stays readable after it, and the change
feed crosses the upgrade commit as a compatible pair, because the upgrade
changes no logical schema, once the feed's boundary gate
(`user_schema_fingerprint` in
`crates/omnigraph/src/changes/row_compare.rs`, keyed on field names today)
excludes the system roles or keys them by role. The field ID resolves a
spelling within one table incarnation and never a type or a graph identity,
which RFC 0030 §10.1 forbids inferring from field IDs. No new durable record
is needed: the field ID is already in every Lance manifest.

### Pre-RFC graphs with colliding properties

A graph created before the companion patch can hold an accepted IR with a
user `id`/`src`/`dst` property and a duplicate physical column; it is
corrupt today (the property is shadowed). Validation refuses such IR with a
named collision error directing export and rebuild. Every guarantee in this
document is scoped to graphs that pass validation.

### Future system columns: no further versions

The `system-columns` name is the last marker this concern will mint. A
marker was needed only because the legacy names sit in the user namespace; with the prefix
reserved, every future engine-owned field takes `__name` in storage and
`@name` in the language, lands beside `__id` with no version bump and no
feature name, and cannot collide. The criterion against a feature name:
an addition old binaries simply do not read needs nothing; any
capability an old binary would misread, or drop on rewrite, mints a name
(`ir_version` ends at 5). Guideline for all future work: **engine-owned names use the
`__` prefix, surfaced as `@name`; single underscore stays with the
substrate; nothing engine-owned is added outside the reserved namespace.**
Per-name reservation lists must never come back.

### The upgrade

The upgrade is invoked explicitly through one engine operation exposed on
two surfaces: a CLI command in single-graph mode, and a per-graph field in
the cluster configuration, durable declarative state applied through the
normal cluster apply in cluster mode (where HTTP schema apply is already
disabled). The trigger path never invokes it: declaring a freed name on a
pre-upgrade graph errors and names the upgrade; the layer that knows the
invocation surface adds its remediation (the CLI command, or the
cluster-config field). The cluster field is
`graphs.<graph-id>.system_columns: new` (final spelling at implementation),
declared once and left in place: an apply on a new-vintage graph is a no-op,
and on an old-vintage graph it runs the operation. The CLI form is `omnigraph
schema upgrade-system-columns <graph>` (final spelling at implementation).

**Preflight.** Before any effect, the operation validates the whole graph
against the new-vintage rules and refuses with one error listing every
offender: a declared property named `__id`, `__src`, or `__dst` (a rename
target, legal on a pre-RFC graph, where only the five Lance names were
reserved), and any property whose name starts with `_` (illegal under the
prefix rule the graph is about to adopt). The fix for both is an ordinary
pre-upgrade schema apply renaming the property with `@rename_from`; a rename
is a spelling change under RFC 0028 and legal under the old-vintage rules.
The operation also keeps schema apply's main-only precondition: a graph with
any non-main branch is refused ("schema apply requires a graph with only
main"), because each branch owns a native table fork (RFC 0042) that a rename
on main does not reach, so branches are merged or deleted first and main's
tables and stamp are the only ones renamed. The offender list is available
before any downtime through `cluster plan` and a dry-run flag on the CLI
command (final spelling at implementation, beside `omnigraph schema plan`),
under the
diagnostic code `system_columns_preflight` (final spelling at
implementation); the rename and the upgrade may share one revision, the
rename applying first. Constraint references to `src`/`dst` in `_schema.pg`
are not offenders: the operation respells them (below). In cluster mode the
operator's own `.pg` source carries the same respelling in the upgrade
revision, since a bare `src`/`dst` reference resolves against declared
properties on the upgraded graph (Compiler and language).

**Effects and their identities.** The upgrade is a schema apply in the sense
of the unified write protocol (RFC 0022): a `SchemaApply` intent is
persisted in the `__recovery/` sidecar before the first effect, and the
effects run in this order.

1. The `__manifest` internal-schema stamp on main advances from 8 to 9, by a
   schema-metadata commit on the `__manifest` dataset (today only init writes
   the stamp, inside its Create commit), recorded in the intent with the
   dataset version before and after, so the publish in (3) validates its read
   set against the post-stamp version. It publishes no graph content
   (RFC 0022 §3.5), and internal system branch refs keep their forked stamp,
   which
   the publisher's per-branch guard accepts within {8, 9}. Every binary
   that predates this RFC refuses the graph at its next open from here on,
   and a process already holding it refuses its next publish (`guard_stamp`
   in `crates/omnigraph/src/db/manifest/migrations.rs`, run by the
   publisher's `load_publish_state`; Compatibility and
   reversibility).
2. One `alter_columns` commit per node and edge table, carrying one
   rename-only alteration on a node table and three on an edge table,
   recorded in the intent with its exact transaction identity, the table's
   dataset version before and after, as the existing `SchemaApply` effect
   kinds record theirs.
3. The three staging files written and the `SchemaApply` outcome published in
   `__manifest`, as today.
4. Today's staging-to-final promotion, unchanged in shape and position:
   `_schema.ir.json` re-stamped at `ir_version` 5 with `system-columns` added
   to the recomputed set, `_schema.pg` with constraint references respelled
   to `@id`/`@src`/`@dst`, `__schema_state.json` promoted last. The graph is
   new-vintage once `_schema.ir.json` is promoted, and the window between
   the publish and the promotion is the one
   `ensure_read_only_schema_coherent` already refuses to serve. The
   respelled `_schema.pg` is this apply's desired source, so
   `validate_current_source_matches` holds after promotion.

Nothing outside the graph root is written; stored queries are
configuration and need no change (Existing graphs). The intent carries an
effect kind older binaries do not decode, which they meet only in the window
between the intent's persistence and the stamp advance: on those binaries a
read-write open
fails on the undecodable sidecar (`list_sidecars` propagates the parse
error), and a read-only open sets it aside and serves the pre-upgrade state
(`list_parseable_sidecars_for_read_only`), which is coherent because no
effect precedes the stamp advance. Evidence pins both.

**Recovery.** The rename is idempotent per table: a table whose schema
already spells `__id` is complete, one that spells `id` is pending, and the
intent's recorded transaction identities prove which is which. The
operation's only recovery outcome is therefore roll-forward: a read-write
open of a graph carrying an unfinished upgrade intent advances the stamp if
the intent records no post-stamp version, completes the
remaining renames, publishes, and promotes. No compensation path
exists, because a half-renamed graph has no consistent old-vintage state to
return to (some tables would spell `__id` under an old-vintage IR); in
RFC 0034's (draft) vocabulary the operation recovers under
`RollForwardOnly`, and the intent is durable authority, never compensated.
A read-only open cannot
roll forward and refuses a graph carrying an unfinished upgrade intent, as it
refuses an incoherent `SchemaApply` outcome today
(`ensure_read_only_schema_coherent`; unpublished ordinary effects it may
ignore, an unfinished upgrade intent it may not). That refusal reads "graph
carries an unfinished system-column upgrade; open read-write to complete it"
(final spelling at implementation), and a read-write open is what resolves
it. In cluster mode the roll-forward runs in the next read-write open, a
re-run of the same `cluster apply` revision or the server boot; `cluster
plan` opens read-only (`preview_schema_migration` and `observe_live_graph`
in `crates/omnigraph-cluster/src/config.rs`) and reports the pending upgrade
as that refusal until then; `cluster validate` opens no graph; `omnigraph
snapshot` in embedded mode opens read-write
(`crates/omnigraph-cli/src/client.rs`) and is itself a roll-forward.
Recovery runs under the same
schema gate as every other schema apply. The existing schema-apply failpoints
(`schema_apply.before_staging_write`, `after_staging_write`,
`post_sidecar_pre_effect`, `post_table_commit`, `after_manifest_commit`)
apply unchanged, plus one new point between consecutive table renames, so
the DST harness drives the half-renamed state, the state a crash-at-k-write
run also reaches.

**Lance.** Verified on Lance 11.0.0, the version `Cargo.lock` pins, in
`alter_columns` (`src/dataset/schema_evolution.rs`): a rename-only
alteration changes the field's name in place, addressed by field ID, and
commits `Operation::Project` with the new schema; fragments are rewritten
only when a column is cast, so a rename touches none; indexes are keyed by
field ID and stay attached, the function's own doc line being "If a column
has an index, its index will be preserved." An index keeps its
creation-time name (the `id` BTree index every node table carries stays
`id_idx`), and the engine already addresses indexes by field ID
(`user_indices_for_column` in `crates/omnigraph/src/table_store.rs`), never
by a name re-derived from the current spelling; that stays the rule.
Evidence pins the behavior as a Lance surface guard, so a Lance bump that
changes it fails the guard before it ships.

### Compiler and language

The `.gq` grammar gains `binding.@ident` and bare `@ident` in mutation
predicate position (`@` is free in both; today it appears only in top-level
annotations). Lowering emits role-resolved column references instead of the
literal `id`. The `.pg` grammar gains the meta-field spelling in constraint
references: `@unique(@src, @dst)` names the endpoint roles, and `@id` the
identity, on both vintages. The bare `id`/`src`/`dst` spelling in a
constraint list, which today lowers to the system roles (`field_ref` in
`crates/omnigraph-compiler/src/catalog/schema_ir.rs`), keeps that meaning on
old-vintage graphs only, where no user property can carry those names. On
new-vintage graphs it resolves against declared properties like every bare
name, so `@key(id)` names the user property `id` and is the unknown-property
error without one. The upgrade respells the old meaning (The upgrade). The
`.pg` catalog builders prepend the spelling the resolution point dictates
instead of `Field::new("id", ...)`.

## Invariants

Aligned with logical contract over physical state: the logical contract
(every table has an identity column, edges have endpoints) is unchanged;
the spelling becomes per-graph logical state resolved from the schema
authority, never inspected from storage. RFC 0028's identity model is
preserved with one stated amendment: 0028 declares the `id`/`src`/`dst`
fields cannot be renamed or supplied by a schema declaration; this RFC
amends the rename half of that sentence, applying 0028's own
names-are-spellings principle to the fields it had exempted. The
declaration half stands: system columns remain undeclarable.

This is an on-disk format change, which is on the RFC-required list and
is why this document exists; Compatibility and reversibility carries the
plan, and Evidence and tests carries its gates. Checked against
[../dev/invariants.md](../dev/invariants.md): no deny-list item is
touched, and no new background process, cache, or coordination primitive
is introduced. Invariant 6 (stable identity survives renames, not
lifetimes) is the one Hard Invariant this RFC engages, through the 0028
amendment above, and it is strengthened: system column identity becomes
role-resolved exactly as user column identity already is. No other Hard
Invariant is weakened.

Historical reads (Design) use a Lance field ID only to locate, within one
table incarnation, the spelling of a role whose identity the accepted IR has
already fixed; no identity is inferred from it, which is the reading
Invariant 6 forbids and the `omnigraph.stable_property_id` marker exists to
prevent for user properties. Invariant 6's wording gains that clause when
this RFC is accepted, and RFC 0030 §10.1's requirement not to infer graph
identity from field IDs is answered the same way.

## Compatibility and reversibility

The proposed stamp is provisionally 9 after RFC 0042's schema v8. Recheck the
next available stamp when this draft is activated.

Two fences keep a new-vintage graph away from binaries that predate this
RFC, and they act at different depths. The `__manifest` internal-schema
stamp advances from 8 to 9 on every new-vintage graph, at creation or as
the upgrade's first effect; `refuse_if_internal_schema_unsupported` reads
it as the first object-store read of both open modes, before the recovery
sweeps a read-write open runs, so every binary that predates this RFC,
whether it reads v6 (0.9.x, 0.10.x), development v7, or v8
(the 0.11.x line, RFCs 0042 and 0062),
refuses the graph before it can write anything,
with the existing ceiling refusal ("`__manifest` is stamped at internal
schema v9 but this binary expects v8", from `refuse_if_stamp_unsupported` in
`crates/omnigraph/src/db/manifest/migrations.rs`; a v6 reader names v6),
whose remedy
is the newer binary, never a rebuild. The feature set in the schema IR is
the second fence: on today's main its number is refused before recovery as
well, by `refuse_unsupported_schema_versions` in
`crates/omnigraph/src/db/schema_state.rs` (added with RFC 0054's
withdrawal), while 0.10.x reads it only after recovery; its names are read
at contract validation after recovery on every binary, and it carries the
capability knowledge. The stamp stays the first fence because renamed
physical columns are a storage-format change (`docs/dev/versioning.md`
§Changing an axis) and because the publisher's per-branch `guard_stamp` is
the one gate a process already holding the graph re-runs, where the envelope
check does not run.
Old-vintage graphs stay stamped 8 permanently, as they never gain
`system-columns`: the stamp is a storage-format fence, not a migration floor,
so `MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION` stays 8 while
`INTERNAL_MANIFEST_SCHEMA_VERSION` becomes 9, and this RFC's binary is the
first to serve two stamps. That retires the single-version contract stated
in `crates/omnigraph/src/db/manifest/migrations.rs` (its module doc, the
sub-floor refusal text, `release_for_internal_schema_version`, and the
guard's range test) and in `docs/user/operations/upgrade.md` ("one storage
format per binary", including its export-binary table), in
`docs/dev/versioning.md` (the storage row of its policy table, §Current
storage contract, and §Changing an axis), and in the doc comment on
`refuse_if_internal_schema_unsupported` in
`crates/omnigraph/src/db/manifest.rs` (every branch at CURRENT): 8 is the
one stamp
this binary can upgrade in place, through the explicit operation rather than
an open-time dispatcher. Rollout step 2 owns those rewrites.

On the second fence, old binaries refuse graphs beyond their knowledge with
the existing hard "unsupported ir_version" error:

| Binary generation | Accepts | Mechanism |
|---|---|---|
| predating RFC 0044's implementation (0.10.x and earlier) | 2 | exact-equality check on the scalar |
| implementing RFC 0044 (today's main, #593) | {2, 4} | membership check on the scalar; 3 refused (RFC 0054) |
| this RFC's | {2, 4, 5} | membership check, plus refusal within 5 of any feature name it does not know; 3 stays refused |

No binary can half-read unknown
spellings. Acceptance of 4 rides on the edge-key machinery being
present: this RFC's implementation builds on RFC 0044's (#593, on main),
and a binary that removed that machinery must refuse 4 explicitly (RFC
0044, Reversibility), because accepting a keyed graph without it
re-opens the duplicate-edge bug (#583) on exactly the tables
whose schema declares immunity. 3 is refused on every generation and
never reinterpreted (RFC 0054). A 4-stamped graph stays 4 until its
first accept under this RFC's release, which re-stamps it 5 (or 2 when
nothing requires a name) and moves it beyond the {2, 4} generation that
minted it; the move is fail-closed on those binaries. The spelling
populations are exactly two, permanently: neither is a deprecation
window, because this RFC's promise is that old graphs never migrate, and
Future system columns (in Design) adds no third. The scalar ends at 5,
so the numbering race between this RFC, RFC 0044, and their successors
is closed: the numbers 2 and 4 keep their merged meanings, 3 stays
burned, and features after this RFC add names, never versions.

Once no code assumes a spelling, the spelling is per-graph data; changing
the default for new graphs is a one-line policy change. The meta-field
namespace is additive. The prefix reservation is the least reversible piece
socially (releasing a namespace is easy, reclaiming it is not) and the one
with the strongest external precedent. The upgrade itself is not reversible
in place: its recovery is roll-forward-only (Design), and a new-vintage
graph never returns to the old spellings, since a rebuilt graph is
new-vintage and this release's export does not load into an older binary.
Rollback is restoring the whole pre-upgrade graph root with the pre-upgrade
fleet, as for every storage-format change.

## Alternatives

**Cost: two storage vintages.** Raw-Lance tools see both spellings and read
a graph's own from `GET /schema`; the engine's resolution point carries the
per-graph case forever. The alternative, a flag-day break with eager
store-wide migration, rewrites every existing graph at once; this RFC
prefers zero-action compatibility.

**Rejected: physical spellings on the wire.** Zero change to today's
payload bytes on old-vintage graphs, but every multi-graph client must read
`GET /schema` before parsing a row, and a user property named `id` can
never round-trip through load and export, the very property this RFC frees.
The change feed, Blob selectors, and merge conflicts already chose logical
envelopes; load and export join them.

**Rejected: refusing pre-upgrade history after the upgrade.** The design
without field-ID resolution: versions below the upgrade commit refused. It
fails a change-feed consumer (RFC 0030) whose cursor is behind the upgrade
commit and every time-travel query into the graph's past, for a rename
that changed no logical schema, while the field ID it would save reading is
already durable in every Lance manifest.

**Rejected: the `ir_version` envelope as the only old-binary fence.** On
0.10.x it is read after a read-write open's recovery sweeps, so that binary
would write before refusing; today's main refuses the number before recovery
but still after the stamp, which is the first object-store read on every
binary, and only the stamp is re-checked by a process already holding the
graph, at its next publish.

**Rejected: compensating a failed upgrade.** The reason is stated in
Recovery (The upgrade).

**Cost: a novel spelling.** Cypher/ISO GQL users know `id(n)`, not `.@id`.
The function spelling was rejected: it consumes generic function names and
needs a new function per future system field; the sigil reserves a
namespace once, mirroring `__`. The docs owe one line: "instead of `id(n)`,
write `n.@id`".

**Rejected: documentation only.** Leaves the friction and the audit
obligation in place.

**Rejected: the reservation as the end state.** The companion patch closes
the bug but keeps `id` unavailable forever; it is the fence that makes this
RFC's guarantees provable, not the destination.

**Rejected: user-wins shadowing.** SQLite's documented `rowid` footgun;
reopens #529's silent variant.

**Rejected: binding-as-identity** (`$a = $b` as identity equality).
Elegant but overloads bare bindings in projections; separable, composes
with the meta-field namespace.

**Rejected: a further counting version for the rename.** Any position N
on the scalar makes "old spellings plus a feature after N" inexpressible,
because old-spelling graphs are permanent and keep gaining capabilities.

**Rejected: deferring the scheme to a dedicated versioning RFC.** Same
scheme, one more document; the first capability it governs is this RFC's
own, and RFC 0044 already carries its half (4, as merged and implemented
in #593), so settling it
here keeps one owner and lets acceptance close the question.

**Rejected: a dedicated spelling flag beside the scalar.** Solves this
RFC alone with one field and no registry; but every later orthogonal
capability then needs its own ad-hoc flag plus a scalar bump to avoid
being silently ignored by older binaries, where the set gives all of
them one mechanism with per-name fail-closed refusal.

**Do nothing.** The companion patch will replace the misleading error on
its own; stopping there hardens the break's cost with every release toward
1.0.

## Evidence and tests

The gates this RFC owns, each stated beside the behavior that defines it:

- Old-vintage continuity: an old-vintage fixture graph opens and answers
  identically before and after the release (result rows and diagnostics,
  except the new hint on a bare `id` and the `@id` member of a projected
  node object), and a graph with no schema apply keeps its
  IR bytes and hash identical to today's (Per-graph role resolution).
- Refusal: the {2, 4} generation (today's main) refuses an old-vintage
  `ir_version` 5 graph, stamped 8, with the existing hard "unsupported
  ir_version" error at `refuse_unsupported_schema_versions`, before any
  write; the 2-only generation never reaches the IR, refusing at the stamp
  (Early fence); a set-carrying graph
  naming a capability the binary does not know is refused; an IR whose
  feature set mismatches its declarations is refused at accept and by
  `validate_schema_ir` on load; a 2 or 4 IR carrying a set field is
  refused; a 3 IR stays refused; a corrupt IR carrying a user
  `id`/`src`/`dst` property is refused with the named collision error
  (`ir_version` ends at 5; Compatibility and reversibility; Pre-RFC
  graphs with colliding properties).
- Stamping: a post-release accept whose derived set is empty stamps 2;
  one requiring any name stamps 5 with exactly the derived set, never 4;
  the upgrade stamps the recomputed set with `system-columns` added; an
  untouched old graph's absent set keeps its IR bytes and hash identical
  (`ir_version` ends at 5).
- Admission: a `_`-leading property name on a new graph is refused with
  the reserved-namespace error, and the five Lance names admit through
  the prefix rule alone (Schema language).
- New-graph spellings: a graph created after the release stores
  `__id`/`__src`/`__dst` (Summary; Per-graph role resolution).
- Meta-field resolution: `$p.@id`, `$e.@src`, `$e.@dst` answer on both
  vintages under the key `p.@id` (or the `as` alias); `$p.id` with no
  declared property `id` fails on both vintages
  with the unknown-property error naming `$p.@id`; `return { $p }` and edge
  `from`/`to` answer unchanged on both (Query language).
- Wire envelope: export emits the identity beside `type`/`edge` on both
  vintages; load accepts that shape on both, accepts `data.id` as the
  identity on an old-vintage graph, and on a new-vintage graph reads
  `data.id` as the user property when declared and refuses it as an
  unknown input field otherwise; a user property named `id`, `src`, or `dst`
  round-trips through export and load on a new-vintage graph (Wire surfaces).
- Historical reads: after the upgrade, `snapshot_at` a pre-upgrade version
  and a change-feed page spanning the upgrade commit both return the rows
  they returned before it, the page crossing the commit because the boundary
  gate's fingerprint no longer keys on the system roles' names (Historical
  reads).
- Early fence: a binary of the 2-only and of the {2, 4} generation refuses
  a new-vintage graph at `refuse_if_internal_schema_unsupported` with no
  object-store write, in both open modes, including a graph left
  half-upgraded (Compatibility and reversibility); on a graph carrying the
  intent but not yet the stamp, a read-write open on those
  binaries fails on the undecodable upgrade intent, and a read-only open
  serves the pre-upgrade rows (The upgrade).
- Vintage stability: an ordinary schema apply never changes
  `system-columns` membership; only the upgrade adds it (Per-graph role
  resolution names the resolver change this requires).
- Upgrade preflight: a graph declaring `__id`, `__src`, `__dst`, or any
  `_`-leading property, or carrying a non-main branch, is refused before any
  effect: the properties in one error listing every offender, the branch by
  schema apply's existing main-only error;
  a schema apply declaring a freed name on a pre-upgrade
  graph is refused with the error naming the upgrade (The upgrade; Existing
  graphs).
- Upgrade effects: after the upgrade every table spells `__id`/`__src`/
  `__dst`, the IR carries `system-columns` at `ir_version` 5, `_schema.pg`
  constraint references read `@id`/`@src`/`@dst`, the stamp reads 9, and a
  query valid before the upgrade returns the same rows after it (The
  upgrade).
- Upgrade recovery: under the DST harness, a crash at every schema-apply
  failpoint and between any two table renames leaves a graph the next
  read-write open rolls forward to the complete upgrade, never to a mixed
  state, and a read-only open of the half-upgraded graph refuses (The
  upgrade).
- Lance surface guard: on the pinned Lance version, a rename-only
  `alter_columns` commits a `Project` transaction, preserves every field ID
  and fragment, keeps each index attached under its creation-time name, and
  keeps the `lance-schema:unenforced-primary-key` field metadata on the
  renamed field; an `Overwrite` re-derives the system roles' field IDs from a
  schema that places `id`, `src`, `dst` first
  (`crates/omnigraph/tests/lance_surface_guards.rs`; The upgrade).
- Constraint spelling: `@unique(@src, @dst)` admits on both vintages; bare
  `src`/`dst` in a constraint list lowers to the roles on an old-vintage
  graph and to declared properties on a new-vintage one (Compiler and
  language).
- Surface survey: the grep pinned in Per-graph role resolution (205
  lines, 21 files at `a99907b4`) is the reroute worklist; complete when
  the same grep matches only the resolution point's own module and test
  code.

The suites these gates extend exist today:
`crates/omnigraph/tests/schema_apply.rs` and
`crates/omnigraph/tests/validators.rs` (admission and schema-authority
validation), the compiler's schema-IR unit tests beside
`validate_schema_ir`, the graph fixture suites,
`crates/omnigraph/tests/failpoints.rs` and the DST harness (RFC 0037) for
the recovery gates, `crates/omnigraph/tests/lance_surface_guards.rs` for
the Lance gate, and `crates/omnigraph-cli/tests/crossversion_upgrade.rs`
(cross-version refusal and continuity). Acceptance: every gate above lands as a test in
one of these suites, or a new suite beside them, and passes in the same
CI battery as today's. The draft implementation (#548) carries the
per-test enumeration.

## Rollout

1. The companion reservation patch lands first and ships alone: `id`,
   `src`, `dst` are refused at admission with a clear error, closing
   #529's misleading failure and fencing the coexistence guarantees this
   RFC depends on.
2. Resolution and admission (the draft implementation, #548): system
   columns resolve by role through the accepted vintage, live and
   historical (Historical reads), new graphs admit under the prefix rule,
   spell `__id`/`__src`/`__dst`, and stamp `__manifest` 9 (this binary
   serves {8, 9}), the meta-field namespace lands in `.gq` and in `.pg`
   constraint references, the wire envelope moves the identity beside
   `type`/`edge` on export and load, and the versioning machinery ships
   whole: the feature-set field, {2, 4, 5} acceptance (4 with the
   edge-key machinery of #593 present, 3 refused, per Compatibility),
   unknown-name refusal, the
   derivation check at accept and load, and the total stamping rule (2 or
   5 with the set, never 4). Old graphs and queries see no behavior change
   beyond the `@id` member of a projected node object; export consumers see
   the envelope, and the step rewrites the
   `data.id` shape in `docs/user/schema/index.md` §IDs,
   `docs/dev/ingestion.md`, and the export and load test fixtures. It also
   respells `@unique(src, dst)` and `@key(src, dst)` to the meta-field form
   in `docs/user/schema/index.md`, `docs/user/branching/merge.md`, and the
   keyed `.gqt` cases under `crates/omnigraph-gqt/cases/`, which init
   new-vintage graphs. It also
   carries the single-version-contract retirement named in Compatibility and
   reversibility: the `migrations.rs` module doc, the sub-floor refusal text,
   `release_for_internal_schema_version`, the guard's range test,
   `docs/user/operations/upgrade.md` with its export-binary table,
   `docs/dev/versioning.md`, and the doc comment on
   `refuse_if_internal_schema_unsupported`.
   `implementation` stays `in-progress`.
3. The upgrade: the engine operation with its preflight, ordered effects,
   roll-forward recovery, and `_schema.pg` respelling; its CLI and
   cluster-config surfaces; the Lance surface guard. The operating
   procedure in cluster mode is one revision: back up the whole graph root
   and the deployment bundle, stop every server serving the graph first,
   apply the revision carrying the per-graph field, boot, and resume.
   Stop-first is the step this upgrade adds to the ordinary
   apply-then-restart procedure, because a serving process holds an
   old-vintage catalog and the control plane does not fence servers, its
   one-writer boundary being operator-owned
   (`docs/dev/control-plane.md` §Concurrency); servers do not hot-reload, so
   the restart is one the control plane already requires. Completion is the
   stamp reading 9 in `omnigraph snapshot` (which opens read-write and
   completes a pending roll-forward first) and the new-vintage spellings in
   `GET /schema`'s system-column field, and the
   boot-time registry check validates the stored queries against the
   upgraded catalog as it does today. `implementation` advances to
   `complete` when this lands.

Stopping after step 1 leaves the bug fenced; stopping after step 2 leaves
every graph fully working with the upgrade not yet offered. No step
strands a graph. Release timing relative to other pre-1.0 format work is
the maintainers' scheduling call for the format batch, outside this
document.

## Unresolved questions

None.

## Decision log

- 2026-08-23: Draft opened for review as PR #546; motivating discussion on
  issue #529.
- 2026-09-01: Review (ragnorc, #546): the design approach confirmed
  (vintage-keyed resolution, meta-fields, the no-migration posture, the
  reserved `__` namespace as the home for future engine-owned columns).
  Applied from the same review: conversion to the normalized template and
  the registry row; in the conversion, the former unresolved question on
  the Lance rename moved to `blocked_on`, and the old Format activation
  and refusal material folded into Compatibility and reversibility plus
  Evidence and tests.
- 2026-09-01: The joint `ir_version` numbering with RFC 0044, raised in
  the same review, is proposed settled in this RFC (`ir_version` ends at
  4, in Design), with acceptance closing it: an `ir_version` 4 IR
  carries a derived feature-name set with fail-closed refusal of unknown
  names, the vintage keys on `system-columns` membership, and the number
  3 keeps its merged meaning, respelled as the `edge-keys` name on
  set-carrying graphs at RFC 0044's acceptance. A dedicated versioning
  RFC, a further counting version, and a dedicated spelling flag were
  considered and rejected (Alternatives).
- 2026-09-05: Post-merge amendment after the second review of PR #546
  (2026-09-01, on the pre-rebase head `cf354b67`). The sentences it
  supersedes, by section.
  Summary: "`$p.id` keeps its exact current behavior on existing graphs
  and refers only to a user property on new ones", "API payloads carry
  each graph's own column names", "no behavior change beyond a deprecation
  lint on legacy identity spellings". Query language: the coexistence row
  "the system column, as today (deprecation lint)", "so `$p.id` there
  is always the system column", "The upgrade (below) moves a graph to the
  new-vintage row", and "On new graphs, with no such property declared, the
  compiler fails with". API results: the whole section (now Wire
  surfaces). Existing graphs: the stored-query sentences from "Stored
  queries are configuration" through "is the owner's to update". Name
  resolution and coexistence: "plus, on old-vintage graphs only, the
  legacy spellings `id` (and `src`/`dst` on edges) resolve to the system
  columns" and the deprecation-lint sentence. The upgrade: "atomic with
  its normal commit discipline", every sentence on the rewrite tool and
  the two ordered revisions, and "The design assumes Lance column renames
  are metadata-only" through "with an explicit one-shot migration tool as
  fallback". Compatibility and reversibility: no sentence removed; the
  two-fence paragraph is prepended. Alternatives: the paragraph from
  "**Cost: per-vintage surfaces.**" through "raw-Lance tools see both
  spellings", now "Cost: two storage vintages".
  Evidence and tests: the Deprecation lint and Rewrite and upgrade gating
  gates, the Upgrade gate, the Meta-field resolution clause "`$p.id` on a
  new graph with no such declared property fails", the Old-vintage
  continuity clause "diagnostics may gain the deprecation lint", and the
  suites sentence. Rollout steps 2 and 3. Unresolved question 1. The
  `blocked_on` entry.
  Grounds, each read in the source at `7cf2b168`: the compiler never
  resolved `$p.id` or a bare mutation predicate to a system column
  (`typecheck.rs`), so the legacy alias, the lint, and the rewrite tool
  addressed a surface that did not exist; `capture_historical_read_view`
  binds the current catalog to `snapshot_at(version)`, so history needed
  the field-ID rule; `open_with_storage_and_mode` runs the recovery sweeps
  before the schema contract read, so the `ir_version` fence alone let an
  old binary write before refusing; load and export reserved `data.id`
  for the identity; on Lance 11.0.0 a rename-only `alter_columns` is a
  field-ID-preserving `Project`. Decided here, alternatives recorded:
  fixed logical envelopes on the wire (export changes shape on every
  vintage) over physical spellings; field-ID resolution of history
  over refusing pre-upgrade versions; the meta-field spelling
  `@unique(@src, @dst)` for constraint references over the storage spelling
  `@unique(__src, __dst)` (former unresolved question 1, which the merged
  text had left to the #548 reviewers), because the storage spelling would
  put a bucket name into `.pg` source, which Wire surfaces forbids
  everywhere else; and the two-stamp scheme, this binary serving {6, 7},
  which retires the single-version contract in `migrations.rs` and
  `docs/user/operations/upgrade.md` (owners in Compatibility and
  reversibility and in Rollout step 2) over a standalone one-shot
  converter.
- 2026-09-08: Renumbered before the amendment PR opened, against three
  merges since 2026-09-05. #593 implemented RFC 0044 and moved the
  edge-key number from 3 to 4, because the withdrawn actor-provenance
  build stamped 3 and RFC 0054 refuses every version-3 graph before
  effects; the set-carrying version is therefore 5, the Design section
  formerly titled "`ir_version` ends at 4" is "`ir_version` ends at 5",
  4 keeps its merged meaning, 3 stays burned, and the number 3 the
  2026-09-01 entry keeps as the edge-key number is today's 4. #686 (RFC 0062)
  advanced the `__manifest` internal-schema stamp to 7; the two-stamp
  scheme is therefore {7, 8}, with 8 the new-vintage stamp, and the
  {6, 7} of the 2026-09-05 entry reads {7, 8}. RFC 0054's withdrawal
  (`5e5d8a30`, 2026-09-06) put an `ir_version` envelope check before
  recovery on main (`refuse_unsupported_schema_versions`), so the
  2026-09-05 ground "the `ir_version` fence alone let an old binary write
  before refusing" holds for 0.10.x only; the stamp's reasons are restated
  in Compatibility and reversibility and in Alternatives. Reusing 4 or 7 was
  rejected on the grounds #593 gave for 3: a merged binary already stamps
  each (main since #593 and #686), and a number once stamped is never
  reinterpreted.

- 2026-09-09: RFC 0042 native retirement uses internal schema v8. This draft
  provisionally serves {8, 9}, with 9 reserved for its new-vintage storage
  meaning; historical stamp choices in earlier decision entries are unchanged.
