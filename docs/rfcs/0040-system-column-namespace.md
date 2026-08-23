# RFC 0040: System column namespace

| | |
|---|---|
| **Status** | Proposed |
| **Author track** | Public contribution |
| **Author(s)** | azim afroozeh ([`azimafroozeh`](https://github.com/azimafroozeh)) |
| **Discussion** | [Issue #529](https://github.com/ModernRelay/omnigraph/issues/529) |
| **Implementation** | to follow |
| **Number** | provisional; reserved only by merge, re-checked at PR time |

> Status is maintained by maintainers: `Proposed` while the PR is open,
> `Accepted` on merge, `Declined` on close, and `Superseded by NNNN` later.

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

OmniGraph reserves the leading-underscore property namespace for the system
and releases `id`, `src`, and `dst` to user schemas. The implicit stored
columns, the columns the engine adds to every table without a declaration,
are spelled `__id`, `__src`, `__dst` on newly created graphs. Today the
engine hardcodes those spellings; this RFC replaces every such assumption
with per-graph resolution by role, keyed on the graph's ***vintage***, the
version generation recorded in its stored schema authority. That is what lets every existing graph keep
its spellings and stay readable with no migration. The query language gains
a ***meta-field namespace***: `$p.@id` (edges: `$e.@src`, `$e.@dst`) reads
the system identity on every graph. `$p.id` keeps its exact current behavior
on existing graphs and refers only to a user property on new ones, so no
meaning is ever ambiguous. API payloads carry each graph's own column names.
At the release boundary, existing graphs, queries, and clients see no
behavior change beyond a deprecation lint on legacy identity spellings; the
`_`-prefix rejection applies to newly created graphs' schema admission. Each
graph adopts the new spellings at its own explicit upgrade.

## Motivation

A user property named `id` collides with the implicit physical id column.
Issue #529 reports the visible half: `node Grp { id: String @key }` fails
with "@key must reference declared properties" although the property is
declared. The silent half is worse: without `@key` the schema is accepted
and the table carries two `id` columns. Edge properties named `src`/`dst`
hit both failure modes. A companion patch, to be submitted alongside this
RFC, will reserve the three names with a clear error. Three liabilities
remain:

1. Porting friction is permanent: a natural key literally named `id` is the
   default in relational tables, REST payloads, and CSV exports.
2. Per-name reservation lists: the compiler already carries one for Lance's
   `_rowid` family, with an audit obligation on every Lance upgrade.
3. The names sit in the user's namespace. ArangoDB (`_id`, `_from`, `_to`),
   MongoDB (`_id`), Elasticsearch, and Lance all chose a reserved prefix;
   SQLite/DuckDB `rowid` shadowing is a documented footgun; Neo4j's one
   accessor rename took a multi-year deprecation cycle. Pre-1.0 is the
   cheap moment.

## Guide-level explanation

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
| Old-vintage graph | the system column, as today (deprecation lint) | system identity |
| New-vintage graph | a user property, or a loud unknown-property error | system identity |

The upgrade moves a graph to the new-vintage row. A valid old-vintage graph
cannot contain a user `id` property (the companion patch will enforce it at
admission; this RFC's schema-authority validation enforces it for old
graphs; the already-corrupt case is refused, see Reference), so `$p.id`
there is always the system column. On new graphs, with no such property declared, the
compiler fails with "unknown property 'id'; the system identity is
`$p.@id`". No query is ever silently reinterpreted.

### What does not change: user properties

A declared property such as `since: Date?` is untouched on every vintage:
column name, type, access (`$e.since`), constraints, wire field. The RFC
moves three engine-owned columns and reserves one prefix; the only change
user properties see is a gain: on new graphs they may be named `id`, `src`,
or `dst`.

### API results

Payloads carry each graph's own column names: existing graphs keep `id`,
new graphs carry `__id`/`__src`/`__dst`. The schema introspection
response (`GET /schema`) gains an optional field carrying the graph's
system column spellings; reading it is the discovery mechanism for
multi-graph clients, not raw storage (the spellings cannot appear in the
`.pg` source itself, since system columns are undeclarable).

### Existing graphs

Nothing changes at the release boundary. A graph changes only at its
***upgrade***, the explicit operation that renames its system columns, run
by the owner (mechanism in Reference-level design). A schema apply that declares a freed name on a
pre-upgrade graph is refused with an error naming the upgrade as the fix;
the upgrade never runs implicitly. Stored queries are configuration, not
graph state (declared in the cluster config, loaded at boot), so a
mechanical rewrite tool updates their `.gq` sources beforehand and the
upgrade refuses while any still uses a legacy spelling. The owner accepts
the remaining consequences: the graph's API fields switch, and query text
in application code and dashboards spelling `$x.id` is the owner's to
update. After it, the graph behaves like a new one.

## Reference-level design

### Prefix reservation, keyed on vintage

Admission rejects `_`-leading property names as a semantic check after
parse (a grammar rejection would give an unhelpful error). Schema authority
validation repeats it, and on both paths the rule is vintage-keyed: new
graphs get the prefix rule; old graphs keep the historical rules, the exact
five Lance names (a legally declared `_row_id` stays readable) plus the
three-name collision rule, whose error names the upgrade as the fix.
Brand-new graphs always admit under the new rule.

### Per-graph role resolution

The compiler models system columns as roles (`SystemFieldRole::Id`/`Src`/
`Dst`) and, per RFC 0028, treats user column names as spellings over stable
identities. This RFC extends that to system columns (amending 0028 once,
see Invariants): no code path may assume a system column's spelling. The
vintage is concretely the version field of the graph's accepted schema IR.
New graphs record the new version. Ordinary schema applies re-emit the
accepted version unchanged (a required change to the resolver, which today
stamps the current version constant unconditionally on every resolution),
so no unrelated apply can flip spellings; only the upgrade advances it. One graph is always internally uniform, and a
graph with no schema apply keeps its IR bytes and hash identical to
today's. The catalog, a projection of the IR, is the one resolution point,
and builds its Arrow schemas with the vintage's spelling. A grep of the
literals `"id"`/`"src"`/`"dst"` in the engine crate's non-test sources at
`a99907b4` counts roughly one hundred occurrences in 21 files; each is
rerouted through the resolution point by this RFC.

### Name resolution and coexistence

Meta-fields resolve by role. Bare names resolve against declared
properties, plus, on old-vintage graphs only, the legacy spellings `id`
(and `src`/`dst` on edges) resolve to the system columns; the vintage keys
the rule, never physical inspection. A new lint marks a bare name resolving
to a system column as the legacy spelling, surfacing where query
diagnostics already surface; whether responses carry a structured warnings
field is settled during implementation. Inside the reserved namespace,
single underscore belongs to the substrate (`_rowid`), double to OmniGraph
(`__manifest`, `__graph_index`, now `__id`).

### Pre-RFC graphs with colliding properties

A graph created before the companion patch can hold an accepted IR with a
user `id`/`src`/`dst` property and a duplicate physical column; it is
corrupt today (the property is shadowed). Validation refuses such IR with a
named collision error directing export and rebuild. Every guarantee in this
document is scoped to graphs that pass validation.

### Format activation and refusal

Old binaries validate the IR version with strict equality, so they refuse
new-vintage graphs with the existing "unsupported ir_version" error; no old
binary can half-read unknown spellings. The new binary accepts exactly two
vintages, permanently: the predecessor is not a deprecation window, because
this RFC's promise is that old graphs never migrate. The set is closed at
two (next section). Evidence gates: an old-vintage fixture opens and
answers identically before and after; a new-vintage graph is refused by the
predecessor binary; a corrupt IR is refused with the named error.

### Future system columns: no further versions

This vintage is the last one this concern will mint. Versioning was needed
only because the legacy names sit in the user namespace; with the prefix
reserved, every future engine-owned field takes `__name` in storage and
`@name` in the language, lands beside `__id` with no version bump, and
cannot collide. Guideline for all future work: **engine-owned names use the
`__` prefix, surfaced as `@name`; single underscore stays with the
substrate; nothing engine-owned is added outside the reserved namespace.**
Per-name reservation lists must never come back.

### The upgrade

The upgrade is invoked explicitly through one engine operation exposed on
two surfaces: a CLI command in single-graph mode, and a per-graph field in
the cluster configuration applied through the normal cluster apply in
cluster mode (where HTTP schema apply is already disabled). The trigger
path never invokes it: declaring a freed name on a pre-upgrade graph
errors and names the upgrade; the layer that knows the invocation surface
adds its remediation (the CLI command, or the cluster-config field). The upgrade apply renames the system columns
and advances the vintage, atomic with its normal commit discipline. Stored queries stay outside that
transaction deliberately: they are configuration the server loads into its
registry at boot, and the graph transaction has no write authority over
the operator's `.gq` sources or the cluster ledger. They are rewritten
before the upgrade by a mechanical tool over the `.gq` sources. The
rewrite operates on the parsed query, never text: binding kinds come from
the match clause; `$x.id` becomes `$x.@id`, `$e.src`/`$e.dst` the
meta-field forms, bare `id`/`src`/`dst` in mutation predicates their
meta-field forms; a query that fails to parse is reported, never guessed
at. Validation is assigned to the layers that own the state: in cluster
mode, cluster apply validates the desired revision's query sources against
the post-upgrade catalog before invoking the engine operation and refuses
with a named error listing every stored query still using a legacy
identity spelling; the existing boot-time registry check remains the
enforcement on load, and in single-graph mode that same boot check is the
gate at the next server start. Nothing mutates the registry or the query
sources except the rewrite tool. Because a serving process holds the
registry it loaded at boot, the rollout is two ordered revisions, and the
ordering is what removes every degraded interval: the first revision
ships the rewritten query sources and is followed by the restart the
cluster workflow already requires, which is safe because meta-fields
resolve by role on every vintage, so a rewritten query is already valid
on the still-old-vintage graph; the upgrade rides a later revision, and
the serving process's rewritten registry stays valid straight through the
rename. Combining both into one revision remains permitted but leaves the
running server's pre-rewrite registry failing against the renamed columns
until its restart; an operator who accepts that window is choosing it
knowingly, and the boot check refuses a stale registry on the restart
either way. The
rewrite is provably unambiguous for every graph the upgrade accepts: it
runs pre-upgrade, where this RFC's collision validation (above) refuses
IRs carrying a user property with those names, and an apply declaring a
freed name pre-upgrade is refused, so no colliding property can appear
between rewrite and upgrade; every such reference means the system column.
The design assumes Lance column renames are metadata-only; verifying that
is an implementation gate, with an explicit one-shot migration tool as
fallback.

### Compiler and language

The `.gq` grammar gains `binding.@ident` and bare `@ident` in mutation
predicate position (`@` is free in both; today it appears only in top-level
annotations). Lowering emits role-resolved column references instead of the
literal `id`. The `.pg` catalog builders prepend the spelling the
resolution point dictates instead of `Field::new("id", ...)`.

## Invariants & deny-list check

Aligned with logical contract over physical state: the logical contract
(every table has an identity column, edges have endpoints) is unchanged;
the spelling becomes per-graph logical state resolved from the schema
authority, never inspected from storage. RFC 0028's identity model is
preserved with one stated amendment: 0028 declares the `id`/`src`/`dst`
fields cannot be renamed or supplied by a schema declaration; this RFC
amends the rename half of that sentence, applying 0028's own
names-are-spellings principle to the fields it had exempted. The
declaration half stands: system columns remain undeclarable.

One deny-list item is knowingly brushed: this is an on-disk format change,
which demands compatibility, refusal, and rebuild evidence. Format
activation and refusal carries that plan. No other deny-list item is
touched; no new background process, cache, or coordination primitive is
introduced. Checked against [../dev/invariants.md](../dev/invariants.md):
no Hard Invariant is weakened and no Known Gap moves; the governing
principle is the one invariant touched, and it is strengthened.

## Drawbacks & alternatives

**Cost: per-vintage surfaces.** A `$p.id` query written against an old
graph fails loudly against a new one; multi-graph clients read spellings
from the graph metadata surface; raw-Lance tools see both spellings. The
alternative, a flag-day break with eager store-wide migration, breaks every
existing query, stored query, and client at once; this RFC prefers
zero-action compatibility.

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

**Do nothing.** The companion patch will replace the misleading error on
its own; stopping there hardens the break's cost with every release toward
1.0.

## Reversibility

Once no code assumes a spelling, the spelling is per-graph data; changing
the default for new graphs is a one-line policy change. The meta-field
namespace is additive. The prefix reservation is the least reversible piece
socially (releasing a namespace is easy, reclaiming it is not) and the one
with the strongest external precedent.

## Unresolved questions

1. Is a Lance column rename metadata-only, and do existing indexes on the
   renamed column survive it? Index names derive from column spellings and
   index replacement is by name, so the upgrade may need to rebuild or
   re-register indexes even if the data needs no rewrite. Implementation
   gate for the upgrade; fallback is the migration tool.
2. Do constraint references to endpoint columns adopt the meta-field
   spelling (`@unique(@src, @dst)`) or the storage spelling
   (`@unique(__src, __dst)`, as the draft implementation is expected to
   do)? Settled during implementation review.
3. Release timing relative to other pre-1.0 format work. Settled by the
   maintainers when scheduling the format batch.
