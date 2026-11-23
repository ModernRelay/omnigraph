# System column namespace

**Decides:** system columns are roles resolved per graph, spelled `__id`, `__src` and `__dst` on graphs with the `system-columns` feature and read in the language as `@id`, `@src` and `@dst`, so `id`, `src` and `dst` are free for user properties.
**Contract:** keeps schema:1
**Code:** `crates/omnigraph-compiler/src/catalog/schema_ir.rs`, `crates/omnigraph-compiler/src/query/query.pest`, `crates/omnigraph-catalog/src/lib.rs`, `crates/omnigraph/src/loader/mod.rs`, `crates/omnigraph/src/db/omnigraph/system_column_upgrade.rs`, `crates/omnigraph-cli/src/schema_upgrade.rs`
**Cited as:** RFC 0040

## Context

A user property named `id`, or an edge property named `src` or `dst`, collided
with the implicit columns: `@key(id)` failed, and without a key the table
carried two `id` columns. A natural key named `id` is the common case in data
being ported. Reserving the three names fixes the collision but keeps them
from users forever. Existing graphs must keep their spellings and stay
readable, so the spelling cannot be a global constant.

## Decision

- No code assumes a system column's spelling. The catalog resolves the roles
  `Id`, `Src` and `Dst` from the accepted IR.
- The vintage is membership of `system-columns` in the IR's feature set:
  present means `__id`/`__src`/`__dst`, absent means `id`/`src`/`dst`. Graph
  creation records it and only the explicit upgrade adds it. An ordinary
  apply preserves it and rederives every other feature.
- On a graph with the feature, a property name starting with `_` is refused
  and `id`, `src`, `dst` are ordinary names. A graph without it keeps its
  historical reservations.
- Queries read system fields as `$x.@id`, `$e.@src`, `$e.@dst`, and bare
  `@id` in a mutation predicate. A bare name resolves only against declared
  properties, on both vintages, so no query changes meaning.
- Payloads carry identity by role. Load and export put `id` beside `type` or
  `edge` and keep `data` for user properties; a projected node object holds
  its identity under `@id`. The schema output may carry the physical
  spellings for raw-storage tools.
- `ir_version` ends at 5. An empty feature set stamps 2; any feature stamps
  5 with the set. 4 is read and never written, 3 is refused, and an unknown
  feature name is refused. A later schema capability adds a feature name,
  never a number.
- A historical image takes its spellings from its own Lance schema: the one
  field carrying the primary-key marker is `id` or `__id`
  (`system_columns_at_image`). A malformed or mixed image refuses.
- `omnigraph schema upgrade-system-columns` renames the columns by field id,
  keeping indexes, as detached commits published with the new contract in
  one commit. Preflight refuses `_`-prefixed or `__id`-style properties and
  any non-main branch, listing every offender. There is no reverse
  operation.
- A later engine-owned field takes `__name` in storage and `@name` in the
  language, with no new feature or version.

## Rejected

- **Physical spellings on the wire:** every client must read the schema
  before parsing a row, and a user property `id` could never round-trip
  through load and export.
- **Refusing history from before the upgrade:** breaks change-feed cursors
  and time travel behind the upgrade for a rename that changed no logical
  schema.
- **A further counting version for the spelling:** old-spelling graphs keep
  gaining features, so a position on one scale cannot express "old spelling
  plus a later feature".
- **A function spelling such as `id(n)`:** consumes function names and needs
  a new function per system field; the sigil reserves a namespace once.
- **User properties shadowing system columns:** the documented `rowid`
  footgun, and it reopens the silent duplicate-column case.

## Not built

- A system-column upgrade of a graph inside a cluster; the CLI refuses one.
