# Bug case study: camelCase property filters lowercased at runtime

**Issue:** [#283](https://github.com/ModernRelay/omnigraph/issues/283) (mirrored
in the dev-graph as `iss-990`)
**Reported on:** 0.7.0 (release binary)
**Status of code:** present on `v0.7.0`; fixed on branch `fix/iss-283-camelcase-filter` (read pushdown + pending mutation scan)
**Severity:** correctness — a valid, lint-clean query fails at run time.

## Symptom

A read query that filters on a **camelCase** schema field lints and plans
cleanly but fails when it executes:

```text
No field named reponame. Column names are case sensitive.
```

Minimal repro:

```pg
node SourceDocument {
  repoName: String @index
}
```

```gq
query find($repoName: String) {
  match { $d: SourceDocument { repoName: $repoName } }
  return { $d.repoName }
}
```

`omnigraph lint` passes; running the query errors. The operator workaround is to
rename the field to all-lowercase (`repo`), which is why this looked like a
schema-design quirk rather than an engine bug.

## Root cause

The filter-pushdown path builds the Lance scan predicate's column reference with
`datafusion::prelude::col(property)`:

- **Site:** `crates/omnigraph/src/exec/query.rs` — `ir_expr_to_expr`:
  ```rust
  IRExpr::PropAccess { property, .. } => Some(col(property)),
  ```
- `col(&str)` runs DataFusion's SQL **identifier normalization**
  (`Column::from_qualified_name` → `parse_identifiers_normalized(.., false)`),
  which **lowercases unquoted identifiers**. So `col("repoName")` resolves to a
  column named `reponame`.
- Lance stores columns **case-preserved** (`repoName`) and resolves them
  case-sensitively, so the scan can't find `reponame` and errors.

The IR is not at fault: the parser and lowering preserve the original case
(`property: pm.prop_name.clone()`), which is exactly why the compiler resolves
`repoName` and **lint passes**. The case is destroyed only at the
engine → Lance boundary.

There is a **second** boundary with the same root cause but a *different*
parser: the pending-batch scan in `table_store.rs::scan_pending_batches` splices
the mutation predicate string into a DataFusion `SELECT … WHERE {filter}` over a
`MemTable`, and DataFusion's SQL parser lowercases the unquoted column the same
way (`repoName` → `reponame`). See **Part 2** of the fix — it surfaces only on a
*chained* mutation that re-reads the pending side, which is why a single
update/delete on a camelCase predicate looked fine.

### Why the rest of the engine is unaffected

The two pushdown sites above were the offenders; the remaining paths already
treat column names case-sensitively and handle camelCase correctly:

- **Projection / return** uses the real Arrow field name (`f.name()`).
- **In-memory filtering** (the fallback for non-pushable predicates) looks the
  column up by the preserved property name against the batch schema.
- **The committed Lance mutation scan** (`Scanner::filter(&str)`) preserves an
  unquoted identifier's case, so committed-row matching on a camelCase predicate
  already worked.

So the read bug surfaces for predicates that *are* pushed down (e.g. an equality
on a scalar camelCase column), and the mutation bug only for the pending-side
re-scan of a chained mutation.

### Why it slipped through

The `ir_filter_to_expr` unit tests only use the all-lowercase field `count`, so
no test exercised a camelCase property. Nothing in CI compared the emitted
column name against the schema's casing.

## Fix

There are **two** engine→Lance boundaries that lose case, and they need
**different** fixes because the two consumers disagree on quoting semantics.

### Part 1 — read pushdown (`exec/query.rs`, `ir_expr_to_expr`)

Use DataFusion's case-preserving column constructor, `ident()`, instead of
`col()`:

```rust
IRExpr::PropAccess { property, .. } => Some(datafusion::prelude::ident(property)),
```

`ident()` builds `Expr::Column(Column::new_unqualified(property))` with no SQL
parse and no normalization, so the case is preserved. Property references here
are always bare column names (the variable is dropped via `..`), so there is no
qualified-name (`a.b`) handling to lose.

This is the right layer and the right shape:

- It is a **no-op for the lowercase columns that work today** (`slug`, `id`,
  `status`, …) — lowercasing those was already a no-op — so there is no
  regression risk for the common case.
- It makes pushdown **consistent** with projection and in-memory filtering,
  which already use case-preserved names.
- It also restores **index use** for camelCase columns: today such a filter
  errors before the BTREE is even considered.

### Part 2 — pending mutation scan (`table_store.rs`, `scan_pending_batches`)

`update`/`delete` predicates lower through `predicate_to_sql(..)` into a single
**SQL string** (`format!("{} {} {}", column, op, value_sql)`). That one string
is consumed by **two** different parsers, and *they disagree on what quoting
means*:

- The **committed** side passes the string to Lance's `Scanner::filter(&str)`.
  Lance **preserves an unquoted identifier's case** (so unquoted camelCase
  *already works* on the committed scan) but treats a double-quoted `"col"` as a
  **string literal** — `"repoName" = 'acme'` parses as `'repoName' = 'acme'`,
  a constant-false predicate that silently matches **zero** committed rows.
- The **pending** side splices the same string into a DataFusion
  `SELECT … FROM pending WHERE {filter}` over a `MemTable`. DataFusion's SQL
  parser **lowercases** an unquoted identifier (`repoName` → `reponame`) and
  fails to resolve against the case-sensitive `MemTable` schema.

So no single quoting choice for the column satisfies both: quoting fixes the
pending side but breaks the committed side, and vice versa. The fix keeps the
predicate **unquoted** (what the committed Lance scan needs) and makes the
*pending* context case-preserving instead, by disabling SQL identifier
normalization on its `SessionContext`:

```rust
let mut config = SessionConfig::new();
config.options_mut().sql_parser.enable_ident_normalization = false;
let ctx = SessionContext::new_with_config(config);
```

`predicate_to_sql` itself never lowercased anything (it copies the preserved
property name), so its emitted string is unchanged — it gains only a comment
recording the unquoted contract. The projection list in the same function is
already double-quoted and is unaffected (quoted identifiers are case-preserved
under either normalization setting).

Rejected alternatives: banning/normalizing camelCase at the compiler (a real
usability regression — camelCase fields are legitimate), lowercasing column
names in storage (a breaking on-disk change), merely making lint *warn* (a
band-aid that leaves the runtime broken), or **quoting the column in
`predicate_to_sql`** (empirically breaks 7 existing lowercase-column mutation
tests because Lance reads `"col"` as a string literal — see Part 2).

## Scope and caveats

- **Not Windows-specific.** The original report's environment was Windows, but
  the cause is platform-independent.
- **The mutation path was only *partially* broken, and not where first
  assumed.** The committed side of `scan_with_pending(..)` (Lance
  `Scanner::filter(&str)`) and `delete`'s `delete_where(..)` / `Dataset::delete`
  preserve an unquoted identifier's case, so a *single* `update`/`delete` on a
  camelCase predicate already worked. Only the **pending** side — the in-memory
  `MemTable` re-scan that a *chained* mutation hits — lowercased the column.
  This was confirmed empirically: a single update+delete on `repoName` passes
  unfixed; a chained update that re-reads the pending side fails with
  `No field named reponame`. The fix is Part 2 above (disable identifier
  normalization on the pending `SessionContext`), **not** quoting the column.
  The eventual MR-A migration (`delete_where` → Lance 7
  `DeleteBuilder::execute_uncommitted`, structured `Expr`) is the longer-term
  shape but is out of scope here.
- **Check the coercion lookup.** Adjacent to the fix, the literal-coercion step
  (`prop_data_type(.., schema)`, which keeps the BTREE usable) also resolves the
  column by name. Confirm it uses the preserved name; if it mishandles case a
  camelCase filter would resolve but lose its index — a silent perf regression,
  not a crash.
- **Do not use `col(r#""repoName""#)` as the general read-path fix.** Quoting
  would preserve this one name, but it routes through SQL identifier parsing and
  changes qualified-name semantics. The IR property here is already a bare
  column name, so `ident(property)` / `Column::new_unqualified(property)` is the
  precise structured expression.
- **Do not "fix" the mutation string by quoting the column.** It is tempting to
  reuse a `quote_ident` helper symmetric with `literal_to_sql`'s value escaping,
  but the column quote-rules differ between the two consumers of the predicate
  string: Lance's `Scanner::filter(&str)` reads `"col"` as a *string literal*
  (silently matching nothing), while DataFusion's `ctx.sql` reads it as a
  case-preserved identifier. Because the committed Lance scan already preserves
  the *unquoted* identifier's case, the column must stay unquoted and the
  pending DataFusion context must be told not to normalize — not the reverse.

## Validation (test-first)

1. **Red:** add an `ir_filter_to_expr` test asserting the emitted
   `Expr::Column` name for a camelCase property is `repoName`, not `reponame`.
   Fails on current code.
2. **Green:** apply the `col` → `ident` change (Part 1) and the pending-context
   `enable_ident_normalization = false` change (Part 2).
3. **End-to-end:** a camelCase `@index` field with
   `match { T { camelField: $x } }` returns the row (the unit test alone can't
   catch an engine↔Lance boundary regression).
4. **Mutation parity:** with the same camelCase field, cover:
   - `update T where camelField == $x set otherField = ...` updates the intended
     row.
   - `delete T where camelField == $x` deletes the intended row and cascades as
     expected.
   - A chained update that hits the pending side of `scan_with_pending` still
     works, so both the committed Lance scan and pending DataFusion `MemTable`
     predicate paths are case-preserving.
5. **Index preservation:** keep or add a plan/trace assertion that the
   camelCase `@index` equality predicate still reaches the scalar-index path.
   A result-only test can pass while silently falling back to a full scan.
6. Run the full engine suite (`cargo test -p omnigraph-engine`) — in particular
   the existing BTREE index-eligibility tests, which `ident()` must not disturb.
