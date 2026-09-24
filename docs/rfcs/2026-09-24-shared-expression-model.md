---
rfc: "2026-09-24-shared-expression-model"
title: "Shared expression model"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-24
updated: 2026-09-24
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Shared expression model

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

Every `file:line` anchor into omnigraph is at upstream commit `545176bd`,
and every anchor into Lance is at crate `lance` 11.0.0, the
version in omnigraph's `Cargo.lock`, written as `lance-11.0.0/<path>`.

## Summary

GQ gets one ***expression*** type, a tree used by every clause that holds a
value or a condition. Its leaves are property accesses, system fields
(`@id`, `@src`, `@dst`), literals, parameters and `now()`, and its inner
nodes are comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`,
`starts_with`, `contains`), the null tests `is null` and `is not null`, the
Boolean operators `and`, `or` and `not`, the aggregate calls, the search
calls and the ranking calls that exist today. A
match-block filter, a mutation `where`, a `return` item, an `order` key and an
`update` assignment each hold an expression of that one type. What differs
per clause is a ***context rule***: the type the clause requires of its
expression (Boolean for a filter and a `where`, a sortable value for an
`order` key) and the node kinds it refuses (an aggregate inside a `where`, a
search call inside a `return`). The context rule is a check applied to the
one type, never a second type.

Four things change for users. A filter and a mutation `where` accept `and`,
`or`, `not` and parentheses, with the precedence `or` below `and` below `not`
below comparison. A property can be tested with `is null` and `is not null`.
A `return` item may be a comparison or a Boolean expression under an alias,
giving a `Bool` column. An `order` key may be any expression that appears
in `return`, aggregates included, or a return alias; a property key sorts
as today, returned or not. Every query that parses today parses after this
RFC, except one whose bare mutation property or return alias is one of the
five reserved words; that break is named in Compatibility. This is a consequence of the
design, not a promise this RFC makes, since GQ is pre-stable and a better
language wins over compatibility wherever the two conflict.

Inside the compiler the three AST condition types `Filter`, `MatchValue` and
`MutationPredicate` and their IR twins `IRFilter` and `IRMutationPredicate`
collapse into `Expr` and `IRExpr`. The planner's
`Predicate::Gq` wraps an `IRExpr`. The engine evaluates one enum in one place
per arm, and the mutation path stops generating Lance SQL strings: it lowers
the same `IRExpr` through the same function the read scan uses and hands the
typed DataFusion expression to Lance, whose delete builder and scanner both
accept one. Graph identity, branch scope, affected-row receipts, the single
publication door and the storage format do not change.

## Motivation

Today each clause has its own small language, and each one has run out in a
place a user noticed.

A mutation `where` is exactly one comparison: the grammar rule is
`mutation_predicate = { (meta_field | ident) ~ comp_op ~ match_value }`
(`crates/omnigraph-compiler/src/query/query.pest:72`). So
`delete Knows where from = "a" and to = "b"` fails to parse at `and`, and the
only way to write it is two statements, which the staging layer joins with
`OR` (`crates/omnigraph/src/exec/staging.rs:619`): the union, not the
intersection. That is issue
[660](https://github.com/ModernRelay/omnigraph/issues/660), reproduced on
0.11.0.

A match-block filter is exactly one comparison too:
`filter = { expr ~ filter_op ~ expr }` (`query.pest:105`). Conjunction is
clause juxtaposition, and there is no `or`, so
`$p.name = "Alice" or $p.name = "Bob"` fails to parse. That is issue
[651](https://github.com/ModernRelay/omnigraph/issues/651), which asks the
question this RFC answers: whether reads and mutations share one predicate
grammar.

An `order` key is a property access or a return alias and nothing else
(`crates/omnigraph/src/exec/projection.rs:553` and the planner's
`resolve_query`, `crates/omnigraph-planner/src/optimizer.rs:222-228`, raise
`unsupported ordering expression` for anything else; the engine's `sort` and
`hidden_columns` arms, `crates/omnigraph/src/engine/lower.rs:752-757,832-835`,
refuse a key of any other shape with an internal error, since the planner
binds every key before the engine sees it). `order { count($d) desc }`
is refused; the `.gqt` case `order_clause_aggregate_refused.gqt` pins the
refusal. That is part 3 of issue
[566](https://github.com/ModernRelay/omnigraph/issues/566).

The three gaps have one cause. A condition exists in four AST shapes: `Filter
{ left: Expr, op: CompOp, right: Expr }` (`query/ast.rs:343`), `MatchValue`
for inline binding matches (`ast.rs:321`), `MutationPredicate { property:
String, op: CompOp, value: MatchValue }` (`ast.rs:589`), and the value side of
`MutationAssignment` (`ast.rs:583`). The IR repeats the split with `IRFilter`
(`ir/mod.rs:162`) and `IRMutationPredicate` (`ir/mod.rs:50`). Each shape has its
own type-checking path (`typecheck.rs:1165` for filters, `:616` for mutation
predicates), its own lowering and its own evaluator. Adding `and` to one shape
would leave the others behind and add a fifth.

The evaluators are duplicated as well. The v2 engine's `engine/expr.rs:101-163`
is a copy of the frozen v1 `exec/projection.rs:77-135`, and
`engine/scan.rs:713-804` is a copy of `exec/query.rs:4621-4732`. The mutation
path does not use either: `predicate_to_sql` (`exec/mutation.rs:390`) prints
an `IRMutationPredicate` as a Lance SQL string, which the deny-list in the
invariants names as a rejected shape ("ad-hoc SQL or `IN (...)` string
generation where structured expressions or SIP apply",
`docs/dev/invariants.md:116`).

Column-store query engines and dataframe libraries show the alternative.
Both keep one expression tree per compiler stage and let every operator hold
expressions of that tree. The SQL engine surveyed for this RFC binds a
`WHERE` and an `UPDATE ... SET` value with the same expression-binder base; a
context is a small subclass that sets a target type and refuses a few node
kinds. The dataframe library surveyed has one expression enum whose binary
node `{ left, op, right }` carries arithmetic, comparison and Boolean
operators alike, and its filter, select, column-append, group-by and sort
plan nodes all hold expression values from one arena. A Boolean predicate
check is a rule over the plan, not a separate type. Neither engine has a
mutation-only condition type.

## User and operational behavior

### Grammar

The `expr` rule becomes a precedence ladder. Lowest binding first:

```pest
expr              = { or_expr }
or_expr           = { and_expr ~ (kw_or ~ and_expr)* }
and_expr          = { not_expr ~ (kw_and ~ not_expr)* }
not_expr          = { (kw_not ~ not_expr) | comparison }
comparison        = { operand ~ ((filter_op ~ operand) | null_test)? }
null_test         = { kw_is ~ kw_not? ~ kw_null }
operand           = { "(" ~ expr ~ ")" | now_call | nearest_ordering | search_call
                    | fuzzy_call | match_text_call | bm25_call | rrf_call | agg_call
                    | prop_access | meta_field | variable | literal
                    | reserved_property | expr_ident }
kw_and            = @{ "and" ~ !(ASCII_ALPHANUMERIC | "_") }
kw_or             = @{ "or" ~ !(ASCII_ALPHANUMERIC | "_") }
kw_not            = @{ "not" ~ !(ASCII_ALPHANUMERIC | "_") }
kw_is             = @{ "is" ~ !(ASCII_ALPHANUMERIC | "_") }
kw_null           = @{ "null" ~ !(ASCII_ALPHANUMERIC | "_") }
reserved          = { kw_and | kw_or | kw_not | kw_is | kw_null }
reserved_property = { reserved }
reserved_alias    = { reserved }
expr_ident        = @{ !reserved ~ ident }
```

`filter_op` and `comp_op` are unchanged (`query.pest:148-149`). Every
keyword in the ladder is word-bounded, as `kw_set` and `edge_ident` are
today (`query.pest:44,157`), so `nothing`, `notnull` and `android` stay
identifiers. A comparison is non-associative: `$a < $b < $c` is a parse
error, reported by the enclosing rule with pest's positional message
(`expected "}"` or similar), not a message about chained comparisons. `is
null` and `is not null` sit at the comparison level, so `not $p.email is
null` reads as `not ($p.email is null)`, and `$p.email is not null and
$p.age > 30` needs no parentheses. `and`, `or`, `not`, `is` and `null`
become reserved words in operand position and as a return alias: `expr_ident` refuses them, while
`prop_access` keeps plain `ident`, so `$p.and` stays legal after a dot. A
bare reserved word matches `reserved_property` in any operand position,
read or mutation, so the message holds for both, and the parser turns that
match into ``parse error: `and` is a reserved word; a
property of that name is written `$p.and` in a read and cannot be named
bare in a mutation `where` ``, with the word it met in place of `and`. A
reserved word declared as a return alias, `return { $p.age as and }`, is a
parse error at the declaration. After `"as"`, `projection` tries
`reserved_alias = { reserved }` before `expr_ident`, and the parser turns
that match into ``parse error: `and` is a reserved word and cannot be a
return alias``, with the word it met in place of `and`. A bare `meta_field`
is legal only under the mutation context rule; the match-filter context
refuses it (the same `T` error an unbound `ident` gets). `not` followed by `{`
stays the pattern negation `negation = { "not" ~ "{" ~ clause+ ~ "}" }`
(`query.pest:108`), which is an anti-join over a sub-pattern, not a Boolean
operator over a value; the ordered choice in `clause` tries `negation` first,
so the two never compete.

The rules that hold an expression change to hold `expr`:

| Rule today | Rule after | Context rule |
|---|---|---|
| `filter = { expr ~ filter_op ~ expr }` | `filter = { expr }` | type `Bool`; no aggregate |
| `clause = { negation \| binding \| traversal \| filter \| text_search_clause }` | `clause = { negation \| binding \| traversal \| filter }` | `text_search_clause = { search_call \| fuzzy_call \| match_text_call }` is removed: a bare search call is a `filter` whose expression is the call, lowered to that call `= true`, as today's parser lowers the standalone spelling (`query/parser.rs:579`) |
| `mutation_predicate = { (meta_field \| ident) ~ comp_op ~ match_value }` | `mutation_predicate = { expr }` | type `Bool`; leaves are the target type's properties, `@id`, `@src`, `@dst`, literals, parameters, `now()`; no binding variable, no aggregate, no search or ranking call; `starts_with` and `contains` (substring and list membership) are legal as in a filter, lowered by the same `ir_expr_to_df_expr` arms |
| `mutation_assignment = { ident ~ ":" ~ match_value ~ ","? }` | `mutation_assignment = { ident ~ ":" ~ expr ~ ","? }` | type of the assigned property; constants only (below) |
| `prop_match = { ident ~ ":" ~ match_value }` | `prop_match = { ident ~ ":" ~ expr }` | type of the matched property, or its scalar element type on a list property; constants only (below) |
| `ordering = { nearest_ordering \| (expr ~ order_dir?) }` | unchanged text; a key other than a property access, a system field or the leading search key must be a return alias or an expression present in `return` (below) | a sortable value (context rules below) |
| `projection = { expr ~ ("as" ~ ident)? ~ ","? }` | `projection = { expr ~ ("as" ~ (reserved_alias \| expr_ident))? ~ ","? }`: a reserved word is not an alias | any scalar type, `Bool` from an aliased comparison or Boolean expression included, or an aggregate; search calls refused as today (T35) |

A bare `ident` inside a mutation `where` names a property of the
statement's target type, as today; `from` and `to` stay accepted as the
legacy spellings of `@src` and `@dst` (`typecheck.rs:657`). A property whose
declared name is `and`, `or`, `not`, `is` or `null` can no longer be written
bare as an operand of a mutation `where`; `and: 1` in an assignment or a
binding match, and `$p.and` after a dot, stay legal. None of the five
words can be declared as a return alias either. The compatibility section
covers both.

Assignment values and inline binding matches take ***constants***: a
`Literal`, a declared `Param`, `now()`, and comparisons or Boolean
operators over those. A property, `@id`, `@src` or
`@dst` leaf there is `T45` (type rules below). A constant over literals only
folds at compile time, in the compiler's lowering (`ir/fold.rs`), with one
exception: a comparison of two `Date` or `DateTime` literals waits for the
run-time fold, since the compiler crate carries no calendar. A constant
that reads a parameter or `now()` cannot fold there, since compilation does
not receive parameter values (`ir/lower.rs:35`). Its value is
invocation-stable: it is a pure function of the query text and of the
parameter map fixed before the retry loop, `now()` included, so every
attempt computes the same value and a retry never re-reads a clock or a
parameter (`enrich_mutation_params` binds `now()` once before the loop,
`exec/mutation.rs:877-879,1747-1754`; the evaluation itself runs inside each
attempt, against that attempt's catalog).

The value has two consumers. An assignment hands it to the write through
`resolve_expr_value` (`exec/mutation.rs:11-23`), which accepts only a
`Literal` or a `Param` today and becomes total over constants. A
`prop_match` uses it as the right operand of its filter. A constant is
evaluated with the same operator rules as every other expression, stated
below. With `$flag` null, `true or $flag` is `true` and `false and $flag`
is `false`. A comparison with a null operand, such as `$age > 30` with
`$age` null, is null. Only a null result is a null value: `Literal::Null`
(`query/ast.rs:474`) of the type the context requires. A literal-only
constant is never null, since GQ has no `null` literal.

A null result assigned to a nullable property writes a null. A null result
in an inline match compares with null and so, by the null rules below,
selects no row. A null result assigned to a non-nullable property is
refused, loud under invariant 8 and never written as a default. Before
building the batch, the write path checks each evaluated constant against
its property's nullability and refuses a null result for a non-nullable
property with a typed error that names the property, so the user sees the
refusal as a typed outcome rather than an internal Arrow error. Arrow's
check stays as the backstop, and it is today's refusal on the scalar path:
`literal_to_typed_array` (`exec/mutation.rs:32`) builds the typed null
array, `apply_assignments` (`:496`) hands it to `RecordBatch::try_new`, and
Arrow rejects a null in a non-nullable field
(`arrow-array-58.3.0/src/record_batch.rs:348-353`), surfacing through
`OmniError::arrow_internal`. These anchors were read, not executed. The
Arrow backstop covers the scalar path only; the new check covers every
property type, `Blob` included, so a null result assigned to a
non-nullable `Blob` property is refused with the same typed error before
the batch. Clearing a nullable `Blob` stays outside this RFC: today a null
assigned to a nullable `Blob` property takes the copy-the-old-value branch
of `apply_assignments` (`exec/mutation.rs:458-477`) and keeps the old value
instead of clearing it, a pre-existing defect tracked separately.

A `prop_match` is typed apart from an assignment. An assignment takes the
property's type. An inline match on a scalar property takes the property's
type; on a list property it takes the scalar element type, so `tags:
"rust"` on a `[String]` property matches by membership, and the existing
membership checks keep refusing a list value (`T3` for a literal, `T7` for
a parameter, `typecheck.rs:936-987`). A `prop_match` lowers to
`Binary { Compare(Eq) }` with the binding's `PropAccess` on the left and
the constant on the right, or `Compare(Contains)` on a list
property, as `build_binding_filters` chooses today
(`ir/lower.rs:645-649`).

### What the new text means

Three-valued logic, the same rules the three-valued `and` of the SQL
engines and dataframe libraries surveyed for this RFC implements:

- A comparison with a null operand is null. This is today's behavior for the
  six comparisons `=`, `!=`, `<`, `<=`, `>`, `>=` on both arms: the in-memory
  arm's Arrow comparison yields null and the filter drops the row, and the
  pushed-down arm's DataFusion predicate treats null as not true. The
  in-memory `contains` and `starts_with` return false on a null operand
  today (`engine/expr.rs:203-204,238-239`) where the pushed arm yields null;
  they change to return null, so `not` over them agrees across the arms. The
  null truth-table case covers both operators.
- `x and y` is false if either side is false, true if both are true, null
  otherwise. `x or y` is true if either side is true, false if both are
  false, null otherwise. `not null` is null.
- A filter and a mutation `where` keep exactly the rows whose expression is
  true. A null result excludes the row. So `not ($p.age > 30)` does not
  select the rows whose `age` is null; that is the SQL rule, stated here so
  it is a contract and not a surprise.
- `x is null` is true when `x` is null and false otherwise; `x is not null`
  is its negation. Both yield a non-null `Bool`, so they are the one way to
  bring null rows into a predicate: `not ($p.age > 30) or $p.age is null`
  selects every row the first comparison did not.
- `null` is not a literal in GQ: `$p.x = null` is a parse error, and `is
  null` and `is not null` are the only null tests. A nullable parameter
  bound to `null` makes every comparison on it null, so `$p.x = $v` with
  `$v` null selects no row.

Type rules:

- The operands of `and`, `or` and `not` must be `Bool` or `Bool?`. There is
  no implicit conversion to Boolean; `$p.age and $p.name` is type error
  ``T41: `and` needs Bool operands, got I64 and String``, and `not $p.age`
  is ``T41: `not` needs a Bool operand, got I64`` (T40 is the highest code
  in use at `545176bd`: T39 names the exists-block outer-reference rule and
  T40 the block-aggregate comparison rules; this RFC adds T41 to T46).
- A bare `true` or `false` is the Boolean literal in every position. A
  mutation `where` on a type that declares a property named `true` or
  `false` therefore cannot tell the literal from the property, and any
  Boolean literal in it is refused rather than read as a constant that would
  select every row: ``T46: `false` is a Boolean literal here; the property
  named `false` of `Flag` cannot be named bare in a mutation `where`; rename
  it in the schema (`@rename_from`)``. `bool_lit` is word-bounded, so
  `trueish` stays an identifier. The compatibility section lists the break.
- A comparison, `and`, `or` or `not` is `Bool?` when any operand is
  nullable and `Bool` otherwise; `is null` and `is not null` are always
  `Bool`. `expect shape` writes the inferred form.
- `is null` accepts any scalar, list or vector operand, including a
  non-nullable property, where it is constant false. The one operand it
  refuses is a `Blob` property, which keeps its `T7` refusal in filters
  (`typecheck.rs:1197`).
- A comparison or Boolean expression in `return` must carry an alias;
  without one it is ``T43: a comparison in return needs an alias; write `…
  as <name>` ``. The alias is the column name in rows, `expect shape` and
  the descriptor. In an aggregate `return`, a Boolean item is refused by
  `T9` as today.
- A search, fuzzy or match_text call stands only as a top-level conjunct of
  a match filter, bare or `= true`; under `or` or `not`, or as an operand of
  another comparison, it is refused with `T38` (`typecheck.rs:1186`), whose
  text becomes ``T38: search predicates require a standalone call or `=
  true`, alone or joined by and``.
- Comparison typing is unchanged: `types_compatible`
  (`typecheck.rs:2137-2149`), the `contains` overload for lists and strings
  (`:1201-1242`), the `starts_with` String rule (`:1244-1264`), the refusal of
  list, vector and Blob comparisons (`:1197,1269,1274`).
- A mutation `where` that names a binding variable, an aggregate, a search or
  a ranking call is refused at compile time with a T-coded error naming the
  clause, as `T11` does today for an unknown property (`typecheck.rs:630`).
  `T14` keeps its text for a variable (`typecheck.rs:706`). An aggregate,
  search or ranking call in a mutation `where` or assignment is `T44`, the
  call's keyword in front: ``T44: `count` cannot appear in a mutation
  where; a where compares the row's own properties, parameters and now()``.
  A property, `@id`, `@src` or `@dst` leaf in an assignment value or a
  binding match is `T45`, the leaf in front: ``T45: `age` cannot appear in
  an assignment value; assignments and binding matches are constants per
  invocation``.

Examples. The endpoint intersection from issue
[660](https://github.com/ModernRelay/omnigraph/issues/660), on a graph of
four `Knows` edges, `ab1` and `ab2` from `a` to `b`, `ac` and `db`:

```gq
query exact() { delete Knows where @src = "a" and @dst = "b" }
```

removes `ab1` and `ab2` and nothing else: `affected: nodes=0 edges=2`. Today's
two-statement form removes four. The disjunction from issue
[651](https://github.com/ModernRelay/omnigraph/issues/651):

```gq
query either() {
    match { $p: Person  $p.name = "Alice" or $p.name = "Bob" }
    return { $p.slug }
}
```

Precedence and grouping:

```gq
query grouped($cut: I64) {
    match {
        $d: Deal
        ($d.stage = "open" or $d.stage = "paused") and not $d.amount < $cut
    }
    return { $d.slug }
}
```

A null test and a Boolean column in `return`:

```gq
query contactable() {
    match { $p: Person  $p.email is not null }
    return { $p.slug, $p.age > 30 as adult }
}
```

An aggregate in `order`, from issue
[566](https://github.com/ModernRelay/omnigraph/issues/566):

```gq
query per_market($slug: String) {
    match { $p: Person { slug: $slug }  $d: Deal  $d ledByPerson $p  $d relevantMarket $m }
    return { $m.name, count($d) as deals }
    order { count($d) desc }
    limit 25
}
```

### What a Cypher or GQL user finds different

The ladder, the three-valued null rules, `is null`, aliased Boolean columns
in `return` and the `order` rule match what Cypher and ISO GQL do. Five
things differ, each an error rather than a silent difference: a chained
comparison `1 < $x < 3` is refused, since a comparison is non-associative
here; a pattern negation needs its braces, `not { $p knows $f }`, because a
brace-less `not` applies to a Boolean value; not-equal is spelled `!=`
only, as before this RFC; an unaliased comparison or Boolean `return` item
needs an alias (`T43`); and there is no `null` literal, so `is null` and
`is not null` are the only null tests. Deleting what a pattern bound, Cypher's `MATCH … DELETE`,
is not a GQ form today and this RFC does not add it; the mutation `where`
becoming the same expression type as a read filter is the step that a
later RFC needs.

### Order keys bind against the return list

An `order` key binds by its shape, and every shape has one rule. A key that
is a property access or a system field of a bound variable keeps today's
binding against the match scope and is carried as a hidden sort column on
both engines (`engine/lower.rs:626-641,736-746,811-837`,
`exec/projection.rs:531-541`). The leading `nearest`, `bm25` or `rrf` key
is consumed by `search_node` (`optimizer.rs:173-179`) before any binding
and keeps T17, T18, T21 and T33. Every other key, an aggregate, a
comparison, a Boolean expression or any call, is resolved against the
`return` list only, the rule that an order key binds only to the return
list: first as a return alias, then as an expression equal to a `return` item,
compared as trees. A key that matches neither is a compile-time error,
``T42: order key `max($d.amount)` does not appear in return; add it to
return or order by its alias``, the key as written in the backticks. So
`order { count($d) desc }` with `count($d) as deals` in `return` sorts by
the `deals` column, and `order { max($d.amount) }` or `order { $p.age > 30
}` with no such return item is refused with that message. Some SQL engines
instead append a hidden select item for any key; this RFC keeps today's hidden
columns for property keys and adds none for any other expression, because a sort key that adds an aggregate to a
non-aggregated `return` would change the query's grouping and therefore its
rows, and because an explicit error with an obvious fix costs one cheap
retry while a silent row change costs a wrong answer.

### Engine setting

Read queries whose IR holds a filter shape v1 does not evaluate (`and`,
`or`, `not`, a null test, a bare Boolean operand, a comparison over a
comparison) execute under `set engine = v2;`. Under the default `engine = v1`
(`crates/omnigraph-compiler/src/settings.rs:340`) they are refused with the
typed error of the rule below. The refusal has one address:
`execute_on_route` (`crates/omnigraph/src/exec/query_doors.rs:230-303`) is
the one function that reads `settings.engine()` and branches to
`execute_query` for `Engine::V1` or to the v2 engine for `Engine::V2`. The
`V1` arm gains one check before `execute_query`: walk `QueryIR.pipeline`
recursively (`NodeScan.filters`, `Expand.dst_filters`, `Filter` and
`AntiJoin.inner`), `return_exprs` and `order_by`. A filter position is
accepted only in the shapes v1 evaluates today: a `Binary { op: Compare }`
root whose two operands are each a `PropAccess`, a `Literal` or a `Param`
(`now()` included), the operands v1's `evaluate_expr` supports
(`exec/projection.rs:78-139`), and the search comparison `Binary { left:
Search | Fuzzy | MatchText, op: Compare(Eq), right: Literal(Bool(true)) }`.
Every other valid expression in a filter position is refused: `and`, `or`,
`not`, a null test, a bare Boolean property, parameter or literal, and a
comparison with a comparison as an operand, such as `($p.age > 30) =
($q.age > 30)`. The walk also refuses if `return_exprs` or `order_by` hold
any `Binary`, `Not` or `IsNull` node at all (a `Binary { Compare }` root is
legal only as a filter), and if an `order` key is anything other than a
property access or system field, an alias, or the leading `nearest`,
`bm25` or `rrf` key. That leading key is exempt because `search_node`
consumes it before any binding, as on v2 (see "Order keys bind against the
return list" above). The door file
is outside the v1 freeze (`crates/omnigraph/tests/v1_frozen.rs:9-10` lists
`exec/query_doors.rs` as unpinned), so the check lands without touching a
frozen byte, and v1's evaluator in `exec/projection.rs` never sees a node it
cannot evaluate. Mutations enter through their own door and are not
gated.

The message follows one rule, which this RFC is the first instance of and
which every later v2-only feature reuses: when v1 cannot run a query and v2
can, the error says so and shows both switches, with the construct named in
front. The door returns it as `CompilerError::Plan`
(`crates/omnigraph-compiler/src/error.rs:120-121`), so the user sees
`plan error: ` before the text:

```text
compound predicates (and, or, not, is null) are not supported on engine v1; engine v2 runs them: add "set engine = v2;" before the query, or start the server with OMNIGRAPH_ENGINE=v2
```

A comparison or Boolean expression in `return` or `order` gets the same
text with `comparisons in return or order` in front, any other refused
filter shape gets it with `filters other than one comparison or search
call` in front, and any other `order`
key the walk refuses gets it with `order keys other than a property, a
system field, an alias or the leading search key` in front. The second fix
is the
process-level setting, the `engine` setting's `env`
(`crates/omnigraph-compiler/src/settings.rs:64`); it is the only fix a
stored query can take, since a stored query carries no settings (see
"Stored queries and the catalog" below). When neither engine can run the
query, the ordinary error stands alone: a type error or a parse error names
the fault and says nothing about engines, because switching would not help.
The door decides which of the two it is: it runs the compile first, so a
query that fails to compile never reaches the engine check. The setting stays explicit on purpose: v2 becomes the
default in the release after it has served one full release without a
finding from the `engine-v2` mode of the GQ logic-test matrix, and that flip is a release decision outside this RFC.
Mutations have one execution path shared by both settings, so a compound
`where` works under either.

### Errors and explain

Type errors keep the `T` code family and name the clause. Explain output
keeps its shape apart from the logical `Filter` node (see "Explain and plan
hashes" below): expressions serialize as their GQ text through `Display`
(`crates/omnigraph-compiler/src/ir/mod.rs:169-217`; the `text` field at
`crates/omnigraph-planner/src/logical.rs:112`,
`physical.rs:790-793,845-863`). `Display` prints from the tree with minimal
parentheses: a child whose operator binds looser than its parent's (`or`
under `and`, `and` or `or` under `not`) is parenthesized; a comparison or
null-test child that is an operand of a comparison or of a null test is
parenthesized too, at equal precedence, because a comparison is
non-associative; a child that repeats its parent's `and` or `or` is
parenthesized when it is the right operand, so the printed text reparses to
the same tree (`a and (b and c)` keeps its parentheses, a left-nested chain
prints bare); nothing else is, and string literals print in GQ quoting
(`ast.rs:487-511`). So the
conjunct `$d.stage = "open" or $d.stage = "paused"` prints without outer
parentheses, `($p.a = 1 or $p.b = 2) and $p.c = 3` prints with them,
`($p.age > 30) = true` and `($p.age > 30) is null` keep theirs, and
the user's redundant parentheses are not kept. A split conjunct prints as
its own filter row. `EXPLAIN_VERSION` moves to 2 with phase 1, because the
logical `Filter` node's `predicate` key becomes `conjuncts` (see "Explain
and plan hashes" below). Every `logical_hash` changes
once, with the `LOGICAL_PLAN_VERSION` bump of phase 1 (the version is the
first word the hash covers, `logical.rs:646`), and afterwards changes only
with plan shape.

## Design

### One type per stage

The AST `Expr` (`query/ast.rs:385-425`) gains two variants and loses three
sibling types:

```rust
pub enum Expr {
    // existing: Now, PropAccess, Nearest, Search, Fuzzy, MatchText, Bm25,
    // Rrf, Variable, Literal, Aggregate, AliasRef
    Binary { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
    Not(Box<Expr>),
    IsNull { expr: Box<Expr>, negated: bool },
}

pub enum BinaryOp {
    Compare(CompOp),
    And,
    Or,
}
```

`Filter` becomes `Expr` at every use: a `clause` filter is an `Expr`. `MatchValue`
is removed; `prop_match`, `mutation_assignment` and the old `mutation_predicate`
value hold an `Expr`. `MutationPredicate` is removed; `UpdateMutation` and
`DeleteMutation` (`ast.rs:570-580`) hold `predicate: Expr`. `CompOp` keeps its nine values
including the lowering-only `StringContains` (`ast.rs:350-367`).

`IRExpr` (`ir/mod.rs:356-396`) mirrors the change: `Binary { left, op, right }`,
`Not(Box<IRExpr>)` and `IsNull { expr, negated }`. `IRFilter` is removed; `IROp::NodeScan { filters:
Vec<IRExpr> }`, `IROp::Expand { dst_filters: Vec<IRExpr> }` and
`IROp::Filter(IRExpr)` hold expressions the type checker has proven Boolean.
`IRMutationPredicate` is removed; `MutationOpIR::Update` and `::Delete` hold
`predicate: IRExpr`. `IRAssignment.value` is already an `IRExpr`. The IR keeps
the tree as written; no split happens in the compiler, so `Display` prints the
tree back for explain under the parenthesization rule of the explain
section.

A bare property in a mutation `where` or assignment lowers to
`IRExpr::PropAccess { variable: <target type name>, property }` with the
physical column already substituted by the compiler: `@id` through
`physical_property` as reads do (`ir/lower.rs:796-803`), and on an edge
type `@src`/`from` and `@dst`/`to` to `system_columns.src`/`.dst`, the
mapping `predicate_to_sql` performs today (`exec/mutation.rs:396-402`).
`ir_expr_to_df_expr` then lowers it as `ident(property)`
(`engine/scan.rs:797`), the same arm a read scan uses; the binding name is
not consulted.

`IRExpr`, `IRProjection`, `IROrdering` and the types they contain implement
`PartialEq`, `Eq` and `Hash` alongside today's `Debug` and `Clone`
(`ir/mod.rs:355-404`): derived where the fields allow, with `CompOp` and
`AggFunc` gaining the `Hash` derive they lack, and by hand for `Literal`
(`query/ast.rs:472-477`), whose `Float(f64)` compares and hashes by
`f64::to_bits`, so `0.0` and `-0.0` are distinct and `NaN` equals itself.
Two expressions are equal exactly when their trees are equal under that
rule, node by node. The planner needs this in three places: matching an
`order` key to a `return` item, the `Eq` and `Hash` that `Predicate`'s
derive requires of `GqFilter` (`logical.rs:93`), and deduplicating a
conjunct that arrives twice from a match filter and an inline binding
match. Today
`GqFilter` fakes both equality and hashing by formatting the `IRFilter` with
`Debug` and comparing the strings (`logical.rs:118-135`); that wrapper
becomes a plain newtype and the string comparison is deleted. The same two
traits are the precondition for a later common subexpression elimination
pass, so no rework waits there.

The planner's `Predicate::Gq { reads, text, filter: GqFilter }`
(`crates/omnigraph-planner/src/logical.rs:110-115`) wraps `GqFilter(pub
IRExpr)`. `PhysicalNode::Filter { filters: Vec<IRFilter> }`
(`physical.rs:417-420`) becomes `filters: Vec<IRExpr>`, one entry per
top-level conjunct. `reads_of_expr` (`optimizer.rs:651-701`) walks the new
variants; every other planner walk either matches exhaustively or answers
`None` for any non-retrieval expression, which is the right answer for the
new variants.

The consumers that ask a question of one filter hold an `IRFilter` today
and one `IRExpr` conjunct after; the table lists those whose question
changes:

| Consumer | Today | After |
|---|---|---|
| `PlanSource::filter_pushable` (`crates/omnigraph-planner/src/source.rs:144`; engine answer `engine/plan_source.rs:265`) | "can the scan lower this filter?", answered by whether `ir_filter_to_expr` returns `Some` | same question of `ir_expr_to_df_expr` on one conjunct; `placement_target` first requires the conjunct to read one binding (`optimizer.rs:1162-1169`), then asks the lowering, so an `or` whose sides read one binding is pushable and one that reads two never reaches the question |
| `is_search_filter`, `is_positive_search_filter`, `search_filter_query` (`engine/search.rs:700-727`) | match `filter.left` for a search call and `filter.op == Eq`, `filter.right == true` | the compiler lowers both spellings a search predicate has, bare `search(...)` and `search(...) = true` (T38, `typecheck.rs:1186`), and the same for `fuzzy(...)` and `match_text(...)`, to one IR shape, `Binary { left: Search \| Fuzzy \| MatchText, op: Compare(Eq), right: Literal(Bool(true)) }`, the comparison shape v1's frozen search dispatch already reads (`exec/query.rs:1663-1685,4155-4156`); the three functions match that one shape on one conjunct, and `search(...) and x > 3` splits into two conjuncts before they see it |
| `ScanExec` (`engine/operators/scan.rs:42,57`) and `read_candidates` / the `Expand` destination read (`engine/operators/scan/input.rs:137,227`) | `Vec<IRFilter>` / `&[IRFilter]` evaluated conjunct by conjunct | `Vec<IRExpr>` / `&[IRExpr]`, same loop, each conjunct through the one evaluator |
| the planner's cost module (`cost.rs:309`, `equated_property(&IRFilter)`) | inspects one comparison | inspects one conjunct at a comparison root; the contract in the splitting section below |

The rule behind the table: a consumer that asks a question of a filter asks
it of one conjunct, and the split into conjuncts happens once, in the
planner, before any of them looks.

### Context rules as checks

Type checking has one function per node kind (`resolve_expr_type`,
`typecheck.rs:1315-1793`) and one small check per clause, in the shape of a
SQL engine's where-clause binder, which sets a target type and overrides a
few node kinds:

| Clause | Required type | Refused node kinds |
|---|---|---|
| match filter | `Bool` or `Bool?` | `Aggregate`, `AliasRef`, a bare `meta_field`; a `Search`, `Fuzzy` or `MatchText` call anywhere but a top-level conjunct (`T38`) |
| mutation `where` | `Bool` or `Bool?` | `Variable` other than a declared parameter, `Aggregate`, `Nearest`, `Search`, `Fuzzy`, `MatchText`, `Bm25`, `Rrf`, `AliasRef` |
| mutation assignment | the property's type | every node but `Literal`, a `Variable` naming a declared parameter, `Now`, and comparisons and Boolean operators over those; an aggregate, search or ranking call is `T44`; a property, `@id`, `@src` or `@dst` leaf is `T45` |
| `prop_match` | the property's type on a scalar property; the scalar element type on a list property, with today's membership checks refusing a list value (`typecheck.rs:936-987`) | as for a mutation assignment |
| `return` item | any scalar or aggregate | `Search`, `Fuzzy`, `MatchText`, `Rrf` as today (T35, T37); a comparison or Boolean expression without an alias (`T43`); a Boolean item in an aggregate `return` (`T9`) |
| `order` key | any value the sort accepts today: an orderable scalar (`ScalarType::is_orderable`, `types.rs:119`) sorts by value; a list, a Vector or a node binding is accepted and sorts as the engine sorts that type today (a node binding by its `@id`), kept explicitly rather than refused | `Search`, `Fuzzy`, `MatchText`; a key other than a property access, a system field or the leading search key that matches no return item (`T42`) |

`typecheck_mutation_predicate`, `typecheck_edge_mutation_predicate` and
`check_match_value_type` (`typecheck.rs:616-727`) are replaced by
`resolve_expr_type` under a mutation scope whose only bindings are the target
type's properties and the declared parameters. The `T7`, `T11` and `T14`
messages keep their codes and texts.

### A filter node holds a conjunct list; one pass places them

`LogicalNode::Filter { input, predicate: Predicate }` (`logical.rs:325`)
becomes `Filter { input, conjuncts: Vec<IRExpr> }`: the top-level `and` chain
of the filter as written, split once into its conjuncts when the node is
built, each conjunct proven `Bool` by the type checker. This is the shape
of the SQL engines and dataframe libraries surveyed for this RFC: a filter
node holding a list of expressions, split into conjuncts in its
constructor, or an iterator over the top-level `and` chain.
An `or` is one conjunct; a `not` is one conjunct; each conjunct keeps the
tree the user wrote, so explain prints each from its tree.

Placement moves out of plan construction into the one pass that already
exists for it. Today `resolve_pipeline` decides three placements while it
builds the plan: a `NodeScan`'s filters go straight into `ScanSpec.filter`
(`optimizer.rs:299`), an `Expand`'s destination filters are partitioned
between the destination scan and a residual `Filter` on the spot
(`:360-389`), and only a standalone `IROp::Filter` waits for the pass
`place_query_filters` (`:1082-1136`), which moves a filter into a scan when
`placement_target` (`:1141-1177`) names one and skips any filter that is not a
single comparison (`predicate.single_gq()`, `:1120`). Under this RFC
`resolve_pipeline` builds every filter as a `Filter` node above its scan,
with its conjunct list, and decides nothing. `binding_of`
(`optimizer.rs:431-436`) looks through `Filter` nodes to the `TableScan`
beneath them, so the `CrossJoin`'s side prefixes (`join_schema`,
`:801-818`) are unchanged by the node that now sits above a root scan.
`place_query_filters` then
coalesces adjacent `Filter` nodes as its first step (below), and
visits every `Filter` node in the scope, tries every conjunct on its own
through `placement_target`, moves the ones that have a target into that
scan's `ScanSpec.filter` with the existing `and_filter` (`:1072-1077`,
`Predicate::And` at `logical.rs:103`, reused), leaves the others in the node,
and splices the node out when nothing remains. The pass is recorded as
`PASS_PREDICATE_PUSHDOWN` in the explain `passes` list as today
(`:904-911`), so an `expect plan` case can assert that it fired (`pass
predicate_pushdown`) and which columns each scan filter and each residual
filter reads (`filter reads [..]`).

Every filter `resolve_pipeline` places at build today is an inline binding
match with a literal, parameter or `now()` operand, which `filter_pushable`
always accepts, so the pass reproduces today's placement exactly; the
`.gqt` `expect plan` cases are the check.

A match block's filter clauses are the implicit conjunction of its
conditions, and an explicit `and` inside one clause is the same
conjunction. Two filter clauses build two `Filter` nodes and one clause
joining them with `and` builds one, so `place_query_filters` first
coalesces adjacent `Filter` nodes within one scope into one node, without
crossing an `AntiJoin` or scope boundary, its conjunct list in the order
the conditions are written. The two spellings then hold the same conjunct
list, the pass places them the same way, and they yield one plan and one
`logical_hash`, including when no conjunct can be pushed and every one
stays residual. Conditions stay in the match
block beside the pattern they constrain, the form Cypher and ISO GQL also
allow inside a pattern element; a separate `where` clause was weighed and
set aside (Alternatives).

`placement_target` keeps its two rules and applies them per conjunct: a
search conjunct goes to the scan of its field's binding, and a scalar
conjunct goes to the one scan whose binding it reads when
`PlanSource::filter_pushable` accepts it. So `$p.age > 30 and $p.city =
"Berlin"` places both conjuncts in `$p`'s scan; `$p.age > 30 and $p.age >
$q.age` places the first and keeps the second; `$p.x = 1 or $q.y = 2` reads
two bindings and stays a residual filter over the joined rows, which no
scan can shrink early. The split and the pass are the only two places that
know a conjunction exists; every later consumer sees one conjunct.

Cost estimation sees the same conjunct list. `scan_row_estimate`
(`crates/omnigraph-planner/src/cost.rs:280-306`) drops a scan's estimate to
one row when a pushed conjunct equates the binding's whole key or a unique
property with a literal or a parameter, and otherwise leaves the table's
row count. Under this RFC the rule inspects a conjunct only when its root is
a comparison; a conjunct whose root is `or`, `not` or a null test yields no
estimate and the row count stands. So `x = k and y > 3` on a keyed `x` keeps
the one-row estimate, `x = k or y = 3` keeps the full count, and no
statistic is invented for either. This RFC states that contract and adds no
cost code; every estimate and every cost-driven decision lives in the cost
component, whose boundary a separate proposal draws.

No expression in the surface this RFC admits can fail at evaluation time: the
comparisons, `and`, `or`, `not`, `starts_with` and `contains` are total over
their typed operands. So conjunct order is unobservable and the planner may
reorder and push freely. The day arithmetic or a fallible function enters the
model, each `IRExpr` node needs a `can_fail` property and the pushdown rule
must keep a fallible conjunct behind the conjuncts that guard it, as the SQL
engines and dataframe libraries surveyed for this RFC do with a
per-expression can-fail property that their pushdown consults. That
rule is written here so the later RFC inherits it.

### One evaluator per arm

The v2 engine has two lowerings of `IRExpr` and both become total over the
enum:

- `engine/expr.rs::evaluate_filter` and `evaluate_expr` (`:101-163`) evaluate a Boolean `IRExpr`
  over a `RecordBatch` for the residual `LogicalNode::Filter`. `Binary`
  with `And`/`Or` lowers to Arrow's `and_kleene`/`or_kleene`, `Not` to Arrow's
  `not`, `IsNull` to Arrow's `is_null`/`is_not_null`, comparisons to
  `arrow_ord::cmp` as today.
- `engine/scan.rs::ir_filter_to_expr` (`:744`) becomes `ir_expr_to_df_expr`
  and lowers to a DataFusion `Expr` for scan pushdown. `And`/`Or` lower to
  `Expr::BinaryExpr` with `Operator::And`/`Operator::Or`, which
  `build_lance_filter_expr` already builds for the conjunct list
  (`scan.rs:730-732`); `Not` lowers to `Expr::Not`, `IsNull` to
  `Expr::IsNull` or `Expr::IsNotNull`.

`FilterExec` (`engine/operators/filter.rs:25`) holds `Vec<IRExpr>` conjuncts
in place of its `Vec<IRFilter>`; it still calls `evaluate_filter` once per
conjunct (`:118`). The v1 copies in
`exec/projection.rs` and `exec/query.rs` keep their behaviour frozen; their
bytes change only by the phase 1 type rename (an `IRFilter` becomes a
comparison-rooted `IRExpr`, and `exec/mod.rs:24` imports the new names). A
search predicate reaches them in the same comparison shape, its call as the
left operand and `true` as the right, so v1's search dispatch and full-text
query construction (`exec/query.rs:1663-1685,4155-4156`) read it as today,
and the existing standalone search cases under v1 are its control.
Their hashes in `tests/v1_frozen.rs:24-41` are re-pinned in the same PR
under review, the procedure that file's header prescribes
(`v1_frozen.rs:5-6`). The gate in the engine-setting section refuses what
they cannot evaluate.

### Mutations select rows through the same lowering

`predicate_to_sql` (`exec/mutation.rs:390-439`) is removed. The mutation
`where` `IRExpr` lowers through `ir_expr_to_df_expr` to a DataFusion `Expr`,
and that typed expression is what Lance receives. Lance 11.0.0, the version
in omnigraph's `Cargo.lock`, accepts one at both places the mutation path
touches it: `DeleteBuilder::from_expr(dataset, expr)` stores it as
`ExprFilter::Datafusion` (`lance-11.0.0/src/dataset/write/delete.rs:140-143`),
and the scanner that `execute_update` uses to select the committed rows it
will rewrite (`exec/mutation.rs:1385-1412`) has `filter_expr(Expr)`
(`lance-11.0.0/src/dataset/scanner.rs:1585`). `stage_delete`
(`table_store.rs:2745`) switches from `DeleteBuilder::new(ds, &str)` to
`from_expr`. The text Lance writes into its own transaction record is
Lance's `Display` rendering of the expression (`delete.rs:251`), not ours,
and Lance's conflict resolution never reads it back:
`lance-11.0.0/src/io/commit/conflict_resolver.rs` names the `predicate`
field only in its test module, which starts at `:2308`.

The `String` predicate has more consumers than `stage_delete` today, and
each gains an `Expr` form in phase 1: `TableStore::scan` and `scan_with`
(`table_store.rs:2273,2296`), which take the filter through
`ScanTuning::filter_expr` (`:252`) for the delete id scans
(`exec/mutation.rs:1518,1614,1679`); `scan_with_pending` and
`scan_pending_batches` (`table_store.rs:4677,5456`), whose `MemTable` side
applies `DataFrame::filter(expr)` where it builds an SQL `WHERE` today
(`:5477-5480`); `first_row_id_for_filter` (`:5062`); `DeferredStagePlan::Delete`
(`exec/staging.rs:58`); and the `TableStorage::stage_delete` trait method
(`storage_layer.rs:771`). The node-delete cascade builds its endpoint
membership as `id_in_list_expr` (`engine/scan.rs:27`), replacing the
`IN (…)` strings at `exec/mutation.rs:1551-1576`. With these, no SQL text is
produced anywhere in omnigraph's mutation path.

The staging ledger follows. Today `record_delete` keeps each statement's
predicate as a `String` (`exec/staging.rs:332`), later statements are joined
with `OR` and earlier ones excluded with `IS NOT TRUE` by string algebra
(`staging.rs:589-619`, `dedup_delete_filter` at `exec/mutation.rs:592`). The
ledger holds `Expr` values instead, and `dedup_delete_filter` composes them
with `Expr::and`, `Expr::or` and `Expr::is_not_true`; the set semantics of
separate statements, union, are unchanged, and the three-valued exclusion
is the same DataFusion `IS NOT TRUE` node, built rather than printed. Parameters arrive as typed `ScalarValue` literals,
so string escaping and date formatting leave the write path with the SQL
text.

This is the shape of the SQL engines surveyed for this RFC: the delete
operator consumes a filter node bound by the shared where-clause binder and
never binds or prints a predicate itself. When a later RFC
moves write selection onto the v2 read path, the `where` expression becomes the
predicate of that read plan; nothing in this RFC blocks that move or
depends on it.

### Order key binding

`typecheck_read_query` (`typecheck.rs:195-290`) resolves each
`Ordering.expr` that is not a property access, a system field or the
leading search key in two steps: a return alias, or a `return` item equal
to the key as an `Expr` tree. Neither match
is `T42`. The binder rewrites a matched key to
`IRExpr::AliasRef(executed_column_name(item))` in `resolve_query`
(`optimizer.rs:155-260`), after type checking, so T17, T18 and T21 see the
key as written; the engine's existing `AliasRef` arm in `sort`
(`engine/lower.rs:747`) resolves it, as today's `alias:<name>` key does
(`optimizer.rs:548,558`). The planner's `unsupported ordering expression`
refusal (`optimizer.rs:222-228`) disappears, because the binder resolves
every non-property key before it; the engine's internal guards
(`engine/lower.rs:752-757,832-835`) stay, unreachable, for the shapes the
planner never emits.

### What does not change

Graph operators (`Expand`, `AntiJoin`, `Nearest`, `TextSearch`, `RankFuse`) are
plan nodes, not expressions; the `not { }` pattern negation stays an
`AntiJoin`. Search predicates never lower to a DataFusion `Expr`
(`exec/query.rs:4612-4614`) and keep their scanner route. `Predicate::IdAfter`
and `Predicate::VersionWindow` (`logical.rs:96-102`) are engine-internal scan
predicates with no GQ spelling and stay as they are. Row identity, `@id`
selection, branch scope, affected receipts, the staging and publication
protocol and the Lance storage format are untouched.

Inherited from Lance and DataFusion: three-valued `and`/`or`/`not` and
null-comparison semantics (DataFusion), the two expression entry points
`DeleteBuilder::from_expr` and `Scanner::filter_expr` (Lance 11.0.0,
surfaces to be reviewed under `docs/dev/lance.md` before the PR opens), and the predicate text in
Lance's transaction record. Added by omnigraph: the grammar, the context
rules, the conjunct list, the placement pass and the gate.

## Invariants

Affected invariants from [`docs/dev/invariants.md`](../dev/invariants.md):

- **9, query semantics are typed structures.** Strengthened. A mutation
  `where` today reaches storage as a SQL string built by `predicate_to_sql`;
  after this RFC it is an `IRExpr` until the Lance API boundary, the same
  structure the read scan lowers. The deny-list item "ad-hoc SQL or
  `IN (...)` string generation where structured expressions or SIP apply"
  (`invariants.md:116`) has several instances in the mutation path today
  (`predicate_to_sql`, the cascade's `IN (...)` filters at
  `exec/mutation.rs:1551-1576`, the string algebra of `dedup_delete_filter`
  and the staging `OR` join) and none after.
- **8, integrity failures are loud.** Unchanged. Every new refusal (a
  non-Boolean operand of `and`, an aggregate in a `where`, a compound
  predicate under `engine = v1`) is a typed compile-time or gate error, never
  a silently narrowed predicate. A null constant result assigned to a
  non-nullable property is a typed write error that names the property,
  checked before the batch is built, with Arrow's non-nullable-field check
  as the backstop; it is never written as a default.
- **3, every operation uses one coherent accepted view**, and **4, a
  mutation publishes once.** Unchanged. The RFC changes how rows are
  selected, not when a mutation reads or publishes. `record_delete`
  (`exec/staging.rs:332`) stores an `Expr` in place of a `String`; the
  staged-delete protocol around it and the one `stage_delete` per table are
  unchanged.
- **1, respect the substrate.** Unchanged. Lance's delete builder and
  scanner are called through their expression entry points, which they
  already expose; no Lance behavior is bypassed or reimplemented.

Deny-list items the design touches: the SQL string item above, removed; the
hidden-statistics item ("cost-blind plan choice or planner decisions based
on hidden statistics", `invariants.md:119`), untouched: a compound conjunct
yields no estimate rather than an invented one; no new job, cache, lock,
side channel or precondition.

## Compatibility and reversibility

**Posture.** GQ is pre-stable with few users, and this RFC makes no
compatibility promise for query text, plan hashes or explain output. Where
a better language and an unchanged behavior conflict, the language wins and
the `.gqt` case that pinned the old behavior is edited, as
`order_clause_aggregate_refused.gqt` is in phase 3. Every query that parses
today parses after this RFC, except one whose bare mutation property or
return alias is one of the five reserved words; that break is named below. What the design does
preserve, it preserves because of that and because the removed types are
wrappers around the same fields, not because it set out to. The
`.gqt` corpus is still the control for phase 1, whose whole point is a
representation change with no result change; that is a test of the
refactor, not a contract with users.

One break the design does make, named so nobody hunts for it: `and`, `or`,
`not`, `is` and `null` become reserved words in operand position, so a
property literally named one of them can no longer be written bare as an
operand of a mutation `where`; `and: 1` in an assignment or a binding
match, and `$p.and` after a dot in a read, stay legal. A read alias named
one of them breaks the same way: `return { $p.age as and } order { and }`
parses today, and after phase 2 the declaration `as and` is a parse error,
so such a read renames its alias. The `.pg` schema
grammar does not reserve them
(`crates/omnigraph-compiler/src/schema/schema.pest`), so such a schema can
exist. `edge_ident` excludes all five words the same way it already
excluded `not`, so an edge type named `and`, `or`, `is` or `null` can no
longer be traversed bare (`$a and $b` over Boolean parameters is a filter,
not a traversal); the survey found no such edge type checked in. A property
named `true` or `false` breaks the same way in a mutation `where` only: a
bare `false` there is the literal, and the type checker refuses the
ambiguous predicate with `T46` (type rules) instead of selecting rows by a
constant. The parser names the reserved-word collision through
`reserved_property` (see Grammar). A mutation `where` has
no binding-variable spelling, so a mutation cannot select on such a
property at all. A stored query that names such a property bare in a
mutation `where`, or declares such a return alias, fails to parse at the
first boot after phase 2, and the
server quarantines that whole graph
(`crates/omnigraph-server/src/settings.rs:152-170`). A schema rename alone
does not prevent it, because the registry parses each stored source before
any schema binding (`crates/omnigraph-server/src/queries.rs:108-129`). So
before upgrading the operator does both: renames the property in the schema
with `@rename_from`, which keeps its data (`docs/user/schema/index.md:131`),
and rewrites every stored query source that names the property bare or
declares the alias. A survey of the `.gqt` corpus
and the checked-in schemas for such a property or alias is an evidence
item below.

**GQ language version.** The compatibility surfaces RFC
(`docs/rfcs/2026-09-14-compatibility-surfaces.md`, draft) makes the GQ grammar
a versioned surface: `GQ_LANGUAGE_VERSION: (u16, u16)` in
`omnigraph-compiler`, starting at `(1, 0)`, one hash per `query.pest` rule,
major when the grammar accepts less than before or a produced shape changes,
minor when it only accepts more. This RFC is the first language change to
declare its bumps under that rule:

| Phase | Grammar change | Bump | Why |
|---|---|---|---|
| 1 | none; `query.pest` is untouched; the deprecation marker `compat/gq_language.deprecated.txt` names `rule:match_value` and `rule:text_search_clause` | none | the rule hashes do not move, so the version does not either |
| 2 | rewritten: `expr`, `clause`, `filter`, `mutation_predicate`, `mutation_assignment`, `prop_match`, `projection`; added: `or_expr`, `and_expr`, `not_expr`, `comparison`, `null_test`, `operand`, `kw_and`, `kw_or`, `kw_not`, `kw_is`, `kw_null`, `reserved`, `reserved_property`, `reserved_alias`, `expr_ident`; kept unreferenced and deprecated, text unchanged: `match_value`, `text_search_clause` (deleted in the release after this one); `ident`, `comp_op`, `filter_op`, `ordering` untouched | `(1, 0)` to `(2, 0)` | reserving five words narrows what an operand and a return alias accept |
| 3 | none; `query.pest` is untouched | none | the refusal moves from the engine to the type checker; no rule hash moves |

Phase 2 stops referencing two rules, and the surfaces RFC requires a
removal to be deprecated in one release first, its floor refusing a removal
without the prior marker. The three phases ship in one PR, so that PR
keeps `match_value` and `text_search_clause` in
`query.pest` unreferenced, their text unchanged so their rule hashes stay,
and ships `gq_language.deprecated.txt` naming `rule:match_value` and
`rule:text_search_clause`; the following release deletes the two rules.

If the surfaces RFC's phase 3, which adds the constant and the hash file,
has not landed when this RFC's phase 2 does, phase 2 adds
`GQ_LANGUAGE_VERSION` itself as `(2, 0)` with a one-line doc pointing at the
surfaces RFC, and that RFC's phase 3 attaches its hash file to the existing
constant. The version is what a release note, a client and a `.gqt` case can
name: "`or` needs GQ 2.0". A query file does not declare a language version;
the compiler has one grammar at a time, as the surfaces RFC specifies, and
the version records which one.

**Explain and plan hashes.** `EXPLAIN_VERSION`
(`crates/omnigraph-planner/src/explain.rs:15`) moves from 1 to 2 with
phase 1. Its contract is to increment when a field's meaning changes
(`explain.rs:13`), and the logical `Filter` node's `predicate` key becomes
`conjuncts`, so a reader tells the two formats apart by the version. The
`Predicate` serde shape `Gq { reads, text }` (`logical.rs:94,110-115`) is
unchanged, because expressions were already serialized as GQ text. A logical `Filter`
node serializes as `{"node":"Filter","conjuncts":[{"reads":[…],"text":"<gq>"}, …]}`,
one entry per conjunct in the `Predicate::Gq` field shape, where today it
carries one `predicate` key (`logical.rs:688-691`). The `.gqt` harness's
plan reader (`crates/omnigraph-gqt/src/plan.rs:588-593`) moves to the new
key and the new version in the same phase 1 PR, and its `filter reads [..]` line reads the union of a
node's conjunct reads. `LOGICAL_PLAN_VERSION` moves with phase 1 because
the `Filter` node's shape changes, and every `logical_hash` moves once with
it. No `.gqt` case changes, since `expect plan` asserts reads and passes,
not filter text.

**Stored queries and the catalog.** Stored queries are GQ source, recompiled
(`crates/omnigraph-server/src/queries.rs:30-34`); `QueryCatalogEntry`
carries names, descriptions and parameter descriptors, no expression
(`omnigraph-api-types/src/lib.rs:1121`). Neither changes shape, and two
consequences reach stored queries all the same. A stored query cannot carry
`set engine` (`STORED_QUERY_CARRIES_NO_SETTINGS`,
`crates/omnigraph-compiler/src/settings.rs:319`, enforced at
`crates/omnigraph-server/src/queries.rs:115`), so until v2 is the process
default a stored read with a compound predicate runs only on a server
started with `OMNIGRAPH_ENGINE=v2`. A stored mutation that names a reserved
word bare as a property, or a stored read that declares one as a return
alias, quarantines its graph at boot (the reserved-word paragraph above).

**Engine setting.** Under `engine = v1` a read whose filter shape v1 does
not evaluate is refused; the setting itself removes nothing else.

**Session settings.** The session-settings RFC
(`docs/rfcs/2026-09-16-session-settings.md:73-85`) states that every
`request` setting but `ann_nprobes` is answer-preserving, and `engine` is a `request` setting
(`crates/omnigraph-compiler/src/settings.rs:63`). This RFC narrows that
sentence for `engine`: a statement both engines accept is
answer-preserving; a statement only engine v2 accepts is a typed gate error
under v1. The companion change to `2026-09-16-session-settings.md` rewrites
that sentence and records the narrowing in its decision log; it is a
phase-2 deliverable.

**Storage.** No storage format, manifest field or Lance version changes.

**Reverting.** Each phase reverts on its own boundary. Phase 1 (the
representation) reverts as one coordinated change: the removed types return
together with the planner, engine, staging-ledger, storage-trait, harness
and explain consumers phase 1 moved to the new representation, and the
`.gqt` corpus is its control. Phase 2 reverts by removing its syntax, which
turns queries that use it into parse errors; a stored query whose source
uses that syntax is replaced before the downgrade, because the registry
parses stored source at boot. Phase 3 changed no grammar and reverts by
undoing its order-key binding and type-check changes. The storage format is
unchanged by every phase; stored GQ source is the one persisted input a
downgrade must stay compatible with.

## Alternatives

| Alternative | What it buys | Why it loses |
|---|---|---|
| Do nothing | no change | issues [660](https://github.com/ModernRelay/omnigraph/issues/660), [651](https://github.com/ModernRelay/omnigraph/issues/651) and [566](https://github.com/ModernRelay/omnigraph/issues/566) part 3 stay open; a fourth predicate shape is added the next time one clause needs an operator |
| Mutation-only conjunction: `mutation_predicate` becomes a list of comparisons joined by `and` (the first suggestion under issue [660](https://github.com/ModernRelay/omnigraph/issues/660)) | fixes 660 with a small grammar change | adds a fifth condition shape; `or` and grouping still impossible; 651 and 566 untouched; the SQL string path grows |
| `and` only, everywhere: `IRFilter` becomes `Vec<IRFilter>` | fixes 660; no precedence to define | cannot express 651; `not` and grouping need the tree anyway, so the vector is thrown away at the next step. This is the minus-one-mechanism design: it fails on `$p.name = "Alice" or $p.name = "Bob"`, which is inside the documented request |
| Use DataFusion's `Expr` as the IR and drop `IRExpr` | one enum fewer; DataFusion's optimizer for free | the graph typing lives in `IRExpr`: bindings, `@id` roles, aggregate placement, search and ranking calls that never lower to DataFusion (`exec/query.rs:4612-4614`). The language would be defined by a dependency's enum and its version bumps |
| Extend `predicate_to_sql` to print `and`/`or` | smallest mutation diff | keeps the deny-listed string generation and a mutation-only evaluator; the read and write paths still disagree on null and type rules by construction |
| Render the DataFusion `Expr` to SQL at the Lance boundary with `datafusion-sql`'s `Unparser` | one shared lowering, small change at the call site | unnecessary: Lance 11.0.0 accepts the `Expr` itself through `DeleteBuilder::from_expr` and `Scanner::filter_expr`; a renderer would reintroduce quoting and dialect rules (the camelCase case `exec/mutation.rs:1763` guards today) for nothing |
| Hidden `order` items appended for any order key and pruned, as some SQL engines do | `order { max($d.amount) }` works without a return item | a hidden aggregate on a non-aggregated `return` changes the grouping and the rows; the explicit rule costs one typed error with an obvious fix |
| A separate `where { expr }` clause after `match` | a misplaced condition is a parse error; removes the binding-order trap (`T6` on a comparison written before the traversal that binds its variable) | drops the condition-beside-pattern form Cypher and ISO GQL allow and graph users expect; rewrites every stored query, fixture and case; the two spellings of conjunction are declared one plan instead |

Precedent audit. `Predicate::And` in the planner (`logical.rs:103`) is the
in-repo pattern for a structured conjunction and this RFC reuses it.
`IRExpr::Param(NOW_PARAM_NAME)` for `now()` (`ir/lower.rs:821`) is the
precedent for unifying a surface construct into the shared IR instead of a
sibling type. RFC 0040's `@id`/`@src`/`@dst` namespace (`query.pest:141-143`)
is the leaf set the mutation context reuses. No divergence from these.

## Evidence and tests

**Controls.** The existing `.gqt` corpus under `crates/omnigraph-gqt/cases/`
is the behavior control for phase 1: 25 files with a property comparison
in a filter (lines matching `$<variable>.<property> <comparison operator> `),
67 with `order`, 33 with aggregates, 35 with a mutation `where`, 30 with
`expect plan` (counts by grep at `545176bd`, files overlap across rows). Phase 1
leaves every row, count, shape, error and plan expectation green with no
case edits; the harness's plan reader moves to the `Filter` node's new key
and to `EXPLAIN_VERSION` 2 in the same change.

**Both-engine control.** The `engine-v2` mode of the GQ logic-test matrix
(`.github/workflows/gq-logic-tests.yml:133,203`) runs the corpus under
engine v2 on every PR; with phase 1 it is the differential proof that the
representation change altered no result.

**New cases, one per contract line above, all `.gqt` except the
stored-query gate, a server integration test; the `Display` round trip is
a Rust test (below).** Every case that uses
the new syntax opens its query steps with `set engine = v2;`; the gate cases
are the ones that do not.

- precedence: `a or b and c` selects the rows of `a or (b and c)`; the same
  text with parentheses moved selects a different set on a four-row seed.
- `not` over a value versus `not { }` over a pattern in one match block.
- null in `and`/`or`: a nullable property, the four truth-table rows,
  `not ($p.x = 1)` excluding the null row, `$p.x is null` selecting exactly
  it, and `is not null` on a non-nullable property selecting every row;
  `not` over `contains` and over `starts_with` on a null row, identical on
  both arms.
- a comparison in `return`: `$p.age > 30 as adult` gives a `Bool` column,
  null where `age` is null, with `expect shape` asserting `adult: Bool?`.
- placement per conjunct: `$p.age > 30 and $p.age > $q.age` asserts `pass
  predicate_pushdown`, `scan Person as $p: filter reads [p.age]` and
  `filter reads [p.age, q.age]`; the same query written as two match
  clauses asserts the same three lines and the same rows.
- two residual conjuncts in both spellings: `$p.age > $q.age` and
  `$p.score > $q.score` as two match clauses, and joined with `and` in one
  clause, each assert the one residual filter `filter reads [p.age, p.score,
  q.age, q.score]` and the same rows; the equal `logical_hash` follows by
  construction from the one node.
- cross-binding `or` stays a residual filter: `$p.x = 1 or $q.y = 2`
  asserts `filter reads [p.x, q.y]`, `scan … as $p: no filter`, `scan … as
  $q: no filter` and `not pass predicate_pushdown`; a single-binding `or`
  asserts `scan … as $p: filter reads [..]`.
- a traversal destination filter `$f.age > 30 and $f.name starts_with "a"`
  asserts `scan Person as $f: filter reads [f.age, f.name]`, placed by the
  pass and not at plan build, with today's rows for the same query.
- issue [660](https://github.com/ModernRelay/omnigraph/issues/660): a new
  case `issue_660_exact_edge_deletion.gqt` under `crates/omnigraph-gqt/cases/`,
  on the four-edge graph of the example above: `delete Knows where @src =
  "a" and @dst = "b"` gives `affected: nodes=0 edges=2`, leaves `ac` and
  `db`, survives branch create, merge and restart; a compound `update` on a
  node type sets one property on exactly the intersected rows (an edge
  `update` is `T16` today, a limit outside this RFC).
- issue [651](https://github.com/ModernRelay/omnigraph/issues/651):
  same-property `or`, cross-property `or`, nested grouping, parameters in
  both arms.
- issue [566](https://github.com/ModernRelay/omnigraph/issues/566) part 3:
  `order { count($d) desc }` with `count($d) as deals` in `return` sorts by
  it; `order { count($d) }` with the aggregate written unaliased in
  `return` sorts by tree equality; `order { max($d.amount) }` with no
  matching return item expects `T42`. `order_clause_aggregate_refused.gqt`
  flips its first step to a positive case and keeps its second as the `T42`
  control. A property key absent from `return` keeps sorting through the
  hidden column: the five corpus files that do this today
  (`parallel_edge_ties_keep_page_boundaries.gqt`,
  `limit_page_boundary_under_tied_keys_of_two_bindings.gqt`,
  `v2/planner/projection_demand.gqt`,
  `v2/planner/issue_703_expand_destination_projection.gqt`,
  `meta_fields_resolve_by_role.gqt`) stay green unchanged.
- `set engine = v1;` plus a compound read predicate expects the gate error
  naming the construct and showing both fixes; a comparison in `return`
  under v1 expects the same text with its construct named; a leading
  `nearest` order key under v1 runs as today, exempt from the gate; the
  same text under `v2` returns rows; the same text with a type fault inside it
  (`$p.age and $p.name`) expects the plain `T41` under both settings, with
  no mention of engines.
- filter shapes outside v1's evaluator, under both settings: a bare Boolean
  property filter `$p.enabled` and the residual comparison of comparisons
  `($p.age > 30) = ($q.age > 30)` each expect the gate error under `v1` and
  return rows under `v2`.
- the stored-query gate, one server integration test
  (`crates/omnigraph-server/tests/stored_queries.rs`) with two assertions.
  First, a stored read with a compound predicate, on a server under the v1
  default, expects the gate error, and the same stored query on a server
  started with `OMNIGRAPH_ENGINE=v2` returns rows. Second, a registry whose
  stored sources were rewritten away from a reserved-word property and
  alias reloads without quarantine.
- type errors: `and` over non-Boolean operands, an aggregate in a `where`, a
  search call in a `where`, an unaliased comparison in `return` (`T43`), a
  `count` in a mutation `where` (`T44`), a property leaf in an assignment
  value (`T45`).
- `delete Person where name starts_with "a" and tags contains "x"` gives
  the intersected `affected` count.
- reserved-word survey: a schema with a property named `and` and one named
  `is`; the reads `$p.and = 1` and `$p.is = 1` pass, the mutation `where and
  = 1` gets the named parse error, and the read `return { $p.age as and }
  order { and }` gets the named alias parse error at `as and`.
- constants per invocation: `update Person set { adult: $age > 30 } where
  slug = $slug` and the inline match `$p: Person { adult: $age > 30 }`, each
  invoked twice with `$age` 20 and 40, give opposite results for the two
  invocations. With `$flag: Bool?` bound to null, `update Person set {
  adult: true or $flag } where slug = $slug` writes `true` and the inline
  match `$p: Person { adult: true or $flag }` selects the rows whose `adult`
  is true; `false and $flag` in both writes `false` and selects the rows
  whose `adult` is false.
- inline list matches: `tags: "rust"` and `tags: $tag` on a `[String]`
  property select by membership, and a list value there keeps its `T3` or
  `T7` refusal; the existing list-match tests
  (`query/typecheck_tests.rs:266-312`) stay green unchanged.
- null into a non-nullable property, one case with one `expect error`
  step: `update Person set { adult: $age > 30 } where slug = $slug` with
  `$age: I64?` bound to `null` expects the typed error that names the
  property, `adult`, raised before the batch is built.
- camelCase property in a compound mutation predicate, `delete Deal where
  dealStage = "open" and amountUsd > 10`, which retires the unit test
  `predicate_to_sql_backtick_quotes_column_case_preserved`
  (`exec/mutation.rs:1763`) into a row-visible case.
- three deletes in one query on overlapping predicates, asserting the union
  count and the affected receipt, the control for the staging ledger moving
  from strings to expressions.

The row-estimate contract of the conjunct-list section is asserted by the
cost component's own tests, not by `expect plan`, whose vocabulary has no
row-estimate line.

**Rust tests:** two, each with a reason the `.gqt` format cannot express.
First, the stored-query gate server integration test above, one test with
two assertions, because the `.gqt` harness has no server target
(`crates/omnigraph-gqt/src/runner_config.rs:73-92`), the
process-environment reason. Second, a `Display` parse, print and parse
round trip over a fixture of expressions, among them `($p.age > 30) =
true`, `true = ($p.age > 30)` and `($p.age > 30) is null`, each printing
text that parses back to the same tree, because `.gqt` compares no
rendered text (`crates/omnigraph-gqt/README.md:138-199`). Every other
mechanism above is visible in rows, counts or errors, so the `.gqt` corpus
owns it. The existing
staged-delete rebase test (`crates/omnigraph/src/table_store/staged_tests.rs:2739`)
runs with a `from_expr` delete once phase 1 lands, and is the control that
a staged delete rebases as before.

**Generated evidence.** Grammar fuzzing of the expression ladder
(precedence, De Morgan under three-valued logic, the null partition `a` /
`not a` / `a is null`, mutation-read agreement) is generated evidence owned
by a separate proposal, and starts once phase 2 exists to generate against.

**Surveys.** A column-store SQL engine's binder family, plan ownership and
order binding; a dataframe library's expression trees, plan-node ownership,
type check and predicate pushdown; the omnigraph expression inventory with
its duplication map. The anchors in the body name every surveyed omnigraph
file.

## Rollout

1. **Representation.** `Expr` and `IRExpr` gain `Binary`, `Not` and
   `IsNull`; the IR types derive `PartialEq`, `Eq` and `Hash`; `Filter`,
   `MatchValue`, `MutationPredicate`, `IRFilter` and `IRMutationPredicate`
   are removed; `GqFilter` wraps `IRExpr` without the `Debug`-string
   comparison; `LogicalNode::Filter` holds its conjunct list and
   `resolve_pipeline` builds every filter as a node, leaving placement to
   `place_query_filters` per conjunct; `PhysicalNode::Filter` holds
   `Vec<IRExpr>`; `binding_of` looks through `Filter` nodes; the v2
   evaluator and scan lowering handle the new variants; `predicate_to_sql`
   is replaced by the shared lowering, `stage_delete` takes the `Expr`
   through `DeleteBuilder::from_expr`, the update scan through
   `filter_expr`, the other readers of the delete string take `Expr`, and
   the staging ledger holds `Expr` values; the frozen v1 files change by
   the type rename only and their hashes in `tests/v1_frozen.rs` are
   re-pinned in the same PR; `EXPLAIN_VERSION` moves to 2 and the `.gqt`
   harness's plan reader moves to it and to the `Filter` node's `conjuncts`
   key; the deprecation marker names
   `rule:match_value` and `rule:text_search_clause`. No grammar change.
   The corpus under both matrix modes is the control.
2. **Grammar.** The `expr` ladder with `null_test` and the word-bounded
   keywords, `filter = { expr }`, `mutation_predicate = { expr }`,
   `mutation_assignment` and `prop_match` over `expr`, `text_search_clause`
   removed, Boolean expressions in `return`, the v1 gate, the new type
   errors; `GQ_LANGUAGE_VERSION` to `(2, 0)`; `match_value` and
   `text_search_clause` stay as unreferenced rules. The companion
   change to the session-settings RFC lands with it. The release notes name
   the reserved words and tell an operator, before upgrading, to rename a
   property named `and`, `or`, `not`, `is` or `null` in the schema, to
   rewrite every stored query that names it bare, and to rename every return
   alias spelled as one of them. `docs/user/queries/index.md`
   gains the precedence ladder, the null rules with the four-row truth
   table, `is null`, Boolean columns in `return` and the reserved words.
   Closes issues [660](https://github.com/ModernRelay/omnigraph/issues/660)
   and [651](https://github.com/ModernRelay/omnigraph/issues/651).
3. **Order keys.** Binding every key other than a property access, a
   system field or the leading search key against the return list, `T42`
   for such a key that matches nothing; no `GQ_LANGUAGE_VERSION`
   change. `docs/user/queries/index.md` gains the order-key rule. Closes
   issue [566](https://github.com/ModernRelay/omnigraph/issues/566) part 3.

The three phases ship in one PR. The phase
boundaries stay as the order of review and as the revert boundaries in
"Reverting", not as release boundaries; the one piece that waits a release
is the deletion of the two unreferenced rules. `implementation` moves to
`complete` when the PR merges. Each phase is independently safe: phase 1
changes no result, phase 2 adds text that was a parse error, phase 3
accepts text that was a runtime refusal and turns the remaining runtime
refusal into a compile-time one.
Out of scope and not started by this RFC: arithmetic operators, scalar
functions, and the move of write selection onto the read plan (a later
RFC).

The PR carries the three phases beside this text, so it adds the registry row `| [2026-09-24](2026-09-24-shared-expression-model.md) | Shared expression model | maintainer | draft | in-progress |`
and sets `discussion` to the PR URL.

## Unresolved questions

None.

## Decision log

- 2026-09-24: drafted from issues
  [660](https://github.com/ModernRelay/omnigraph/issues/660),
  [651](https://github.com/ModernRelay/omnigraph/issues/651) and
  [566](https://github.com/ModernRelay/omnigraph/issues/566) part 3 and a
  source survey of two open-source query engines; rationales
  are in the body.
