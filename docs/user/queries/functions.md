# Functions and operators

A query computes values with operators and registry functions, written
`name(arguments, option: value)`. Each function below is pure: it reads only
its arguments. Every scalar function here except `if` and `coalesce` returns
null when any argument is null. A function may appear wherever an expression
is written: `where`, `let`, `return`, `order` and a write's assignments.

```gq
query deal_spread($region: String) {
  match { $d: Deal }
  where lower($d.region) = lower($region)
  return { $d.stage, count($d) as deals, stddev($d.amount) as spread,
           percentile($d.amount, 0.5) as median }
  order { spread nulls last }
}
```

## Operators

| Operator | Meaning |
|---|---|
| `=`, `!=`, `<`, `<=`, `>`, `>=` | Comparison; a null operand gives null. Comparisons do not chain. |
| `in` | Membership in a list ([strings and lists](index.md#strings-and-lists)). |
| `is null`, `is not null` | The null tests; always `Bool`. |
| `and`, `or`, `not` | Boolean logic over `Bool` operands ([nulls](index.md#boolean-expressions-and-nulls)). |
| `+`, `-`, `*`, `/`, `%`, unary `-` | Arithmetic ([values](traversal.md#values-and-inline-reads)). |

## Conditionals and strings

| Function | Result |
|---|---|
| `if(c, a, b)` | `a` where `c` is true, else `b`, a null `c` included. Only the branch taken is evaluated. |
| `coalesce(a, …)` | The first non-null argument, else null. A later argument is evaluated only on rows the earlier ones leave null. |
| `concat(a, …)` | The Strings joined. Null when any argument is null. |
| `lower(s)`, `upper(s)` | Unicode case mapping: `upper("ß")` is `"SS"`. |
| `trim(s)`, `ltrim(s)`, `rtrim(s)` | `s` without Unicode whitespace (a tab, a no-break space) at both ends, the start, or the end. A zero-width space is not whitespace. |
| `length(s)` | The number of characters (Unicode scalar values, not bytes), an `I64`. Over a list it is `T7`, with `size` as the fix. |
| `left(s, n)`, `right(s, n)` | The first or last `n` characters; all of `s` when `n` is longer. A negative `n` is `E5`. |
| `substring(s, start, len)` | The `len` characters from the 1-based `start`, clipped to `s`: a start at or below zero shortens the window, and a start past the end gives `""`. A negative `len` is `E5`. |

A count, start or length is an `I32`, `I64` or `U32`; another type is `T7`.
`contains` and `starts_with` are described in
[strings and lists](index.md#strings-and-lists).

## Numbers

| Function | Result |
|---|---|
| `abs(x)`, `floor(x)`, `ceil(x)` | The operand's type. The absolute value of the smallest `I32` or `I64` is `E1`. |
| `round(x, digits: n)` | Half away from zero at `n` decimal digits (0 when omitted; a negative `n` rounds to tens, hundreds, …), in the operand's type. A float computes `(x · 10ⁿ).round() / 10ⁿ`, so `round(2.675, digits: 2)` is `2.68`. An integer that leaves its type is `E1`, a float result that is not finite `E3`, and a float's `n` outside the `I32` range `E5`. |
| `sqrt(x)`, `exp(x)`, `ln(x)`, `log10(x)` | An `F64`; an integer argument is widened. The root of a negative number and the logarithm of zero or a negative number are `E5`; a result that is not finite is `E3`. |
| `power(base, exponent)` | An `F64`. A negative base to a fractional exponent is `E5`; zero to a negative exponent, and any other result that is not finite, is `E3`. |
| `size(list)` | The number of elements, null elements included, an `I64`; `0` for `[]`. |

## Conversion

`cast(x, T)` converts a scalar to the scalar type `T`: `String`, `Bool`,
`I32`, `I64`, `U32`, `U64`, `F32`, `F64`, `Date` or `DateTime`. Another
target (`Vector(3)`, `Blob`, a node type) is `T7`, and so is a pair with no
conversion below, such as a `Date` to a number or a `Bool` to a float.

| From → to | Conversion |
|---|---|
| number → number | An integer exactly where it fits; a float truncated toward zero into an integer; an integer to the nearest float. A value that does not fit is `E6`. |
| `Bool` ↔ integer | `true` is 1 and `false` 0; any non-zero integer is `true`. |
| any scalar → `String` | An integer in decimal, a float in its shortest decimal form, a `Bool` as `true` or `false`, a `Date` or `DateTime` as its literal. |
| `String` → any scalar | Only a literal of the target type: an optionally signed integer, a decimal number with an optional exponent, `true` or `false` in any letter case, a `Date` or `DateTime` literal. Any other text is `E6`. |
| `Date` ↔ `DateTime` | A date becomes its midnight; an instant becomes the day it falls on. |

## Aggregates

`count`, `sum`, `avg`, `min`, `max` and `collect` are described in
[return, order, and limit](index.md#return-order-and-limit). Two more
aggregate a group's non-null numeric values into a nullable `F64`, each value
converted to the nearest `F64` first:

- `stddev(x)` is the sample standard deviation (divided by n − 1). It is
  null over fewer than two values.
- `percentile(x, p)` is the exact percentile with linear interpolation
  (SQL's `PERCENTILE_CONT`). It is null over no values. `p` is a numeric
  literal from 0 to 1 or a declared numeric parameter. A literal outside
  that range, a column, or a missing `p` is `T8`. A parameter bound outside
  the range, or to null, is `E5` before the read reads a row.

Both hold the group's values in the query's memory, `query_memory_bytes`
([session settings](settings.md)). Neither takes `distinct` (`T57`). Both
are block values too, as in `where percentile($s.amount, 0.5) { $s: Sale,
$h sells $s } >= $m` ([correlated blocks](index.md#correlated-blocks)).

## Errors at run time

A function that can fail refuses the whole read when one row fails, with an
execution code: `E1` integer overflow, `E3` a float result that is not
finite, `E5` an argument outside the function's domain, `E6` a value that
`cast` does not convert ([execution codes](diagnostics.md#execution-codes)).
It never returns `NaN`, an infinity or a silent null. In `return` the
function runs only on the rows `where` keeps. In a `let` it runs on every row
before `where`, so guard it with `if`:
`let $r = if($x.n >= 0, sqrt($x.n), null)`
([values](traversal.md#values-and-inline-reads)).

## Indexes

An index covers a column, never a function of it, and no function call is
sent to the storage scan. So a filter over a function of a column, such as
`lower($p.email) = $q`, `abs($x.n) < 3` or `contains(lower($d.title), $q)`,
is tested on each row the scan reads; no index narrows it. A conjunct over a
bare column in the same `where` can still use its index, and the rows are the
same with or without one. To use an index, compare the bare column:
`$p.email = $q`, `$x.n < 3` or `contains($d.title, $q)`. For a
case-insensitive lookup that needs an index,
store a normalized copy of the value in its own indexed property and compare
that property ([what `@index` builds](../schema/index.md#what-index-builds)).

## Other accepted spellings

GQ has one canonical spelling for each meaning. The parser also accepts the
spellings below, which SQL, GQL and Cypher share. Each one means exactly its
canonical form, and [`lint`](lint.md#canonical-forms) names it with that form
as its fix. `omnigraph lint --fix` rewrites them ([fix](lint.md#fix)).

| Accepted | Canonical form |
|---|---|
| `case when c then a when d then b else e end` | `if(c, a, if(d, b, e))`; a missing `else` is `null` |
| `case x when v then a … end` | `if(x = v, a, …)`, so a null `x` takes the `else` |
| `a \|\| b \|\| c` | `concat(a, b, c)`; `\|\|` binds like `+` |
| `<>` | `!=` |
| `'text'`, with a double-quoted string's escapes (`\'`, `\"`, `\\`, `\n`, `\r`, `\t`) | `"text"` |
| `WHERE`, `Count`, `LOWER(…)`: a keyword or function name in another letter case | the lower-case word |

`case`, `when`, `then`, `else` and `end` are keywords only where a value
starts. Elsewhere they are names, so a property called `case` still works.
`==`, `&&` and `!` are refused, with the GQ form as the fix
([parse codes](diagnostics.md#parse-codes)).
