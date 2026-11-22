- Schema admission refuses `@check` on a list property, which no write could enforce. A graph that already has one still opens, but every apply of source that keeps it is refused. `@range` still bounds each element of a numeric list. See [constraints][check-scalar-schema].

[check-scalar-schema]: ../docs/user/schema/index.md#constraints
