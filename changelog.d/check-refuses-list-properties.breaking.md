- Schema admission refuses `@check` on a list property, which no write could enforce. A graph that already has one still opens and its writes stay unchecked, but every apply of source that keeps it is refused, including any `cluster apply` of its deployment. Drop the property in one change and add it back without the constraint. `@range` still bounds each element of a numeric list. See [constraints][check-scalar-schema].

[check-scalar-schema]: ../docs/user/schema/index.md#constraints
