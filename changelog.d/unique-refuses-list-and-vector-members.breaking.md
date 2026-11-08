- Schema admission refuses a body-level `@unique` on a list or vector property, which no write could satisfy. A graph that already has one still opens, but every apply of source that keeps it is refused, including any `cluster apply` of its deployment. Drop the properties it names in one change, or rebuild the graph. See [unique constraints][unique-list-vector-schema].

[unique-list-vector-schema]: ../docs/user/schema/index.md#constraints
