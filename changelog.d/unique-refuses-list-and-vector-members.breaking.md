- Schema admission refuses a body-level `@unique` on a list or vector property, which no write could satisfy. A graph that already declares one still opens. Before reapplying its source, drop the constraint by dropping the properties it names, or rebuild the graph if it also names other members. See [unique constraints][unique-list-vector-schema].

[unique-list-vector-schema]: ../docs/user/schema/index.md#constraints
