- A `load csv` overwrite into an empty node table or an edge table no longer scans its committed ids or hashes every input id, the largest cost of a bulk load into a new graph. See [explain load][explain-load-removal].

[explain-load-removal]: ../docs/user/queries/explain-load.md
