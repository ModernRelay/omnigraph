- An edge `load csv` over integer-keyed node types checks its endpoints on the parse partitions as rows arrive, not on one thread after the parse. See [explain load][explain-load-probe].

[explain-load-probe]: ../docs/user/queries/explain-load.md
