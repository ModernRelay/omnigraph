- A triple is written in its edge's declared order, source first: over `WorksAt: Person -> Company`, `$c worksAt $p` is refused with `T82` whatever the end types, and its fix swaps the ends to `$p worksAt $c`, which reads the same edges. Between ends of one type the written order was already the direction. See [traversal][gq-3-t82-guide].

[gq-3-t82-guide]: ../docs/user/queries/traversal.md
