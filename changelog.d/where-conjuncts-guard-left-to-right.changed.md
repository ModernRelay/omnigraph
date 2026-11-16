- A `where`'s conjuncts, blocks included, filter in written order, so `$n != 0 and $t / $n > 1` never divides by zero and a failing conjunct is not skipped for a later one. See the [traversal guide][guard-guide].

[guard-guide]: ../docs/user/queries/traversal.md
