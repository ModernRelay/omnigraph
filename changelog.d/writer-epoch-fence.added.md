- Branches gain a [writer epoch][writer-epoch-guide]: once a program embedding
  the engine claims a branch, other processes' writes get 409 `writer_fenced`.
  New graphs are storage format 17; older binaries refuse them.

[writer-epoch-guide]: ../docs/user/operations/troubleshooting.md#writer-epoch
