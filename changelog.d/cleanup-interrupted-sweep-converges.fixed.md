- On a local graph, `cleanup` now finishes a table whose previous run stopped after deleting a table version's unused index files and before deleting the version itself. Before, every later run failed for that table on the missing index directory and deleted nothing; it now counts the directory as already removed and deletes the version. See the [maintenance guide][cleanup-interrupted-sweep].

[cleanup-interrupted-sweep]: ../docs/user/operations/maintenance.md#cleanup
