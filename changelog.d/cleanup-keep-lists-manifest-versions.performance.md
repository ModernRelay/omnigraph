- `cleanup` lists `__manifest` versions instead of reading every one: `--keep N`
  alone reads only those it retains and the merge bases, and `--older-than`
  reads the others' times concurrently. See [cleanup][cleanup-keep-listing].

[cleanup-keep-listing]: ../docs/user/operations/maintenance.md#cleanup
