- `cleanup --keep N` without `--older-than` lists each live branch's
  `__manifest` versions instead of reading every one, so planning no longer
  reads one manifest per graph commit. See [cleanup][cleanup-keep-listing].

[cleanup-keep-listing]: ../docs/user/operations/maintenance.md#cleanup
