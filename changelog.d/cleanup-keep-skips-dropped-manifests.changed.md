- `cleanup --keep N` without `--older-than` no longer stops on an unreadable
  catalog manifest of a graph commit it drops, since it no longer reads one;
  `--older-than` still stops. See [cleanup][cleanup-unread-drops].

[cleanup-unread-drops]: ../docs/user/operations/maintenance.md#cleanup
