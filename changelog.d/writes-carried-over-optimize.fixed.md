- A write that overlaps an `optimize` publication in the same process no longer returns `read_set_conflict`, and prepares again at most once. See [maintenance][carried-writes-maintenance].

[carried-writes-maintenance]: ../docs/user/operations/maintenance.md#optimize
