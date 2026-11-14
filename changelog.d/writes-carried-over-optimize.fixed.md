- Writes no longer stall or fail while `optimize` publishes in the same process: a write that meets an optimize commit is carried over it without preparing again. See [maintenance][carried-writes-maintenance].

[carried-writes-maintenance]: ../docs/user/operations/maintenance.md#optimize
