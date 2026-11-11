- A write that overlaps an `optimize` or index-build publication no longer prepares again or returns `read_set_conflict`, unless it updates or deletes a rewritten row. See [maintenance][carried-writes-maintenance].

[carried-writes-maintenance]: ../docs/user/operations/maintenance.md#optimize
