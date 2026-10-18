- The `write_max_bytes` session setting no longer has a fixed maximum: the
  former `33554432` ceiling is removed and any value of at least `1` is
  accepted, with the default unchanged at 32 MiB. A value raised above the
  default is held in memory as one typed batch at an overwrite load's commit;
  append and merge keep their 8,192-row chunk bound, and the Blob put limit
  stays capped at 32 MiB.
