- A keyed write (`load --mode merge`, a mutation `insert` on a keyed type, and
  a branch merge) now builds its merge join on the rows being written instead
  of on the whole table. On a table past 128 Ki rows, a small upsert no longer
  holds every row's key in memory: in one measurement of a 2,000-row upsert
  into a 2,000,000-row table, the join's memory fell from 129 MB to 111 KB.
  Results are unchanged.
