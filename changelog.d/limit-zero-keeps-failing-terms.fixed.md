- A read with `limit 0` now refuses when a return item, order key or filter fails
  on a row, as any other `limit` does, instead of returning no rows.
