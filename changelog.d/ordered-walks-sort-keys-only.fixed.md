- Commit changes, the change feed, its baseline and export no longer fail with
  `internal error while reading changes` or an `ordered_scan_input_batch_bytes`
  resource error on tables holding a very wide row or a large fragment of
  multi-kilobyte rows. These walks now sort only row keys and read complete rows
  in bounded chunks, as branch merge already did.
