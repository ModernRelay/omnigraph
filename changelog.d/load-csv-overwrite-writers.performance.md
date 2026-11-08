- A `load csv` overwrite now writes its Lance files through up to
  `stage_write_concurrency` writers at once (default 8) instead of one after
  another: the validated batches are cut into contiguous runs of at most one
  Lance file (1 M rows), each run is one writer, and the files are published in
  input order, so a table's files are the ones a single writer would have
  produced. The setting's description names this second use beside the
  cross-table width it already controlled.
