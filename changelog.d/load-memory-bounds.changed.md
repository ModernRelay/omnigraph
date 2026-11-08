- One `load csv` statement's retained source, decoded batches, validation
  state and staging working set share one [`query_memory_bytes`][load-memory-settings]
  allowance, and a statement it cannot hold refuses before any fragment write.
  `write_max_bytes` bounds each staged-write input of a CSV load, an overwrite
  batch refusing as `staged_write_input_bytes`, and the whole-statement
  `strict_input_arrow_bytes` check applies to JSONL loads alone.
  A source replaced between its metadata probe and its read refuses as an
  external-source error with nothing staged ([CSV loading limits][load-memory-csv]).

[load-memory-settings]: ../docs/user/queries/settings.md
[load-memory-csv]: ../docs/user/mutations/index.md#limits-and-conflicts
