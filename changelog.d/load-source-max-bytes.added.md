- The `load csv` source bound is now the [`load_source_max_bytes` session
  setting][load-source-max-bytes] (`request` scope, at least `1`, unchanged
  32 MiB default, `OMNIGRAPH_LOAD_SOURCE_MAX_BYTES` supplying the process
  default) instead of a hardcoded 32 MiB constant. A source over the setting
  is still refused as `csv_source_bytes` before any payload byte is read, and
  a served load's workload-admission reservation follows the request's value.

[load-source-max-bytes]: ../docs/user/queries/settings.md
