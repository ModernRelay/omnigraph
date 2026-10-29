- Embedded hosts can bound optimize per table (`TableCeilings`) and per run
  (`run_source_bytes`); a table over a ceiling is deferred before its first
  write, with nothing written for it.
