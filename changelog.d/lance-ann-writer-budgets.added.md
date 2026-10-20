- Query inspection reports optional ANN stage timings and prefilter work for each nearest-search attempt. Strict inserts and overwrite writes now use the existing `write_max_bytes` setting as Lance's per-writer buffer accumulation target; this is a flush threshold, not a total memory cap. See [query profiles][profiles] and [write settings][settings].

[profiles]: ../docs/user/queries/explain.md#profile
[settings]: ../docs/user/queries/settings.md
