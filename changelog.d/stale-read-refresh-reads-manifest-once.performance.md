- A read after another process commits refreshes `__manifest` with one read of
  the new version instead of two, about 40% fewer requests for that read. See
  the [freshness probe][stale-read].

[stale-read]: ../docs/dev/lance.md#current-compatibility-fences
