- A read on a warm graph handle checks for new commits by asking for the next
  `__manifest` version by name instead of listing the version directory. On
  S3-compatible storage that listing returned up to 1,000 keys on every read
  once a branch passed 1,000 commits; the check is now one small HEAD request
  while nothing changed. See the [catalog freshness probe][freshness-probe].

[freshness-probe]: ../docs/dev/lance.md#current-compatibility-fences
