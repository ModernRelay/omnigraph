- Blob delivery failures log their error class. A `GET` or `HEAD /blob` 500,
  including one raised while resolving the requested branch or snapshot, and
  a managed Blob body that fails mid-stream now log `error_variant` (for
  example `Storage` or `BlobIntegrity`), `storage_kind` and `manifest_kind`,
  with the stage or byte range. The error's message stays out of both the
  response and the log, because it can hold object URIs or credentials; the
  response is unchanged. See [HTTP errors][blob-delivery-error-class-log].

[blob-delivery-error-class-log]: ../docs/user/operations/troubleshooting.md#http-errors
