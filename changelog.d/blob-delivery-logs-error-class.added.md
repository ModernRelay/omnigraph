- Blob delivery failures log their error class. A `GET` or `HEAD /blob` 500
  logs `error_kind="blob_pre_header_internal"` with a `stage`: `target` is a
  failure resolving a snapshot target for a policy-gated request, `cell` is
  any failure of the engine's Blob read, including the branch or snapshot
  resolution it does itself, and `transport` is the server's own refusal
  before headers. The `target` and `cell` stages log `error_variant` (for
  example `Storage` or `BlobIntegrity`), `storage_kind` and `manifest_kind`;
  `transport` logs `error_variant="unclassified"`. A managed Blob body that
  fails mid-stream logs the byte range with one of three kinds:
  `blob_payload_read` with the same error class, `blob_payload_short_read`
  with the returned and expected byte counts, or `blob_payload_permit_closed`.
  The error's message stays out of both the response and the log, because it
  can hold object URIs or credentials; the response is unchanged. See
  [HTTP errors][blob-delivery-error-class-log].

[blob-delivery-error-class-log]: ../docs/user/operations/troubleshooting.md#http-errors
