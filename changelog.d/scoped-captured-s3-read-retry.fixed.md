- A scoped S3 adapter built from a captured binding, which a managed first bootstrap reads through, and an S3 clone open now retry a read that fails transiently, within the [scoped retry budget][scoped-s3-read-retry].

[scoped-s3-read-retry]: ../docs/dev/control-plane.md#concurrency
