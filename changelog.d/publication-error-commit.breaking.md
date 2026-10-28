- Rust embedders must pass a graph commit ID to `manifest_publish_in_doubt`;
  `ManifestError.publication_in_doubt` now holds `Option<Box<String>>` instead of
  `bool`. Follow the [error API update][publication-error-api].

[publication-error-api]: ../docs/user/operations/storage-failures.md#rust-error-api
