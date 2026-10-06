- Rust callers: `OmniError` has a new variant, `StoredExternalBlobDenied`,
  for an update that must carry a stored external Blob reference the graph's
  policy refuses. An exhaustive `match` on `OmniError` must add an arm for it.
