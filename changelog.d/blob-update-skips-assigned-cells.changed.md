- An update no longer reads the Blob cells it assigns, and a stored external
  reference the policy refuses has its own error. Replacing or clearing a
  Blob never takes, probes or reads its old value, so it succeeds when that
  value is an external reference whose source is gone or outside the graph's
  external Blob policy. An update that must carry such a reference in a Blob
  it does not assign fails with the new typed
  `OmniError::StoredExternalBlobDenied` (HTTP 400), naming the type, id and
  property, where it earlier failed with the misleading `ExternalBlobPolicy`
  message about new URI ingress.
