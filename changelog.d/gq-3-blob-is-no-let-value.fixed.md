- A Blob property bound by `let`, tested with `is null`, or passed to a
  function outside an assignment is `T24` at type check; the read used to be
  accepted and then fail when it ran. See [Blobs][blob-no-read-value].

[blob-no-read-value]: ../docs/user/blobs.md#query-behavior
