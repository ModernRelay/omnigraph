- A served mutation, load, Blob write or merge can keep serving after a staging
  failure that is proven unpublished; uncertain publication, mutable control
  writes and unknown paths still fail closed. Lost acknowledgements for
  supported Lance 11 fresh-file and create-only manifest writes settle the
  physical storage scope, while retaining transaction files until cleanup
  from a replacement scope satisfies its usual safety checks. See
  [shutdown and writer ownership][definite-storage-failures].

[definite-storage-failures]: ../docs/user/deployment.md#writer-topology
