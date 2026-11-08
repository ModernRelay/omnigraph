- A served write whose publication fails before its manifest request is sent,
  for example on a throttled transaction-file put, now returns its 500 and
  keeps serving instead of containing the process. Ambiguous manifest requests
  are re-issued unchanged and verified without publishing a duplicate; local
  filesystem failures after publication and candidate-tag rollback remain in
  doubt unless their outcome is proven. Contained writes now log the failure's
  class. See [storage failures][publication-failures-definite].

[publication-failures-definite]: ../docs/user/operations/storage-failures.md
