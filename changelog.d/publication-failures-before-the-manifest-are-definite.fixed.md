- A served write whose publication fails before its manifest request is sent,
  for example on a throttled transaction-file put, now returns its 500 and
  keeps serving instead of containing the process. A manifest request whose
  answer is lost is re-issued unchanged until the store answers it, so a
  dropped acknowledgement ends as a committed write or a definite conflict.
  Only a request that no re-issue settles is still reported in doubt. The log
  of a contained write now names the failure's class. See
  [storage failures][publication-failures-definite].

[publication-failures-definite]: ../docs/user/operations/storage-failures.md
