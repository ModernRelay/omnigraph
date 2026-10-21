- The server's `engine failure without definite completion evidence` log line
  names the graph commit of a publication whose outcome is unknown, in its own
  `publication` field. Look that commit up to learn whether the write landed;
  the line still carries no error message. See
  [storage failures][unknown-publication-log].

[unknown-publication-log]: ../docs/user/operations/storage-failures.md#logs
