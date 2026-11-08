- A CLI data write refused because an allowed external Blob source could not
  be read (HTTP 424 with `external_blob_source`) now reports
  `command_outcome.action` `refresh` instead of `reconcile`. The source is
  read during admission, before the write has any effect, so restore the
  source or correct the reference and run the command again. See
  [failed data-write commands][external-blob-source-refresh-outcome].

[external-blob-source-refresh-outcome]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
