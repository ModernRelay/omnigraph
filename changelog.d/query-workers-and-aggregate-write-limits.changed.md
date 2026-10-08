- Query success and errors wait for registered graph producers and blocking
  workers; cancellation retains their resources until they stop. Multi-table
  keyed writes, keyed parsed-payload estimates and removed-ID collections now
  have separate 32 MiB operation-wide limits, with typed refusal before data
  staging. Large deletes and overwrite loads that previously succeeded can now
  be refused: the removed-ID limit holds 671,088 generated IDs, no setting
  changes it, and an overwrite cannot be split. These limits do not bound total
  memory or qualify native I/O settlement and online deployment. See
  [mutation limits][write-limit-guide].

[write-limit-guide]: ../docs/user/mutations/index.md#limits-and-conflicts
