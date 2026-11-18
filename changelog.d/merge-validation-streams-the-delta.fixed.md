- A `branch merge` no longer refuses a delta for the width of its rows:
  validation streams the delta and counts only its keys toward its 32 MiB
  limit. See the [validation limit][merge-validation-streams].

[merge-validation-streams]: ../docs/user/branching/merge.md#validation-limit
