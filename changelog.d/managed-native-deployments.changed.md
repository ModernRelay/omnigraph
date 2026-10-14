- Managed cluster commands now use immutable previews and native deployment
  delivery. Apply uses `data.preview_id`; exact delivery status supports
  `--wait`, and cancellation is limited to queued, never-attempted delivery.
  Managed history is bounded to 100 entries. JSON and exit codes follow the
  [managed deployment contract][managed-native-deployments]; direct commands
  and cloud lifecycle outcomes are unchanged.

[managed-native-deployments]: ../docs/user/cli/reference.md#managed-cluster-commands
