- `omnigraph upgrade` no longer contacts external Blob stores. Validating a
  source table's Blob dependencies opened every external reference, so a
  moved or unreachable external object failed the upgrade. The upgrade now
  lists each external reference in `work.external_blob_exclusions` by the URI
  its stored descriptor holds, without its byte range, and reads back only
  managed bytes. Blob validation now fails the upgrade's preflight on a Blob
  field nested inside another field (a table whose only Blob fields are
  nested used to pass unvalidated), on a top-level Blob column not in the
  Blob-v2 encoding, and on a Blob descriptor that fails the engine's integrity
  checks, such as an external URI that is not absolute; a check reports each
  as a `preflight_failed` finding, and the validation itself writes nothing.
  See the [storage upgrade guide][upgrade-external-blob-exclusions].

[upgrade-external-blob-exclusions]: ../docs/user/operations/upgrade.md#explicit-storage-migration
