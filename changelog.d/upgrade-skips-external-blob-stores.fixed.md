- `omnigraph upgrade` no longer contacts external Blob stores. Validating a
  source table's Blob dependencies opened every external reference, so a
  moved or unreachable external object failed the upgrade. The upgrade now
  lists each external reference in `work.external_blob_exclusions` from its
  stored descriptor and reads back only managed bytes. A table whose only Blob
  fields are nested inside other fields now fails the preflight instead of
  passing unvalidated. See the
  [storage upgrade guide][upgrade-external-blob-exclusions].

[upgrade-external-blob-exclusions]: ../docs/user/operations/upgrade.md#explicit-storage-migration
