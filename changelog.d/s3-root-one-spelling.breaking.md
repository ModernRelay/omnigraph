- An `s3://` root with a query, a fragment, an empty segment, a dot segment (also `%2e%2e`), a port or user information is now refused before any request, since it respells another root's key. Its objects are not split: finish any outstanding deployment with the earlier release, then use the spelling the refusal names; see the [upgrade guide][s3-root-one-spelling-upgrade].

[s3-root-one-spelling-upgrade]: ../docs/user/operations/upgrade.md#s3-roots-that-name-two-keys
