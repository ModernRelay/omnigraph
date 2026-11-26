- An `s3://` root whose refusal names another spelling (a dot segment, `%2e%2e` included, a port or user information) is now refused, but its objects are whole: finish any outstanding deployment with the earlier release, then use that spelling. A refusal that names no key (a query, a fragment, an empty segment) names no replacement: choose a plain prefix. See the [upgrade guide][s3-root-one-spelling-upgrade].

[s3-root-one-spelling-upgrade]: ../docs/user/operations/upgrade.md#s3-roots-that-name-two-keys
