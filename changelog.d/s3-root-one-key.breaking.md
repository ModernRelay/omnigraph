- An `s3://` root whose refusal names two keys, Omnigraph's and Lance's (from a space, a non-ASCII letter, a brace or an escape Lance decodes, such as `%41`), is now refused before any request: earlier releases split its objects between those keys. Export each graph there with the earlier release and rebuild it under a plain prefix; see the [upgrade guide][s3-root-one-key-upgrade].

[s3-root-one-key-upgrade]: ../docs/user/operations/upgrade.md#s3-roots-that-name-two-keys
