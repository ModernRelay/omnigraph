- An `s3://` root whose path holds a percent escape, a space (a trailing one too), a non-ASCII letter or a brace is now refused before any request: earlier releases split its objects across two key prefixes, so removing a graph deleted nothing. Export each graph there with the earlier release and rebuild it under a plain prefix; see the [upgrade guide][s3-root-one-key-upgrade].

[s3-root-one-key-upgrade]: ../docs/user/operations/upgrade.md#s3-roots-that-name-two-keys
