- An `s3://` root whose path holds a percent escape or a character a URL escapes (a space, a non-ASCII letter, a brace) is now refused before any request: its objects landed under two key prefixes, so removing a graph deleted nothing. A cluster or graph at such a root must be exported with the earlier release and rebuilt under a plain prefix; see the [upgrade guide][s3-root-one-key-upgrade].

[s3-root-one-key-upgrade]: ../docs/user/operations/upgrade.md#s3-roots-that-name-two-keys
