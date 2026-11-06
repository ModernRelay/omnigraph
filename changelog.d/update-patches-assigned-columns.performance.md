- A scalar-only `update` on a type with Blob or vector properties writes just those columns when cheaper or when whole rows exceed `write_max_bytes`, keeping external Blobs unread. See the [Blob guide][update-column-patch].

[update-column-patch]: ../docs/user/blobs.md
