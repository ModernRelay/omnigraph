- A Blob write whose branch was recreated, or whose schema changed meanwhile, returns `read_set_conflict` as other writes do, not a generic `conflict`. See [Blob writes][blob-identity-refusal].

[blob-identity-refusal]: ../docs/user/blobs.md
