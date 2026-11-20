- A keyed load of wide vectors is no longer refused at about half of `write_max_bytes`: its input estimates now charge a vector its Arrow size. See [load limits][wide-vector-load-limits].

[wide-vector-load-limits]: ../docs/user/mutations/index.md#limits-and-conflicts
