- A graph that 0.16 creates (`init`, a new cluster graph, a rebuild) is storage format 17, which 0.15.x and older refuse to open. Existing graphs stay at 16 until `omnigraph upgrade`, which has no way back. Upgrade every server and CLI that opens a graph before creating one with 0.16. See [storage upgrade][format-17-upgrade].

[format-17-upgrade]: ../docs/user/operations/upgrade.md#storage-upgrade
