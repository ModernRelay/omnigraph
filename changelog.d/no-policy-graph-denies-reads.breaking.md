- Any static bearer token could read every graph on the server that had no applied policy. Such a graph now refuses reads as it refuses every other action. Apply a policy granting `read` to each graph a token should read, or set `OMNIGRAPH_ALLOW_READS_WITHOUT_POLICY=1` on the server to keep the old behaviour. See [authentication][no-policy-reads].

[no-policy-reads]: ../docs/user/operations/server.md#authentication
