- `commit publish` and `commit drop` accept `--as` on a direct graph, as the other direct writes do; they previously refused it as if they were reads. `commit list`, `show` and `changes` still refuse `--as`, and a served publish or drop still takes its actor from the bearer token. A published commit keeps the actor and timestamp of the write that created it. See [unpublished commits][commit-publish-drop-as].

[commit-publish-drop-as]: ../docs/user/branching/index.md#unpublished-commits
