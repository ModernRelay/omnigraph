- `omnigraph load --json` prints the body of the HTTP load route,
  `POST /graphs/{id}/load/ndjson`, on direct, served and managed targets
  alike. The CLI-only `uri` field is gone; `actor_id`, the actor the write is
  attributed to (`null` when none), is new; and `base_branch` is always
  present, `null` without `--from`. A script that read `uri` from the load
  output should take the graph address from its own `--target`, which the
  target line on stderr also reports; the human output still names it. See
  [machine-readable positions][load-json-route-body].

[load-json-route-body]: ../docs/user/cli/reference.md#machine-readable-read-and-write-positions
