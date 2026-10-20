- A `load csv` receipt's `executed` object carries `operators`: what each
  node of the executed plan did, in post-order, with the operator's rows, its
  compute time (`elapsed_compute_ns`) and its wall span (`wall_ns`). The
  statement's `ChangeOutput`, from `omnigraph mutate --json` and `POST
  /mutate`, carries the object as `executed` for a `mode overwrite` load, and
  a GQT load step's `--- expect plan` section accepts
  `load executed <Node>: rows <n>` lines ([explain guide][load-executed-guide]).

[load-executed-guide]: ../docs/user/queries/explain-load.md
