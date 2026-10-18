- **Assert the actor a credential must resolve to.** A request carrying
  `Omnigraph-Expect-Actor: <actor>` is refused with `412 actor_mismatch` before
  graph lookup or effects when its credential resolves to another actor. `--as`
  on a served write or a `cluster --server` command now sends it instead of
  being refused, and a mismatch exits 4 with nothing run. Every response lists
  `expect-actor` in `Omnigraph-Http-Api-Assertions`; against a server that does
  not, which would ignore the header, `--as` refuses before sending. See
  [Assert the actor][expect-actor-guide].

[expect-actor-guide]: ../docs/user/operations/server.md#assert-the-actor
