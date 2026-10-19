- **`--as` on a served write is an assertion, no longer a refusal.** A request carrying
  `Omnigraph-Expect-Actor: <actor>` is refused with `412 actor_mismatch` before
  graph lookup or effects when its credential resolves to another actor. `--as`
  on a served write or a served `cluster plan` or `cluster apply` now sends it instead of
  being refused, and a mismatch exits 4 with nothing run; a script that relied on
  that refusal now runs the write when the token's actor matches. Every response lists
  `expect-actor` in `Omnigraph-Http-Api-Assertions`; against a server that does
  not, which would ignore the header, `--as` refuses before sending. See
  [Assert the actor][expect-actor-guide].

[expect-actor-guide]: ../docs/user/operations/server.md#assert-the-actor
