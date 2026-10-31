- **`--as` on a served write is an assertion, no longer a refusal.** A request carrying
  `Omnigraph-If-Actor: <actor>` is refused with `412 actor_mismatch` before
  graph lookup or effects when its credential resolves to another actor. `--as`
  on a served write or a served `cluster plan` or `cluster apply` now sends it instead of
  being refused, and a mismatch exits 4 with nothing run; a script that relied on
  that refusal now runs the write when the token's actor matches. The header
  arrives with HTTP contract `0.14`, so a server that would ignore it refuses
  the request's contract first. See
  [Assert the actor][if-actor-guide].

[if-actor-guide]: ../docs/user/operations/server.md#assert-the-actor
