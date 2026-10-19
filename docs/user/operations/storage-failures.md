# Storage failures

A served write sends many storage requests: data files, a transaction file,
and last the conditional create of the graph's next manifest version, which
publishes the commit. This page describes what a
client and an operator see when one of those requests fails or gets no answer.
Process shutdown and admission are described under
[Deployment](../deployment.md#binary).

## Publication outcomes

A graph commit becomes visible only when its manifest version is created,
put-if-absent. A failure before that request is definite: nothing was
published, the write answers its error, and the server keeps serving once the
write's own storage requests have settled. That includes a failed put of the
transaction file Lance writes just before the manifest, for example one the
store throttled with `SlowDown`, and a manifest request the store refused
outright.

A manifest request whose answer was lost, timed out or throttled is re-issued
unchanged, up to four times over about three seconds. A re-issue that creates
the version publishes the commit once. A version that already exists is read
back: it holds either this commit, and the write succeeds, or another commit,
and the write meets an ordinary conflict. A dropped acknowledgement therefore
ends as a committed write or a definite conflict, never a duplicate.

Only a manifest request that no re-issue settles leaves the write's outcome
unknown. The write answers 500 (`outcome_unknown` on MCP), and the server
contains itself as described under [Deployment](../deployment.md#binary). Do
not retry such a write blindly; read the branch's commits first.

## Logs

A contained write logs `owned write ended without a definite outcome` with
its status, and the engine failure behind it logs
`engine failure without definite completion evidence` with the failure's
class. Neither line carries the error's message, which can name storage
paths. Each re-issued manifest request logs a warning, and one that no
re-issue settles logs `manifest request outcome unknown after every re-issue`.
