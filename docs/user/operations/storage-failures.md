# Storage failures and restart

A served write sends many storage requests: data files, a transaction file,
and last the conditional create of the graph's next manifest version, which
publishes the commit. This page describes what a
client and an operator see when one of those requests fails or gets no answer.
A request's outcome belongs to the write that sent it: one failed or unanswered
request never stops the server. Process shutdown and admission are described
under [Deployment](../deployment.md#binary).

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

## Pending publications

A manifest request that no re-issue settles leaves its write's outcome unknown,
and its publication pending on its branch:

- That write answers 500 (`outcome_unknown` on MCP).
- The branch's later writes answer 503 `service_unavailable`, naming the
  pending graph commit, with nothing written.
- Reads, other branches and other graphs keep serving.
- The server re-issues the pending commit every 5 to 60 seconds. Once storage
  answers, the pending write either completes or is proven to have lost to
  another commit, and the branch accepts writes again.

Until then a read that does not show the write is not proof that it failed, so
do not repeat it; retry the refused writes once the branch accepts writes. A
clean shutdown gives every pending publication one more attempt and retains
admission if one stays pending. One process holds at most 64 pending
publications; past that every publication is refused.

## Retries

The server's storage clients retry reads, and writes of fresh uniquely named
files, within Lance's own bounds: three client retries and three throttle
retries. A conditional request, such as a manifest create, and a write to a
reused key make one attempt each, and their owner resolves an ambiguous answer
itself. `OBJECT_STORE_CLIENT_MAX_RETRIES`, if set, must be `0`.

A write to a reused key whose outcome is unknown refuses later writes to that
key only, until a clean shutdown; a clean shutdown with such a key retains
admission.

## When the server stops

A panic, a lost write owner, a publication adopted in storage but not in
memory (`RecoveryRequired`), an opaque execution failure, or an uncertain
startup or deployment activation still closes admission for every graph and
exits 2. Their effects are not scoped to one write or branch.

## Restart after a failure

On local and S3 roots the cluster lock carries a lease, so an orchestrator can
restart a failed server with no operator step. Set its restart policy to
restart on failure (Railway `ON_FAILURE`, Kubernetes `restartPolicy: Always`).

- The lock's owner renews the lease with heartbeat objects under
  `__cluster/lease/` every third of a lease. When two heartbeats in a row go
  unanswered, or a successor claims the lock, it stops writing and exits 2
  without releasing.
- A restarted server that finds the lock held watches those heartbeats. It
  refuses with `state_lock_held` while they continue, and takes the lock over
  once they have been silent for a lease and a half.
- The lease is 30 seconds by default, so a restart after a crash waits about
  45 seconds. `OMNIGRAPH_CLUSTER_LEASE_SECONDS` sets it between 2 and 600
  seconds; a successor waits on the value the owner recorded.
- Only a serving process takes a lock over. A direct CLI write or deployment
  refuses a held lock at once. The lock a finished CLI write retains stops
  renewing when the command exits, and the next server takes it over.
- A request the old owner sent before it stopped can still reach storage
  after the takeover. It cannot corrupt the graph: it writes a fresh file
  nothing references, makes a conditional publication the new owner also
  contends for, or writes a reused key whose late landing is harmless.

A lock with no lease, one an older binary wrote or one on an Azure root, still
needs the exact-ID unlock described under
[writer topology](../deployment.md#writer-topology). Older binaries refuse the
leased (version 3) lock record; do not delete it to bypass the version check.

## Logs

No storage-failure log carries an error's message, which can name storage
paths; each records the failure's class.

- `engine failure without definite completion evidence`: the failure's class,
  and whether its uncertainty is scoped to its keys and branch.
- `owned write ended without a definite outcome`: the write's status, and
  whether it contained the process.
- `manifest request outcome unknown; re-issuing the identical conditional
  request`, for each re-issue, and
  `manifest request outcome unknown after every re-issue` when none settles.
- `publication pending` when a publication becomes pending on its branch, and
  `pending publication resolved` when storage answers.
- `took over a silent owner's cluster lock` when a restarted server takes over
  its predecessor's lock, and an error when an owner's lease lapses or another
  server claims it.
