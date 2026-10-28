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
unchanged, up to four times with about three seconds of pauses in total, plus
the time spent on each storage request. A re-issue that creates the version
publishes the commit once. A version that already exists is read
back: it holds either this commit, and the write succeeds, or another commit,
and the write meets an ordinary conflict. Re-issuing the same conditional
request never publishes a duplicate.

## Unknown publications

The outcome stays unknown if no re-issue settles the request or the committed
version cannot be read back reliably. A local filesystem error can also occur
after a write has taken effect, so an error alone does not prove that nothing
was published. When the outcome remains unknown, the write answers 500
`outcome_unknown` on HTTP and MCP alike, and the server keeps serving. Each
manifest version is created only if absent, so the request lands before the
branch's next publication or never: the branch's next write either finds it and
builds on it, or takes that version first, after which the late request can no
longer land. A read that does not show the write is therefore not proof that it
failed until another commit follows its base; read the branch's commits before
repeating it.

The same rule applies when creating a proposal or an unattached snapshot: if
its final authority check fails, deleting its tag does not prove that an earlier
create request cannot still complete. That outcome remains unknown unless the
candidate is proven expired.

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
exits 2. Their effects are not scoped to one write's storage keys.

## Restart after a failure

On S3 roots a server's cluster lock carries a lease, so an orchestrator can
restart a failed server with no operator step. Set its restart policy to
restart on failure (Railway `ON_FAILURE`, Kubernetes `restartPolicy: Always`).

- The lock's owner renews the lease with heartbeat objects under
  `__cluster/lease/` every third of a lease. Heartbeats retry on their own, and
  one whose answer was lost counts once a retry finds it. When no heartbeat
  is answered for two thirds of a lease, or a successor claims the lock, the
  owner stops every storage write at once and exits 2 without releasing. A
  server whose first heartbeat is not answered within that time does not
  start serving.
- A restarted server that finds the lock held watches those heartbeats. It
  refuses with `state_lock_held` while they continue, and takes the lock over
  once they have been silent for a lease and a half.
- The lease is 30 seconds by default, so a restart after a crash waits about
  45 seconds. `OMNIGRAPH_CLUSTER_LEASE_SECONDS` sets it between 2 and 600
  seconds; a successor waits on the value the owner recorded.
- Only a server's lock has a lease, and only a server takes a lock over. A
  direct CLI write or deployment holds an unleased lock and refuses a held
  lock at once; a lock one of them retains needs the exact-ID unlock.
- A request the old owner sent before it stopped can still reach storage
  after the takeover. It cannot corrupt the graph: it writes a fresh file
  nothing references, makes a conditional publication the new owner also
  contends for, or writes a reused key whose late landing is harmless.

A lock with no lease still needs the exact-ID unlock described under
[writer topology](../deployment.md#writer-topology). Local and Azure locks have
no lease, and neither does a CLI writer's lock or a lock an older binary
wrote. A local root has no
conditional update, so a successor could not replace the lock without racing a
release the old owner already sent. Older binaries refuse the leased
(version 3) lock record; do not delete it to bypass the version check.

## Logs

No storage-failure log carries an error's message, which can name storage
paths; each records the failure's class.

- `engine failure without definite completion evidence`: the failure's class,
  the status its response sends, and whether its uncertainty is scoped to its
  storage keys. When the failure is a publication whose outcome is unknown, its
  `publication` field names the graph commit, so you can look the commit up
  later and see whether it landed.
- `owned write ended without a definite outcome`: the status its response
  sends, 500 for `outcome_unknown`, and whether it contained the process.
- `manifest request outcome unknown; re-issuing the identical conditional
  request`, for each re-issue, and
  `manifest request outcome unknown after every re-issue` when none settles.
- `took over a silent owner's cluster lock` when a restarted server takes over
  its predecessor's lock, and an error when an owner's lease lapses or another
  server claims it.

## Rust error API

Rust embedders updating from 0.14.0 must pass the publication's graph commit ID
before the diagnostic message to
`OmniError::manifest_publish_in_doubt(graph_commit_id, message)`. The ID must
name the attempted publication; do not substitute a storage path or invent an
ID when the outcome is unknown.

`ManifestError.publication_in_doubt` changes from `bool` to
`Option<Box<String>>`: `Some` carries that commit ID, and `None` means this is
not an in-doubt publication. Prefer the constructor over struct literals, and
use `is_manifest_publish_in_doubt()` or `publication_id()` to inspect an
`OmniError`. HTTP error responses and CLI syntax are unchanged.
