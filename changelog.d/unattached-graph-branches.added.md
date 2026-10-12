- Unattached graph branches (RFC "Unattached graph branches"): a mutation
  declaration may end in `as unattached branch`, optionally `on "<id>"` or
  `on $base`, which writes the complete result as an unattached snapshot,
  readable by its graph commit id (`kind: "unattached"`, `published: false`
  and `root_commit_id` on the receipt's `commit`), invisible to every branch
  read, and extendable by a further write `on` it; the caller holds the
  latest id as its tip, and two writes on one base each get their own
  snapshot. A snapshot never publishes: `commit publish "<id>"` and
  `POST /commits/{commit_id}/publish` answer 409 with the error code
  `unattached_cannot_publish`. `commit drop "<id>"` and
  `DELETE /commits/{commit_id}` drop the snapshot and every snapshot built
  on it; `GET /commits?unattached=true&branch=<name>` and the CLI's
  `commit list --unattached` list a branch's live snapshots, newest first.
  Every snapshot of a chain expires when its branch head moves past the head
  the first write captured or the branch is deleted. Writing is a `change` on
  the recorded branch; a child accepts an omitted branch and refuses an
  explicit branch that differs from its base's target, including `main`.
  Listing and snapshot reads need `read` on that target. Missing ancestry
  prevents reads, extension and drop and retains the snapshot during cleanup.
  `CommitOutput` gains `kind` and `root_commit_id`; clients infer legacy
  receipts' kind from `published` when the kind field is absent. A graph born by this release is stamped
  internal schema v16; v14 and v15 graphs stay served and refuse
  `as unattached branch` until `omnigraph upgrade` stamps their live branches.
  GQT cases name a bound snapshot in `as unattached branch on "${name}"`.
