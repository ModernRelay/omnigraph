- Unpublished graph commits (RFC "Unpublished graph commits"): a mutation
  declaration or a `branch merge` statement may end in `as commit`, which
  writes the complete result as an unpublished commit, readable by its graph
  commit id (`published: false` on the receipt's `commit`) and invisible to
  every branch read, until `commit publish "<id>"` installs that exact result
  on its recorded target branch or `commit drop "<id>"` removes it. Both are
  control writes through `POST /mutate` and have routes of their own,
  `POST /commits/{commit_id}/publish` (the published commit) and
  `DELETE /commits/{commit_id}` (204); `GET /commits?unpublished=true` lists
  a branch's unpublished commits, `POST /branches/merge` takes `as_commit`
  (exclusive with `delete_branch`), and the CLI gains `commit publish <id>`,
  `commit drop <id>`, `commit list --unpublished` and `branch merge
  --as-commit`. `CommitOutput` gains `published`.
  Two Cedar actions, `commit_publish` and `commit_drop`, gate the control
  writes by the recorded target (`target_branch_scope`). A graph born by this
  release is stamped internal schema v15; v14 graphs stay served and refuse
  `as commit` until `omnigraph upgrade` stamps their live branches. GQT cases
  bind a write's commit id with `--- bind commit $name`, read it with
  `--- query snapshot: $name`, and name it in `commit publish "${name}"` /
  `commit drop "${name}"`.
