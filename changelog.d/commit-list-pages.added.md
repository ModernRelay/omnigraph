- **Page through a branch's commit history.** `GET /commits` takes `limit`
  (1 to 1,000) and an opaque `page_token`, and `omnigraph commit list` takes
  `--limit` and `--page-token`; the MCP `commits_list` tool takes both too. A
  page returns `head_commit_id`, the head its listing resolved, and a
  `next_page_token` while older commits remain; later pages answer for that
  head even after the branch moves. A page reads only the history it returns,
  so its cost no longer grows with the branch's history; under a policy, a
  continuation by a caller whose `read` does not cover every branch is checked
  against the branch's history and costs what the whole listing costs. Without either
  parameter the whole history is listed as before. See
  [commits][commit-list-pages-guide].

[commit-list-pages-guide]: ../docs/user/branching/index.md#commits
