- `branch create` starts a branch at a past graph commit: `--from-commit` on
  the CLI, `from commit "<id>"` in the `branch create` statement (GQ language
  version 2.3) and `from_commit` on `POST /branches`. The new branch holds that
  commit's rows and history, so a fix or revert can be tried where a change was
  made and merged back with the commit as its base. A commit `cleanup` no
  longer retains (`410`), one written under an earlier schema or before the
  storage upgrade, one whose branch was deleted, an unpublished commit and an
  unattached snapshot (`409`) start no branch; the error's code and its new
  `branch_start_refusal` detail are named the same, and the detail names the
  reason. Every branch creation receipt now carries `from_commit_id`, the
  commit the new branch starts at, and the CLI prints it:
  `created branch <name> from <branch> at <commit-id>`. See
  [Start a branch at a commit][branch-at-commit].

[branch-at-commit]: ../docs/user/branching/index.md#start-a-branch-at-a-commit
