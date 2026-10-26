- **Maintenance has its own Cedar actions.** `maintain` covers optimize, index
  builds, the full-text rebuild and the repair report on the branch they
  maintain, and `reclaim` covers cleanup with no branch scope; `change` implies
  neither, so automation can compact a graph without the right to write data or
  delete history. An embedded host that installs a policy checker enforces them
  on every maintenance entry point, optimize and the index writers record the
  requesting actor on their commit, and existing policies grant neither action.
  See [policy actions][maintenance-policy-actions].

[maintenance-policy-actions]: ../docs/user/operations/policy.md#actions
