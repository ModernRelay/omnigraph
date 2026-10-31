- Reading a commit that only a branch holds no longer scans every live branch
  each time. This covers `commit show`, `commit changes`, a diff side spelled
  as a commit id and a read pinned to that commit. A graph handle remembers
  which branch held a commit's history block and reads the next commit of
  that block from that branch alone, so stepping through a branch's commits
  costs one branch read per commit instead of one per live branch.
