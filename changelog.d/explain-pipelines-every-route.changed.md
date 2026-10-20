- Every planner explain document carries `pipelines`, and the document is
  `explain_version` 7: the physical plan is cut into pipelines at every node
  whose operator emits nothing before it has seen its whole input (`Sort`,
  `Aggregate`, `AntiJoin`, `RankFuse`, a load's `Validate`, a costed
  multi-hop `Expand`) as well as at a join's build side (a hash join's build,
  the collected left side of a cross join or a contains join), so a query's
  and a load's explain print the same decomposition a diff's or merge's does.
  A dependent scan prints as `Scan(input_ids)`. The
  engine refuses to lower a plan whose operator declares a different emission
  than the plan's cut, so the printed pipelines are the executed ones. A GQT
  load step's `--- expect plan` section accepts `load pipeline <n>: …` lines.
