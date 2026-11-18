- The plan budget now bounds all compile and planning work, not term nodes alone, and defaults to 32768. A read that nests or chains many `optional`s or blocks can now pass it (`T70`); declare `@budget(plan = n)` up to `plan_budget_limit`, or split the read. See the [traversal guide][plan-budget-work-guide].

[plan-budget-work-guide]: ../docs/user/queries/traversal.md
