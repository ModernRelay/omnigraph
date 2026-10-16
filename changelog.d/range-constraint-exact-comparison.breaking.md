- A graph whose schema declares `@range` on a list property now refuses a
  load, mutation or merge that writes a list element outside the range,
  with `@range violation`; the declaration was not enforced before. Rows
  already stored are left as they are. Writers that relied on the unchecked
  declaration must keep elements inside the range: schema migration refuses
  removing a constraint from a property that stays.
