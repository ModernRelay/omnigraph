- A keyed `insert` and a `merge` load keep the properties they omit, Blob and
  vector included, instead of nulling them; an explicit `null` clears one.
