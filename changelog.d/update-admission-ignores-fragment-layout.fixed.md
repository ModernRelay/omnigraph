- An update's `write_max_bytes` charge now counts only the rows it matches,
  not the rows stored beside them in the same fragment, which could refuse it.
