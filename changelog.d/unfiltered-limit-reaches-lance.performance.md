- A `limit` over an unfiltered node scan now reaches Lance, so
  `match { $p: Person } return { $p.name } limit 10` reads only the fragments
  that hold those rows.
