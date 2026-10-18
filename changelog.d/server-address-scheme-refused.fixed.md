- A `--server` value or configured server `url:` whose scheme is not `http`,
  `https` or `unix` now fails as an invalid server address. It was resolved as
  graph storage and failed with an unrelated storage error such as
  `unsupported storage URI scheme`.
