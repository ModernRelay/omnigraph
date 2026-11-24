- Update `url` to 2.5.9 for [RUSTSEC-2026-0592](https://rustsec.org/advisories/RUSTSEC-2026-0592):
  `Url::origin()` overflowed the stack on a deeply nested `blob:` URL.
  Omnigraph calls it only on its own configured URLs, never on request input.
