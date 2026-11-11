- Update `rustix` to 1.1.6 for [RUSTSEC-2026-0486](https://rustsec.org/advisories/RUSTSEC-2026-0486):
  its extended-attribute functions were unsound for a zero-length buffer.
  Omnigraph does not call them.
