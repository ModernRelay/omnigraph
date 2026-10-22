- `omnigraph studio --target NAME [--graph G] [--json]` opens OmniGraph Studio
  on a configured server target through an `omnigraph-studio://open` link,
  prints the link and exits; it sends no request and reads no credential.
  Direct and managed targets refuse, and where nothing on the platform opens
  Studio's links it prints the link and refuses with `studio_unavailable`
  instead of opening a browser. `target inspect --json` now reports the `home`
  and, for a server target, `binding` digests the link carries, and
  `version --json` reports `protocols.studio: 1`. See [Open Studio][cli-studio].

[cli-studio]: ../docs/user/cli/studio.md
