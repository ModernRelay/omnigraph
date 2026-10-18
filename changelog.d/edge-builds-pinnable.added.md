- Edge builds are now made once a day from the tip of `main` once its whole
  CI run passes, and each is published as its own prerelease,
  `edge-<commit date>-<commit>`, whose archives never change, so a deployment
  can pin it with `VERSION=edge-…` (`-Version` on Windows) and return to it
  later. The 30 newest are kept, and so is the newest build of each storage
  format, which can still export a graph written at that format. The rolling
  `edge` release still holds the newest build.
  Each edge release lists the storage format and HTTP contract the build uses
  next to the latest stable release's, and says when a stable binary could no
  longer open a branch the build writes to. See
  [release channels][edge-builds-pinnable].

[edge-builds-pinnable]: ../docs/user/install.md#release-channels
