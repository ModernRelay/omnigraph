- The shared benchmark catalog no longer pins APFS and clonefile in its defaults:
  a scenario without an explicit environment takes the host's local default
  (APFS with clonefile on macOS, qualified XFS with plain-copy elsewhere), so one
  `benchmarks.yaml` runs on a workstation and on an XFS NVMe lab. The resolved
  backend stays part of the point identity.
- `branch-merge-d50-history64-xfs` (group `aws-xfs-history64`) measures the D50
  merge on a fixture aged by 64 reversible single-row update commits before the
  branches are created, declared for XFS like `branch-merge-d50-process-cold-xfs`.
  It is a new identity; no timing equivalence to the retired `branch-merge-v1`
  history recipe is claimed.
