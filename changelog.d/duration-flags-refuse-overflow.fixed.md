- `cleanup --older-than`, `optimize --max-gate-hold` and `--timeout` refuse a
  duration too long to represent; such a value could wrap to a shorter one.
