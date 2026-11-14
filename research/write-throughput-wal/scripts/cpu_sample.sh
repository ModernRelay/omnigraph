#!/usr/bin/env bash
# Print the cores the running write_throughput binary used over $2 seconds after $1 seconds.
sleep "${1:-8}"
pid=$(for p in /proc/[0-9]*; do [ "$(cat $p/comm 2>/dev/null)" = "write_throughpu" ] && basename $p; done | head -1)
[ -z "$pid" ] && { echo "no process"; exit 0; }
t0=$(awk '{print $14+$15}' /proc/$pid/stat); sleep "${2:-5}"; t1=$(awk '{print $14+$15}' /proc/$pid/stat)
python3 -c "print('cpu cores busy:', ($t1-$t0)/100/${2:-5})"
