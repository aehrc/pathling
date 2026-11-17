#!/bin/bash
# Runs the probe on Spark 4.0.2 and 4.3.0 and keeps the result lines (the logs are very verbose).
# Usage: all.sh <output dir>
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=$1
mkdir -p "$OUT"
for v in 4.0 4.3; do
  bash "$HERE/run.sh" $v > "$OUT/raw-$v.log" 2>&1
  grep -E '^(\||###|\(|  p[0-9]|File leaves|Spark |Exception|java\.lang\.)' "$OUT/raw-$v.log" > "$OUT/result-$v.txt"
done
