#!/bin/bash
# Prints the Spark reads tables for both versions and both metadata variants.
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
for v in 4.3 4.0; do
  for m in m1 m0; do
    echo "== Spark $v $m"
    python3 "$HERE/table.py" "$OUT/logs/reads-$v.log" $m "$@"
  done
done
