#!/bin/bash
# Runs the views on one Spark version over the shared input. Usage: run.sh 4.3|4.0
# Needs the rewrites-layer1 spike's build for that version (its classes provide the view
# definitions) and its data. The shared input is the 4.0 data set; see compare.sh.
set -e
MYDIR="$(cd "$(dirname "$0")" && pwd)"
. "$MYDIR/../rewrites-layer1/env.sh" "$1"
OUT=$SP/spark-4-3-compat/results
mkdir -p "$OUT" "$OUT/classes-$1"
javac -nowarn -cp "$CP" -d "$OUT/classes-$1" "$MYDIR/ResultsDump.java"
SHARED=$DATA/data-4.0
java "${JOPTS[@]}" -cp "$OUT/classes-$1:$CP" ResultsDump "$OUT/results-$1.txt" \
  "$SHARED/engine" "$SHARED/edge" "$SP/singular-meta-pruning/small" > "$OUT/run-$1.log" 2>&1
grep -E '^(Spark|Exception|Caused)' "$OUT/run-$1.log" || true
echo "wrote $OUT/results-$1.txt"
