#!/bin/bash
# Steps: (1) compares the encoded inputs of data-4.3 and data-4.0 on 4.3, (2) runs the views on
# both versions over the shared 4.0 input, (3) diffs the per-view output.
set -e
MYDIR="$(cd "$(dirname "$0")" && pwd)"
. "$MYDIR/../rewrites-layer1/env.sh" 4.3
OUT=$SP/spark-4-3-compat/results
mkdir -p "$OUT" "$OUT/classes-4.3"
javac -nowarn -cp "$CP" -d "$OUT/classes-4.3" "$MYDIR/ResultsDump.java"
java "${JOPTS[@]}" -cp "$OUT/classes-4.3:$CP" ResultsDump inputs "$DATA/data-4.3" "$DATA/data-4.0" \
  > "$OUT/inputs.raw" 2>&1
grep -E '^(Spark|INPUT)' "$OUT/inputs.raw" | tee "$OUT/inputs.txt"
bash "$MYDIR/run.sh" 4.0
bash "$MYDIR/run.sh" 4.3
python3 "$MYDIR/compare.py" "$OUT/results-4.0.txt" "$OUT/results-4.3.txt" | tee "$OUT/comparison.txt"
