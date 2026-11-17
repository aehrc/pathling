#!/bin/bash
# Runs the probe on one Spark version. Usage: run.sh 4.3|4.0 (after build.sh of the same version).
# Data: the patients are generated and encoded under $DATA, the edge set (null elements in name and
# address) is derived from them, and the singular-meta small data set is the one of the
# singular-meta-pruning spike, read only.
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh" "$1"
mkdir -p "$DATA/ndjson" "$DATA/data-$1" "$DATA/logs"
python3 "$HERE/gen_patients.py" "$DATA/ndjson/Patient.ndjson"
java "${JOPTS[@]}" -cp "$CP" ProbeMain "$DATA/data-$1" "$DATA/ndjson" \
  "$SP/singular-meta-pruning/small" > "$DATA/logs/probe-$1.raw" 2>&1
grep -E '^(ROW|DIFF|ERROR|Spark |Caused|Exception|java\.)' "$DATA/logs/probe-$1.raw" | tee "$DATA/logs/probe-$1.log"
