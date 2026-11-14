#!/bin/bash
# Runs the probe on one Spark version. Usage: run.sh 4.3|4.0 (after build.sh of the same version).
# Data: the patients are generated and encoded under $DATA; the singular-meta small data set is
# the one of the singular-meta-pruning spike, read only.
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh" "$1"
mkdir -p "$DATA/ndjson" "$DATA/data-$1" "$DATA/logs"
python3 "$HERE/gen_patients.py" "$DATA/ndjson/Patient.ndjson"
java "${JOPTS[@]}" -cp "$CP" ProbeMain "$DATA/data-$1" "$DATA/ndjson" \
  "$SP/singular-meta-pruning/small" 2>&1 | grep -E '^(\||Spark |.*optimised generates)' | tee "$DATA/logs/probe-$1.log"
