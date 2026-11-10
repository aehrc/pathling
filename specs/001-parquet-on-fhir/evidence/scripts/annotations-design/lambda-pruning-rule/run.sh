#!/bin/bash
# Builds the rule and runs the probes. Data and classes go to the spike's directory under .local.
# Usage: run.sh [cases|inject|araw|engine|all] [-v]
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/env.sh"
mkdir -p "$DATA"
"$HERE/build.sh"
WHAT=${1:-all}; shift || true
JOPTS=(-Xmx6g -Djava.security.manager=allow
  -Dlogback.configurationFile="$W/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/logback.xml"
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
CP="$(cat "$TP"):$J:$DATA/classes"
if [ "$WHAT" = cases ] || [ "$WHAT" = all ]; then
  java "${JOPTS[@]}" -cp "$CP" spike.CaseProbe "$DATA" "$@"
fi
if [ "$WHAT" = inject ] || [ "$WHAT" = all ]; then
  for m in extra operator precbo builder-existing delta delta-colmap; do
    java "${JOPTS[@]}" -cp "$CP" spike.InjectionProbe "$DATA" "$m"
  done
fi
if [ "$WHAT" = araw ] || [ "$WHAT" = all ]; then
  java "${JOPTS[@]}" -cp "$CP" ARawProbe \
    "$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/singular-meta-pruning/small" "$@"
fi
if [ "$WHAT" = engine ] || [ "$WHAT" = all ]; then
  mkdir -p "$DATA/ndjson"
  python3 "$HERE/gen_patients.py" "$DATA/ndjson/Patient.ndjson"
  java "${JOPTS[@]}" -cp "$CP" "$HERE/EngineProbe.java" "$DATA" "$DATA/ndjson" \
    "$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/singular-meta-pruning/small" "$@"
fi
