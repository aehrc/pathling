#!/bin/bash
# Compiles and runs a probe of this folder against the branch's jars. The probe is compiled rather
# than launched from source, so that the UDFs it defines can be serialised to Spark's tasks.
# Usage: run-probe.sh <Probe.java> <args...>
# THIRDPARTY_CP overrides the file listing the third-party classpath (Spark and its closure).
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
ROOT=$W
THIRDPARTY_CP=${THIRDPARTY_CP:-$(thirdparty_cp)}
JARS=""
for m in utilities fhir-schema encoders io terminology fhirpath library-api; do
  JARS="$JARS$ROOT/$m/target/$m-$V.jar:"
done
OUT=${CLASSES_DIR:-$ROOT/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/repeating-cpu/classes}
mkdir -p "$OUT"
MAIN=$1; shift
javac -nowarn -proc:none -cp "$(cat "$THIRDPARTY_CP"):$JARS" -d "$OUT" "$HERE/$MAIN"
exec "$ROOT/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/run.sh" "${MAIN%.java}" "$THIRDPARTY_CP" "$JARS$OUT" "$@"
