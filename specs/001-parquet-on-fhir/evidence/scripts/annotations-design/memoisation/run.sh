#!/bin/bash
# Compiles and runs MemoisationProbe against the built Pathling jars. It is compiled with javac rather
# than run as a single source file, because Spark cannot deserialise the probe's UDF lambdas from the
# source launcher's class loader.
# Usage: run.sh <count|time|trace> <wholeStage|nowholestage|interpreted> <len> [rows] [cacheSize] [contexts]
# Set MASTER to override local[4]. Data files go under the gitignored .local/work directory.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
JARS=""
for m in utilities fhir-schema encoders io terminology fhirpath library-api; do
  JARS="$JARS:$W/$m/target/$m-$V.jar"
done
CP="$(cat "$(thirdparty_cp)")$JARS"
BASE=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/memoisation
OUT=$BASE/classes
mkdir -p "$OUT"
if [ ! -f "$OUT/MemoisationProbe.class" ] || [ "$HERE/MemoisationProbe.java" -nt "$OUT/MemoisationProbe.class" ]; then
  javac -d "$OUT" -cp "$CP" "$HERE/MemoisationProbe.java"
fi
mode=$1; cg=$2; len=$3; shift 3
java -Xmx6g -Dmaster="${MASTER:-local[4]}" -Djava.security.manager=allow \
  -Dlogback.configurationFile="$W/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/logback.xml" \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$OUT:$CP" MemoisationProbe "$mode" "$BASE" "$cg" "$len" "$@"
