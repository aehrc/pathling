#!/bin/bash
# Compiles QuantityCostProbe against a shaded benchmark jar and runs the variants given, each in a
# JVM of its own, over the Observations that run.sh writes (<work dir>/observations-<rows>).
# Usage: run-cost.sh <benchmark jar> <observations parquet> <runs> [variant ...]
# Variants: read, old-struct, code-only, both, both-narrow, code+conv, code+key, code+code,
#   noop-conv-x1, noop-conv-x2, noop-key-x2, replica-row-code, replica-row-both,
#   replica-row-both-decimal, replica-row-code+conv, replica-row-code+const-value,
#   replica-row-code+cache, replica-args-code, replica-args-both, bound-hash-both,
#   const-code, const-code+conv, const-code+string, const-code+decimal, const-both.
# With no variant given, all are run. Note the build each table was measured at
# (evidence/ticket-42-memoisation.md): the variants that call the kind measure the build's kind.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
JAR=$1; DATA=$2; RUNS=$3; shift 3
VARIANTS=${*:-read old-struct code-only both both-narrow code+conv code+key code+code noop-conv-x1 noop-conv-x2 noop-key-x2 replica-row-code replica-row-both replica-row-both-decimal replica-row-code+conv replica-row-code+const-value replica-row-code+cache replica-args-code replica-args-both bound-hash-both const-code const-code+conv const-code+string const-code+decimal const-both}
OUT=$(mktemp -d)
javac -proc:none -d "$OUT" -cp "$JAR" "$HERE/QuantityCostProbe.java"
for v in $VARIANTS; do
  java -Donly="$v" -Xmx8g -Duser.timezone=UTC \
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    -cp "$OUT:$JAR" QuantityCostProbe "$DATA" "$RUNS" | grep RESULT
done
