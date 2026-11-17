#!/bin/bash
# Compiles EngineAnnotationProbe against a shaded benchmark jar and runs it, so that the same probe times
# a build before ticket 42 and one after it on the same data.
# Usage: run.sh <benchmark jar> <work dir> [rows] [runs]
# Build the jar with: mvn package -pl benchmark -am -DskipTests
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
JAR=$1
WORK=$2
ROWS=${3:-1000000}
RUNS=${4:-5}
OUT=$WORK/classes-$(basename "$JAR" .jar)
mkdir -p "$OUT"
javac -d "$OUT" -cp "$JAR" "$HERE/EngineAnnotationProbe.java"
java -Xmx8g -Dmaster="${MASTER:-local[4]}" -Duser.timezone=UTC \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$OUT:$JAR" EngineAnnotationProbe "$WORK" "$ROWS" "$RUNS"
