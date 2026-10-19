#!/usr/bin/env bash
# Builds the spike 08 prototype jar against A0's runtime jar and PySpark's jars. No Maven, no ~/.m2 change.
# Usage: build.sh <runtime-jar> <pyspark-jars-dir> <out-jar>
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
RUNTIME="$1"; SPARK_JARS="$2"; OUT="$3"
CLASSES="$(mktemp -d)"
javac -nowarn -source 21 -target 21 -cp "$RUNTIME:$SPARK_JARS/*" -d "$CLASSES" $(find "$HERE/src" -name '*.java')
jar cf "$OUT" -C "$CLASSES" .
rm -rf "$CLASSES"
shasum -a 256 "$OUT"
