#!/bin/bash
# Runs CatalogAppendProbe against the Spark and Delta jars the repository builds with.
# Usage (from the repository root):
#   run.sh <scratch directory> <in-memory|hive>
# Needs only the third-party classpath of library-api, which is resolved offline from ~/.m2.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
SCRATCH=$1; CATALOG=${2:-in-memory}
mkdir -p "$SCRATCH"
[ -f "$SCRATCH/cp.txt" ] || mvn -q -o -pl library-api dependency:build-classpath \
  -Dmdep.outputFile="$SCRATCH/cp.txt" -DincludeScope=test
java -Xmx4g -Djava.security.manager=allow --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$(cat "$SCRATCH/cp.txt")" "$HERE/CatalogAppendProbe.java" "$SCRATCH" "$CATALOG"
