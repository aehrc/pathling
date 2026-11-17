#!/bin/bash
# Compiles and runs UniquenessProbe on local Spark 4.0.2 against the built fhir-schema classes.
# The fhir-schema classpath file comes from:
#   mvn -o -q dependency:build-classpath -pl fhir-schema -Dmdep.outputFile=<file>
# Usage: run.sh <thirdparty classpath file> <fhir-schema classpath file> <output dir>
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
W=$(cd "$HERE/../../../../../.." && pwd)
TP=$1
FS_CP=$2
OUT=$3
mkdir -p "$OUT/classes"
CP="$(cat "$TP"):$(cat "$FS_CP"):$W/fhir-schema/target/classes"
javac -nowarn -d "$OUT/classes" -cp "$CP" "$HERE/UniquenessProbe.java"
java -Xmx2g -Djava.security.manager=allow \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$CP:$OUT/classes" UniquenessProbe "$OUT" 2>&1 | grep -e '^RESULT'
