#!/bin/bash
# Runs BaseJoinProbe, on the previous layout only, against the Pathling jars built in a
# checkout of the base commit, whose engine reads no other layout. The probe does not use io.
# Usage: run-base.sh <base checkout> <file listing the third-party classpath> \
#   <NDJSON directory> <scratch directory> [trace]
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
CHECKOUT=$1; CPFILE=$2; DATA=$3; SCRATCH=$4; shift 4
JARS=""
for m in utilities fhir-schema encoders terminology fhirpath library-api; do
  JARS="$JARS$(ls "$CHECKOUT"/$m/target/$m-*.jar | grep -v -e '-tests' -e '-sources' -e '-javadoc'):"
done
CP="$JARS$(cat "$CPFILE")"
java $JAVA_OPTS -Xmx4g -Djava.security.manager=allow --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$CP" "$HERE/BaseJoinProbe.java" "$DATA" "$SCRATCH" "$@"
