#!/bin/bash
# Runs JoinProbe, on both layouts, against the Pathling jars built in a checkout.
# Usage: run.sh <checkout> <file listing the third-party classpath> <classpath prefix, or ""> \
#   <NDJSON directory> <scratch directory> [trace]
# The prefix is where the classes compiled from probe-patch.diff go. Set JAVA_OPTS to
# -Dpathling.probe.variant=a, b or c to choose the patched variant.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
CHECKOUT=$1; CPFILE=$2; PREFIX=$3; DATA=$4; SCRATCH=$5; shift 5
JARS=""
for m in utilities fhir-schema encoders io terminology fhirpath library-api; do
  JARS="$JARS$(ls "$CHECKOUT"/$m/target/$m-*.jar | grep -v -e '-tests' -e '-sources' -e '-javadoc'):"
done
CP="${PREFIX:+$PREFIX:}$JARS$(cat "$CPFILE")"
java $JAVA_OPTS -Xmx4g -Djava.security.manager=allow --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$CP" "$HERE/JoinProbe.java" "$DATA" "$SCRATCH" "$@"
