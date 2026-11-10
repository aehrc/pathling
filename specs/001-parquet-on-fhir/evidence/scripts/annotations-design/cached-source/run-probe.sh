#!/bin/bash
# Compiles every probe of this folder against the branch's jars and runs one of them. The probes are
# compiled rather than launched from source, so that the UDFs they define can be serialised.
# Usage: run-probe.sh <Probe> <args...>        (XMX sets the heap, default 12g)
set -e
source "$(dirname "$0")/env.sh"
HERE=$(cd "$(dirname "$0")" && pwd)
JARS=""
for m in utilities fhir-schema encoders io terminology fhirpath library-api; do
  JARS="$JARS$W/$m/target/$m-$V.jar:"
done
OUT=$SPIKE/classes
mkdir -p "$OUT"
MAIN=$1; shift
if [ "$HERE/$MAIN.java" -nt "$OUT/$MAIN.class" ] || [ -n "$(find "$HERE" -name '*.java' -newer "$OUT/$MAIN.class" 2>/dev/null)" ]; then
  javac -nowarn -proc:none -cp "$(cat "$THIRDPARTY_CP"):$JARS" -d "$OUT" "$HERE"/*.java
fi
exec java -Xmx${XMX:-12g} ${JOPTS:-} -Djava.security.manager=allow \
  -Dlogback.configurationFile="$W/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/logback.xml" \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$(cat "$THIRDPARTY_CP"):$JARS$OUT" "$MAIN" "$@"
