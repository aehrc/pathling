#!/bin/bash
# Rebuilds the data and reruns every case. Output goes to $DATA/output.txt as well as the terminal.
# Usage: run.sh [-rowreader]
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/env.sh"
mkdir -p "$DATA"
"$HERE/build.sh"
JOPTS=(-Xmx4g -Dlogback.configurationFile="$W/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/logback.xml"-Djava.security.manager=allow
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
java "${JOPTS[@]}" -cp "$(cat "$TP"):$DATA/classes" spike.D3aProbe "$DATA" "$@" 2>&1 \
  | tee "$DATA/raw.txt" | grep -E '^(@@|Exception)' | sed 's/^@@//' | tee "$DATA/output.txt"
