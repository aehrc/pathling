#!/bin/bash
# One attempt at EngineProbe on the Spark 4.3.0 classpath. The Pathling jars are built against
# Spark 4.0.2, so the 4.0.2 jars are dropped from the third-party classpath and 4.3.0 goes first.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
DATA=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/spark-4-3-upstream
TP=$(thirdparty_cp)
TP2=$(tr ':' '\n' < "$TP" | grep -v 'org/apache/spark/' | paste -sd: -)
CP="$(cat "$(spark43_cp)"):$TP2:$J:$DATA/classes"
JOPTS=(-Xmx6g -Djava.security.manager=allow
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
mkdir -p "$DATA/ndjson"
python3 "$HERE/gen_patients.py" "$DATA/ndjson/Patient.ndjson"
java "${JOPTS[@]}" -cp "$CP" "$HERE/EngineProbe.java" "$DATA" "$DATA/ndjson" \
  "$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/singular-meta-pruning/small" "$@"
