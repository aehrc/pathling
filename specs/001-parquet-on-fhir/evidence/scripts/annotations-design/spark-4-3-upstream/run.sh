#!/bin/bash
# Builds and runs CaseProbe on Spark 4.3.0. Usage: run.sh [-v] [-rowreader] [-aqe]
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
DATA=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/spark-4-3-upstream
"$HERE/build.sh"
JOPTS=(-Xmx6g -Djava.security.manager=allow
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
java "${JOPTS[@]}" -cp "$(cat "$(spark43_cp)"):$DATA/classes" spike.CaseProbe "$DATA" "$@"
