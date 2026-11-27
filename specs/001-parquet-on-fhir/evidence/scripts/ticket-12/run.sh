#!/bin/bash
# Runs a single-file Java probe against a classpath of third-party jars and Pathling jars, logging
# warnings only (logback.xml beside this script).
# Usage: run.sh <Probe.java> <file listing the third-party classpath> <Pathling jars, colon-separated> <args...>
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
MAIN=$1; CP="$(cat "$2"):$3"; shift 3
java -Xmx6g -Djava.security.manager=allow -Dlogback.configurationFile="$HERE/logback.xml" \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$CP" "$MAIN" "$@"
