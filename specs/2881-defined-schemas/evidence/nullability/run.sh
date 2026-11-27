#!/bin/sh
# Usage: ./run.sh [all|q1|q2|q2s|q3|q4|q5] > out.txt
# Build first: mvn -o dependency:build-classpath -Dmdep.outputFile=cp.txt && mvn -o compile
cd "$(dirname "$0")"
exec java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -cp "target/classes:$(cat cp.txt)" spike.NullabilitySpike "$@"
