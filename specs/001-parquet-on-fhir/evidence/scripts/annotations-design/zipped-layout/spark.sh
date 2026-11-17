#!/bin/bash
# Compiles ZipProbe and runs the reads and the C10a SQL on one Spark version. Usage: spark.sh 4.3|4.0
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
V=$1
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
CLS=$OUT/classes-$V
mkdir -p "$CLS" "$OUT/logs"
CP=$(sparkcp "$V")
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false -classpath "$CP" -d "$CLS" "$HERE/ZipProbe.scala"
java "${JOPTS[@]}" -cp "$CP:$CLS" spike.ZipProbe reads "$DATA" "$HERE/cases.txt" > "$OUT/logs/reads-$V.raw" 2>&1
grep -E '^(ROW|Spark )' "$OUT/logs/reads-$V.raw" > "$OUT/logs/reads-$V.log"
java "${JOPTS[@]}" -cp "$CP:$CLS" spike.ZipProbe sql "$HERE/c10a.sql" > "$OUT/logs/c10a-$V.raw" 2>&1
grep -vE '^(WARNING|[0-9]{2}/[0-9]{2}/|Setting default|Using Spark|To adjust)' "$OUT/logs/c10a-$V.raw" > "$OUT/logs/c10a-$V.log" || true
wc -l "$OUT/logs/reads-$V.log"
