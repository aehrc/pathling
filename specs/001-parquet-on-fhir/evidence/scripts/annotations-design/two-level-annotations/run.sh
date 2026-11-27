#!/bin/bash
# Builds TwoLevelProbe against the Spark 4.0.2 classpath and runs it on 4.0 or 4.3.
# Usage: run.sh <4.0|4.3> [-rowreader]
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
SP=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes
DATA=$SP/two-level-annotations
TP=$(thirdparty_cp)
V=$1; shift
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
mkdir -p "$DATA/classes" "$DATA/data-$V"
if [ ! -f "$DATA/classes/spike/TwoLevelProbe.class" ]; then
  java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false -classpath "$(cat "$TP")" -d "$DATA/classes" "$HERE/TwoLevelProbe.scala"
fi
if [ "$V" = "4.3" ]; then
  TP2=$(tr ':' '\n' < "$TP" | grep -v 'org/apache/spark/' | paste -sd: -)
  CP="$(cat "$(spark43_cp)"):$TP2"
else
  CP="$(cat "$TP")"
fi
JOPTS=(-Xmx6g -Djava.security.manager=allow
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
java "${JOPTS[@]}" -cp "$CP:$DATA/classes" spike.TwoLevelProbe "$DATA/data-$V" "$@"
