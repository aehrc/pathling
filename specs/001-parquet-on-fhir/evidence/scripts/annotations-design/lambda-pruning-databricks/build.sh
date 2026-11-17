#!/bin/bash
# Compiles the rule and the Databricks probes against Apache Spark 4.0.2 into one small jar. Spark
# and the Pathling jars are not bundled: they come from the cluster.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/env.sh"
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
OUT=$WORK/classes
rm -rf "$OUT" && mkdir -p "$OUT"
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false -release 17 \
  -classpath "$(cat "$TP"):$J" -d "$OUT" \
  "$OSS/CaseProbe.scala" "$HERE"/LambdaPruning.scala "$HERE"/DbxProbe.scala \
  $(ls "$HERE"/EngineViews.scala 2>/dev/null)
(cd "$OUT" && jar cf "$WORK/lpr.jar" .)
echo "built $WORK/lpr.jar"
