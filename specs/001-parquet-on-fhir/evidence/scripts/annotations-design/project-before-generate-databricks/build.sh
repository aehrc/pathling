#!/bin/bash
# Compiles PbgRewrite and PbgViews against Spark 4.0.2 and the Databricks probe jar (lpr.jar), into
# pbg.jar. Nothing is bundled: Spark, Pathling and lpr.jar come from the cluster.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/../lambda-pruning-databricks/env.sh"
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
OUT=$WORK/pbg-classes
rm -rf "$OUT" && mkdir -p "$OUT"
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false -release 17 \
  -classpath "$(cat "$TP"):$J:$WORK/lpr.jar" -d "$OUT" "$HERE"/PbgRewrite.scala "$HERE"/PbgViews.scala
(cd "$OUT" && jar cf "$WORK/pbg.jar" .)
echo "built $WORK/pbg.jar"
