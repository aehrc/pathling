#!/bin/bash
# Compiles the rule and the case probe against the Spark 4.3.0 classpath into $DATA/classes.
# The classpath comes from spark43-classpath.pom.xml, through spark43_cp in common.sh.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
DATA=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/spark-4-3-upstream
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
rm -rf "$DATA/classes" && mkdir -p "$DATA/classes"
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false \
  -classpath "$(cat "$(spark43_cp)")" -d "$DATA/classes" "$HERE"/*.scala
echo "built $DATA/classes"
