#!/bin/bash
# Compiles the probe with the Scala 2.13 compiler from ~/.m2 into $DATA/classes.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/env.sh"
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
rm -rf "$DATA/classes" && mkdir -p "$DATA/classes"
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false \
  -classpath "$(cat "$TP")" -d "$DATA/classes" "$HERE"/*.scala
echo "built $DATA/classes"
