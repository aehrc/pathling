#!/bin/bash
# Compiles the rule and the case probe with the Scala 2.13 compiler from ~/.m2 into $DATA/classes.
# Usage: build.sh
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/env.sh"
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
rm -rf "$DATA/classes" && mkdir -p "$DATA/classes"
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false \
  -classpath "$(cat "$TP"):$J" -d "$DATA/classes" "$HERE"/*.scala
# The A-raw models of the singular-meta-pruning spike, reused by ARawProbe.
javac -nowarn -cp "$(cat "$TP"):$J:$DATA/classes" -d "$DATA/classes" \
  "$HERE/../singular-meta-pruning/SingularMetaProbe.java" "$HERE/ARawProbe.java"
echo "built $DATA/classes"
