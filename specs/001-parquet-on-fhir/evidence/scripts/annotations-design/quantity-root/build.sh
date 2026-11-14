#!/bin/bash
# Compiles the lambda-pruning rule (from its own directory, unmodified) and this directory's Scala.
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
rm -rf "$DATA/classes" && mkdir -p "$DATA/classes"
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false \
  -classpath "$(cat "$TP"):$J" -d "$DATA/classes" \
  "$SCRIPTS/lambda-pruning-rule/LambdaPruning.scala" "$HERE"/*.scala
echo "built $DATA/classes"
