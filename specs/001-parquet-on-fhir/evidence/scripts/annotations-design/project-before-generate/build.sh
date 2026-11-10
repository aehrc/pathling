#!/bin/bash
# Compiles the Scala helpers and the Java probes for one Spark version. Usage: build.sh 4.3|4.0
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh" "$1"
SC=~/.m2/repository/org/scala-lang
SCALAC_CP="$SC/scala-compiler/2.13.16/scala-compiler-2.13.16.jar:$SC/scala-reflect/2.13.16/scala-reflect-2.13.16.jar:$SC/scala-library/2.13.16/scala-library-2.13.16.jar"
rm -rf "$CLASSES" && mkdir -p "$CLASSES"
java -cp "$SCALAC_CP" scala.tools.nsc.Main -nowarn -usejavacp:false \
  -classpath "$SPARKCP:$J" -d "$CLASSES" "$HERE"/*.scala
javac -nowarn -cp "$CP" -d "$CLASSES" "$HERE"/*.java
echo "built $CLASSES"
