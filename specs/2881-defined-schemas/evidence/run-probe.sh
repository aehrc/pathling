#!/usr/bin/env bash
# Compiles and runs one of the Java probes in this directory against the io module of this
# checkout, for example:
#
#   specs/2881-defined-schemas/evidence/run-probe.sh stream-safety-probe.java StreamSafetyProbe
#
# The probes sit in the package au.csiro.pathling.io, so that they reach the package-private steps
# of a read, and they are loaded by the same class loader as the io classes for that reason.
# Build io first, from the root of the checkout (use mvn in place of the script in a main checkout):
#
#   scripts/mvn-isolated.sh install -pl io -am -DskipTests
set -euo pipefail

source_file=$1
main_class=$2
here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)
maven=mvn
[ -x "$root/scripts/mvn-isolated.sh" ] && [ -f "$root/.git" ] && maven="$root/scripts/mvn-isolated.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

(cd "$root" && "$maven" -q dependency:build-classpath -pl io -Dmdep.includeScope=test \
  -Dmdep.outputFile="$work/cp.txt")
classpath="$root/io/target/classes:$(cat "$work/cp.txt")"
mkdir "$work/classes"
# The test classpath carries a logback configuration that logs Spark at debug level to stdout.
echo '<configuration><root level="OFF"/></configuration>' > "$work/logback.xml"
javac -nowarn -d "$work/classes" -cp "$classpath" "$here/$source_file"
java -Xmx4g -Dlogback.configurationFile="$work/logback.xml" -Duser.timezone=UTC -Djava.security.manager=allow \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -cp "$work/classes:$classpath" "au.csiro.pathling.io.$main_class" 2> "$work/stderr.txt" || {
  cat "$work/stderr.txt" >&2
  exit 1
}
