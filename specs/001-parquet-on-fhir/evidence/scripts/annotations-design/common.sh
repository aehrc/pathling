# Shared environment for the annotations-design probes. Source it from a bash script:
#   . "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
# It sets:
#   W   the root of the checkout, derived from the location of this file;
#   V   the Pathling version of the checkout, read from the root pom.xml (PATHLING_VERSION
#       overrides it);
#   J   the Pathling jars of the checkout, colon-separated. Build them first with
#       mvn package -pl library-runtime -am -DskipTests
# and defines two functions, each printing the path of a classpath file that it generates on
# first use under the gitignored .local/annotations-design directory:
#   thirdparty_cp   the third-party jars of library-api, for the Spark the checkout builds on
#                   (TP overrides the file);
#   spark43_cp      Spark 4.3.0 and its dependencies, from spark43-classpath.pom.xml.
W=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)
V=${PATHLING_VERSION:-$(sed -n 's:^    <version>\(.*\)</version>:\1:p' "$W/pom.xml" | head -1)}
J=""
for m in utilities fhir-schema encoders io terminology fhirpath library-api; do
  J="$J:$W/$m/target/$m-$V.jar"
done
J=${J#:}
CP_DIR=$W/.local/annotations-design

thirdparty_cp() {
  local f=${TP:-$CP_DIR/thirdparty-$V.cp}
  if [ ! -s "$f" ]; then
    mkdir -p "$(dirname "$f")"
    (cd "$W" && mvn -q -pl library-api -am dependency:build-classpath -Dmdep.outputFile="$f.raw") >&2
    # The Pathling modules come from target/ through J, not from the local repository.
    tr ':' '\n' < "$f.raw" | grep -v '/au/csiro/pathling/' | paste -sd: - > "$f"
    rm -f "$f.raw"
  fi
  echo "$f"
}

spark43_cp() {
  local f=$CP_DIR/spark43.cp
  if [ ! -s "$f" ]; then
    mkdir -p "$CP_DIR"
    mvn -q -f "$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/spark43-classpath.pom.xml" \
      dependency:build-classpath -Dmdep.outputFile="$f" >&2
  fi
  echo "$f"
}
