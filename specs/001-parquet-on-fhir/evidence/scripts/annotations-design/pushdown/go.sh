#!/bin/bash
# Runs a probe of this directory against the built Pathling jars of the worktree.
# Usage: go.sh <Probe.java> <args...>   (THIRDPARTY_CP overrides the third-party classpath file.)
set -e
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
ROOT=$W
CPFILE=${THIRDPARTY_CP:-$(thirdparty_cp)}
JARS=""
for m in utilities fhir-schema encoders io terminology fhirpath library-api; do
  JARS="$JARS:$ROOT/$m/target/$m-$V.jar"
done
PROBE=$1; shift
"$ROOT/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/run.sh" \
  "$ROOT/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/pushdown/$PROBE" \
  "$CPFILE" "${JARS#:}" "$@"
