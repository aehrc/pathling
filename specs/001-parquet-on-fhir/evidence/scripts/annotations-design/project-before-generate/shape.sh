#!/bin/bash
# Prints the plans of one view. Usage: shape.sh 4.3|4.0 [viewJson]. Reads the stored PoF data that
# the engine probe of the spark-4-3-upstream spike wrote (read only).
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh" "$1"
shift
java "${JOPTS[@]}" -cp "$CP" PlanShape "$SP/spark-4-3-upstream" "$@"
