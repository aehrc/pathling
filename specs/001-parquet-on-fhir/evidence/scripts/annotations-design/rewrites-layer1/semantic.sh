#!/bin/bash
# Compares the engine's exists(c) and all(c) with candidate SQL forms on the edge data set.
# Usage: semantic.sh 4.3|4.0 (after build.sh and run.sh of the same version, which write the edge set).
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh" "$1"
java "${JOPTS[@]}" -cp "$CP" SemanticProbe "$DATA/data-$1/edge" > "$DATA/logs/semantic-$1.raw" 2>&1
grep -E '^(SEM|Caused|Exception|java\.)' "$DATA/logs/semantic-$1.raw" | tee "$DATA/logs/semantic-$1.log"
