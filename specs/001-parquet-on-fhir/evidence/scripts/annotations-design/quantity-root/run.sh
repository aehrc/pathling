#!/bin/bash
# Reproduces annotations-design-quantity-root.md. Needs the built Pathling jars of the worktree and
# the pushdown evidence's sorted Patient table (annotations-design/pushdown, work_big, written by
# PushdownPrepare from gen_patients.py with 1,200,000 patients and parquet.block.size=262144).
# Output of each part goes to $DATA/part*.out. Nothing here is timed, so it needs no bench lock.
# Usage: run.sh [prepare|a|a3|b|c|all]
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/env.sh"
WHAT=${1:-all}
mkdir -p "$DATA/ndjson"
"$HERE/build.sh"
if [ "$WHAT" = prepare ] || [ "$WHAT" = all ]; then
  [ -f "$DATA/ndjson/Observation.ndjson" ] || python3 "$HERE/gen_observations.py" "$DATA/ndjson" 200000
  "$HERE/jrun.sh" Prepare "$DATA" "$PUSHDOWN_DATA/work_big" 2>&1 | grep -v "^\s*at \|WARN" > "$DATA/prepare.out"
fi
for part in A:a A3:a3 B:b C:c; do
  cls=${part%%:*}; key=${part##*:}
  if [ "$WHAT" = "$key" ] || [ "$WHAT" = all ]; then
    case $cls in
      C) "$HERE/jrun.sh" PartC "$PUSHDOWN_DATA/work_big" "$DATA" ;;
      *) "$HERE/jrun.sh" Part$cls "$DATA" ;;
    esac 2>&1 | grep -v "^\s*at \|WARN" > "$DATA/part$cls.out"
  fi
done
echo "outputs in $DATA/*.out"
