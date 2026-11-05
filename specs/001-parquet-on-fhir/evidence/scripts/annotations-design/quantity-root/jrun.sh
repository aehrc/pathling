#!/bin/bash
# Runs a compiled probe class against the worktree's Pathling jars.
# Usage: jrun.sh <Class> <args...>
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
CLASS=$1; shift
java "${JOPTS[@]}" -cp "$(cat "$TP"):$J:$DATA/classes" "$CLASS" "$@"
