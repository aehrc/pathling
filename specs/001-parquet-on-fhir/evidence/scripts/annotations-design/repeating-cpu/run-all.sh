#!/bin/bash
# Runs every arm on both variants of the data, each under the machine's benchmark lock, and appends
# the RESULT lines to the given file. Usage: run-all.sh <data dir> <results file> <lock file>
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
for arm in bare B A today; do
  for variant in annotated plain; do
    lockf "$3" "$HERE/run-probe.sh" RepeatingCpuBench.java "$1" "$arm" "$variant" 2>&1 | grep RESULT >> "$2"
  done
done
