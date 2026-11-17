#!/bin/bash
# Reproduces the cached-source measurement. Stages: data (generate NDJSON and write Parquet),
# bench (every arm, each in its own JVM, under the shared benchmark lock).
# Usage: run.sh data | bench [results file] | one <source> <variant> <arm> <exprs>
# N_PAT and N_ENC set the row counts (default 1,000,000 Patients and 500,000 Encounters).
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/env.sh"
mkdir -p "$SPIKE/logs"
export JOPTS="-Dpq=$SPIKE/pq -Dnd=$SPIKE/nd"
case "$1" in
  data)
    python3 "$HERE/gen_ndjson.py" "$SPIKE/nd" "${N_PAT:-1000000}" "${N_ENC:-500000}"
    "$HERE/run-probe.sh" Prep "$SPIKE/nd" "$SPIKE/pq" 2>&1 | tee "$SPIKE/logs/prep.log" | grep WROTE
    ;;
  bench)
    OUT=${2:-$SPIKE/results.txt}
    for source in ${SOURCES:-cache json}; do
      for variant in plain ext; do
        for arm in bare A0 A1 L1 D3a; do
          groups="E1,E2 E3"
          [ "$arm" = A1 ] && groups="E1,E2"
          [ "$arm" = D3a ] && groups="E3"
          for g in $groups; do
            lockf "$LOCK" "$HERE/run-probe.sh" CachedSourceBench "$source" "$variant" "$arm" "$g" 2>&1 \
              | tee -a "$SPIKE/logs/bench.log" | grep -E '^(CACHE|RESULT)' >> "$OUT"
          done
        done
      done
    done
    ;;
  one)
    shift
    lockf "$LOCK" "$HERE/run-probe.sh" CachedSourceBench "$@"
    ;;
  *)
    echo "usage: run.sh data | bench [results file]" >&2
    exit 1
    ;;
esac
