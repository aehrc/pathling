#!/usr/bin/env bash
# Spike 08 phase 2: the whole size-l sequence, unattended. See RUNBOOK.md beside this file.
#
#   1. Pre-flight: the A0 runtime jar's hash, the harness scripts against the committed copies, the
#      prototype jar built from the committed sources, and the machine's state.
#   2. Noise check: two fresh A0 labels, then p2_spread.py. If the replicate spread is too wide, stop here.
#   3. The candidates, each one label holding two replicates (extensions off and on, the same code):
#      A+ and B on JSON and the three XML sources, A-variant on JSON.
#   4. The messy JSON variant (one shape error per file), under the drop policy: A0, A+, A-variant, B.
#   5. A closing fresh A0 label, to bracket the candidates.
# Labels are <PREFIX>-* (p2 by default). A label whose conditions-end.txt exists is complete and is skipped.
# With RESUME=1 an interrupted label is continued (run-phases.sh skips its finished steps); without it the
# script stops. An A0 label that fails stops the batch; a candidate or messy label that fails is recorded in
# failed-labels.txt and the batch goes on, so one candidate cannot cost the others their measurement.
# For a dry run: SIZE=s PREFIX=dry IGNORE_SPREAD=1, with RESOURCES_OVERRIDE and EXTS_OVERRIDE to shorten it.
# Runs on macOS's bash 3.2.
set -uo pipefail
SIZE="${SIZE:-l}"
PREFIX="${PREFIX:-p2}"

W="$(cd "$(dirname "$0")/../../../../.." && pwd)"                     # The spike worktree.
P2="$W/specs/001-parquet-on-fhir/evidence/spike-08/phase2"
H=/Users/szu004/dev/pathling/.local/work/issue/2367/pathling-compare   # The shared harness.
WT=/Users/szu004/dev/pathling/.claude/worktrees
RUNTIME=$HOME/.m2/repository/au/csiro/pathling/library-runtime/10.0.0-SNAPSHOT/library-runtime-10.0.0-SNAPSHOT.jar
RUNTIME_SHA=3eb0a768fa6c27052d6eaa66f8499c30558aeab9b14d125277fd7571d54f2320
VENV=$H/venv-514f8f8
JAR=$H/tools/spike08-proto.jar
LOG=$W/.local/work/issue/2831-spike-08/batch-$PREFIX
mkdir -p "$LOG"

say() { echo "=== $(date '+%F %T') $*"; }
conditions() {
  echo "date: $(date)"; echo "uptime: $(uptime)"; echo "swap: $(sysctl -n vm.swapusage)"
  echo "disk: $(df -g /System/Volumes/Data | tail -1)"; echo "java processes:"; pgrep -fl java | cut -c1-200 || echo "  none"
  echo "top cpu:"; ps -Ao pcpu,comm -r | sed -n 2,6p
}
quiet() {
  # Waits up to QUIET_MINUTES (15) for the one-minute load to fall under 3, then goes on either way.
  local i n=$(( ${QUIET_MINUTES:-15} * 2 ))
  [ "$n" -gt 0 ] || return 0
  for i in $(seq 1 "$n"); do
    local load; load=$(sysctl -n vm.loadavg | awk '{print $2}')
    awk -v l="$load" 'BEGIN{exit !(l < 3)}' && return 0
    sleep 30
  done
  say "load still $(sysctl -n vm.loadavg) after ${QUIET_MINUTES:-15} minutes; going on"
}
preflight_label() {
  local free; free=$(df -g /System/Volumes/Data | tail -1 | awk '{print $4}')
  if [ "$free" -lt 15 ]; then say "STOP: ${free} GiB free, under 15"; exit 2; fi
  quiet
}
label() {
  # label <name> <route> [env assignments...]
  local name=$PREFIX-$1 route=$2; shift 2
  local out="$H/results/$name"
  if [ -e "$out/conditions-end.txt" ]; then say "skip $name (complete)"; return 0; fi
  if [ -e "$out" ] && [ "${RESUME:-}" != 1 ]; then say "STOP: $out exists and is incomplete; rerun with RESUME=1"; exit 3; fi
  preflight_label
  say "start $name ($route $*)"
  conditions > "$LOG/$name.conditions-before.txt"
  # The prototype jar goes on the classpath of the candidates only, so A0 runs exactly as in phase 1.
  local jars=""; [ "$route" = pathling ] || jars="$JAR"
  env ${@+"$@"} EXTRA_JARS="$jars" PATHLING_SRC="$WT/bench-514f8f8" RESUME="${RESUME:-}" \
    "$H/run-phases.sh" "$name" "$VENV" "$route" "$SIZE" > "$LOG/$name.log" 2>&1
  local status=$?
  say "end $name (exit $status)"
  { echo "prototype jar: $(shasum -a 256 "$JAR")"; echo "env: $*"; echo "exit: $status"; } > "$out/p2-notes.txt"
  if [ $status -ne 0 ]; then
    if [ "$route" = pathling ] && [ -z "$*" ]; then say "STOP: $name failed; see $LOG/$name.log"; exit 4; fi
    say "FAILED: $name (exit $status), going on; see $LOG/$name.log"
    echo "$name exit $status" >> "$LOG/failed-labels.txt"
  fi
}

# ---------------------------------------------------------------- 1. pre-flight
say "pre-flight"
conditions | tee "$LOG/preflight.txt"
[ "$(shasum -a 256 "$RUNTIME" | cut -d' ' -f1)" = "$RUNTIME_SHA" ] || { say "STOP: the A0 runtime jar in ~/.m2 is not 514f8f8's ($RUNTIME_SHA); rebuild it in $WT/bench-514f8f8 (see RUNBOOK.md)"; exit 5; }
if pgrep -fl java >/dev/null && [ "${ALLOW_JAVA:-}" != 1 ]; then say "STOP: a Java process is running (above); close it, or set ALLOW_JAVA=1 to go on regardless"; exit 6; fi
for f in phases.py fromxml_route.py spike_routes.py spike_checks.py; do
  cmp -s "$H/scripts/$f" "$P2/harness/$f" || { say "STOP: $H/scripts/$f differs from the committed copy $P2/harness/$f"; exit 7; }
done
for f in run-phases.sh p2_spread.py pooled_phases.py compare_phases.py summary_phases.py write_meta.py; do
  cmp -s "$H/$f" "$P2/harness/$f" || { say "STOP: $H/$f differs from the committed copy $P2/harness/$f"; exit 7; }
done
[ -d "$H/data/json-messy/$SIZE" ] || python3 "$P2/harness/make_messy.py" /Users/szu004/dev/sql-on-fhir.js/benchmark/data/synthea-clinical-wide/1/$SIZE "$H/data/json-messy/$SIZE"
"$P2/proto/build.sh" "$RUNTIME" "$VENV/lib/python3.12/site-packages/pyspark/jars" "$JAR" | tee "$LOG/jar.sha256"

# ---------------------------------------------------------------- 2. noise check
label a0-r1-514f8f8 pathling
label a0-r2-514f8f8 pathling
say "replicate spread between the two A0 labels"
if ! python3 "$H/p2_spread.py" $PREFIX-a0-r1-514f8f8 $PREFIX-a0-r2-514f8f8 | tee "$LOG/spread.txt"; then
  if [ "${IGNORE_SPREAD:-}" != 1 ]; then say "STOP: the A0 replicate spread is too wide (spread.txt); the candidates were not run"; exit 8; fi
  say "spread too wide, going on because IGNORE_SPREAD=1"
fi

# ---------------------------------------------------------------- 3. candidates (two replicates each)
label aplus-514f8f8 aplus
label b-514f8f8 b
label avariant-514f8f8 avariant

# ---------------------------------------------------------------- 4. the messy variant, under drop
for r in pathling aplus avariant b; do
  label "messy-$r-514f8f8" "$r" JSON_DIR="$H/data/json-messy/$SIZE" SOURCES_OVERRIDE=json SPIKE_POLICY=drop
done

# ---------------------------------------------------------------- 5. closing A0
label a0-r3-514f8f8 pathling
conditions > "$LOG/after.txt"
[ -e "$LOG/failed-labels.txt" ] && { say "labels that failed:"; cat "$LOG/failed-labels.txt"; }
say "batch done"
