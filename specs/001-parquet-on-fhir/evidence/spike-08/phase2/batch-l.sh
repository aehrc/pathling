#!/usr/bin/env bash
# Spike 08 phase 2: the whole size-l sequence, unattended. See RUNBOOK.md beside this file.
#
#   1. Pre-flight: the A0 runtime jar's hash, the harness scripts against the committed copies, the
#      prototype jar built from the committed sources, and the machine's state.
#   2. One slice per source, in the order JSON, XML lines, XML Bundles, transaction Bundles. Each slice runs
#      every route on that source, interleaved: A0, A+, B and (JSON only) A-variant as replicate r1, then
#      the same as r2. A0 runs extensions off and on, as in phase 1. The candidates run with extensions on
#      only: the extensions setting configures only the legacy encoders, and a candidate fits the schema as
#      the data has it. After each slice, its comparison is appended to slices.md.
#   3. The noise gate, after the JSON slice: p2_spread.py on A0's r1 and r2. If the spread is too wide, stop.
#   4. A last slice: the messy JSON variant (one shape error per file), under the drop policy, extensions on
#      only: A0, A+, A-variant, B.
# Labels are <PREFIX>-<route>-<r1|r2>-514f8f8 (p2 by default); each holds one results file per source and
# setting. A step whose results files all exist is complete and is skipped. With RESUME=1 an interrupted
# step is continued (run-phases.sh skips its finished files); without it the script stops. An A0 step that
# fails stops the batch; a candidate or messy step that fails is recorded in failed-labels.txt and the batch
# goes on, so one candidate cannot cost the others their measurement.
# For a dry run: SIZE=s PREFIX=dry IGNORE_SPREAD=1, with RESOURCES_OVERRIDE and EXTS_OVERRIDE to shorten it.
# A narrower batch: SLICES (the sources, default all four), CANDIDATES (default "aplus b avariant"; avariant
# and orig run on JSON only) and MESSY=0 (no messy slice). The run of the original spike beside A0 and B was
#   PREFIX=p3 SLICES=json CANDIDATES="b orig" MESSY=0 batch-l.sh
# A candidate may be given as name:route:VAR=value[,VAR=value...], a label of its own for a route run with
# settings of its own, such as name:b:SPIKE_POLICY=none,SPARK_CONF=spark.sql.codegen.hugeMethodLimit=8000.
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
step() {
  # step <name> <route> <source> <critical: 1 stops the batch on failure> [env assignments...]
  # Runs one route on one source into results/<PREFIX>-<name>/, through run-phases.sh.
  local name=$PREFIX-$1 route=$2 src=$3 critical=$4; shift 4
  local out="$H/results/$name"
  # The settings this step runs: the candidates' EXTS_OVERRIDE=1, or A0's own (both, unless a dry run says).
  local exts="${EXTS_OVERRIDE:-0 1}" a e missing=0
  for a in ${@+"$@"}; do case "$a" in EXTS_OVERRIDE=*) exts="${a#EXTS_OVERRIDE=}";; esac; done
  for e in $exts; do [ -e "$out/phases-$src-ext$e.json" ] || missing=1; done
  if [ $missing = 0 ]; then say "skip $name $src (complete)"; return 0; fi
  if ls "$out"/phases-"$src"-ext*.out >/dev/null 2>&1 && [ "${RESUME:-}" != 1 ]; then
    say "STOP: $name $src is incomplete; rerun with RESUME=1"; exit 3
  fi
  preflight_label
  say "start $name $src ($route $*)"
  conditions > "$LOG/$name.$src.conditions-before.txt"
  # The prototype jar goes on the classpath of the candidates only, so A0 runs exactly as in phase 1.
  local jars=""; [ "$route" = pathling ] || jars="$JAR"
  env ${@+"$@"} EXTRA_JARS="$jars" PATHLING_SRC="$WT/bench-514f8f8" RESUME=1 SOURCES_OVERRIDE="$src" \
    "$H/run-phases.sh" "$name" "$VENV" "$route" "$SIZE" > "$LOG/$name.$src.log" 2>&1
  local status=$?
  say "end $name $src (exit $status)"
  { echo "prototype jar: $(shasum -a 256 "$JAR")"; echo "env: $*"; echo "exit: $status"; } > "$out/p2-notes-$src.txt"
  if [ $status -ne 0 ]; then
    if [ "$critical" = 1 ]; then say "STOP: $name $src failed; see $LOG/$name.$src.log"; exit 4; fi
    say "FAILED: $name $src (exit $status), going on; see $LOG/$name.$src.log"
    echo "$name $src exit $status" >> "$LOG/failed-labels.txt"
  fi
}
pooled() {
  # pooled <source> <baseline-label> <label> [--py4j]: pooled_phases.py's header and this source's rows, or
  # the last line of its error where it fails.
  local src=$1 out; shift
  if out=$(python3 "$H/pooled_phases.py" "$@" 2>&1); then
    echo "$out" | sed -n 1p; echo "$out" | grep "^$src " || echo "(no rows for $src)"
  else
    echo "(pooled_phases.py failed: $(echo "$out" | tail -1))"
  fi
}
report() {
  # report <source> <title> <baseline-label> <label>...: the slice's rows of pooled_phases.py, into slices.md.
  local src=$1 title=$2 base=$3 l; shift 3
  {
    echo; echo "## $title, $(date '+%F %T')"
    for l in "$@"; do
      echo; echo "### $l against $base (pooled minimum in ms, and the ratio)"; echo '```'
      pooled "$src" "$base" "$l"
      if [ "$l" != "${l#*aplus}" ] || [ "$l" != "${l#*avariant}" ]; then
        echo "-- without py4j plan building:"; pooled "$src" "$base" "$l" --py4j | sed 1d
      fi
      echo '```'
    done
  } >> "$LOG/slices.md"
}
spread() {
  # spread <source>: A0's r1 against r2 on this source, into slices.md; returns p2_spread.py's verdict.
  local src=$1 status
  python3 "$H/p2_spread.py" "$PREFIX-a0-r1-514f8f8" "$PREFIX-a0-r2-514f8f8" > "$LOG/spread-$src.txt"
  status=$?
  { echo; echo "### A0 r1 against r2 (the noise check; the verdict covers every source so far)"; echo '```'
    sed -n 1p "$LOG/spread-$src.txt"
    grep "^$src " "$LOG/spread-$src.txt"; tail -1 "$LOG/spread-$src.txt"; echo '```'; } >> "$LOG/slices.md"
  return $status
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

# ---------------------------------------------------------------- 2 and 3. a slice per source
json_only() { [ "$1" = avariant ] || [ "$1" = orig ]; }
for src in ${SLICES:-json xml-lines xml-bundles xml-bundles-txn}; do
  cands=""
  for c in ${CANDIDATES:-aplus b avariant}; do
    r=$(echo "$c:$c" | cut -d: -f2)
    [ "$src" = json ] || ! json_only "$r" && cands="$cands $c"
  done
  for rep in r1 r2; do
    step "a0-$rep-514f8f8" pathling "$src" 1
    for c in $cands; do
      name=${c%%:*}; r=$(echo "$c:$c" | cut -d: -f2); envs=""
      case "$c" in *:*:*) envs=$(echo "$c" | cut -d: -f3- | tr ',' ' ');; esac
      # shellcheck disable=SC2086  # The settings are VAR=value words without spaces.
      step "$name-$rep-514f8f8" "$r" "$src" 0 EXTS_OVERRIDE=1 $envs
    done
  done
  labels=""; for c in $cands; do labels="$labels $PREFIX-${c%%:*}-r?-514f8f8"; done
  set -f   # The labels are globs for pooled_phases.py, not for the shell.
  report "$src" "Slice: $src" "$PREFIX-a0-r1-514f8f8" $labels
  set +f
  if spread "$src"; then
    say "slice $src done; A0 spread acceptable"
  elif [ "$src" = json ] && [ "${IGNORE_SPREAD:-}" != 1 ]; then
    say "STOP: the A0 replicate spread on JSON is too wide (spread-json.txt); no further slice was run"; exit 8
  else
    say "slice $src done; A0 spread too wide (spread-$src.txt)"
  fi
done

# ---------------------------------------------------------------- 4. the messy variant, under drop
[ "${MESSY:-1}" = 1 ] && for r in pathling aplus avariant b; do
  step "messy-$r-514f8f8" "$r" json 0 JSON_DIR="$H/data/json-messy/$SIZE" SPIKE_POLICY=drop EXTS_OVERRIDE=1
done
[ "${MESSY:-1}" = 1 ] && report json "Slice: messy JSON, under drop" "$PREFIX-messy-pathling-514f8f8" \
  "$PREFIX-messy-aplus-514f8f8" "$PREFIX-messy-avariant-514f8f8" "$PREFIX-messy-b-514f8f8"
conditions > "$LOG/after.txt"
[ -e "$LOG/failed-labels.txt" ] && { say "labels that failed:"; cat "$LOG/failed-labels.txt"; }
say "batch done"
