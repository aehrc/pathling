#!/usr/bin/env bash
# Spike 08: runs scripts/phases.py (fit, encode and decode timed apart) for one build and one route, at size l by
# default, into results/<label>/. Added beside run-revision.sh, which it does not change.
# Usage: run-phases.sh <label> <venv-dir> <pathling|fromxml|aplus|avariant|b> [size]
#   pathling  HEAD's own routes: JSON lines, and XML lines and both Bundle shapes through HAPI.
#   fromxml   the from_xml prototype (scripts/fromxml_route.py) for the three XML sources; no JSON.
#   aplus, b  phase 2's candidates (scripts/spike_routes.py), JSON and the three XML sources.
#   avariant  phase 2's A-variant, JSON only.
# Phase 2 options, all unset by default, which leaves the phase 1 behaviour unchanged:
#   EXTRA_JARS   a jar for --jars (the Spike08*.java prototype jar, which the candidate routes need);
#   JSON_DIR     a JSON data directory in place of the benchmark's (the messy variant);
#   SOURCES_OVERRIDE the sources to run, space-separated, in place of the route's default;
#   SPIKE_POLICY fail (the default in spike_routes.py) or drop;
#   RESOURCES_OVERRIDE, EXTS_OVERRIDE  fewer types, or one extensions setting, for a dry run.
# Never overwrites an existing results/<label>/ directory; RESUME=1 continues an interrupted run.
set -euo pipefail

LABEL="$1"; VENV="$2"; ROUTE="$3"; SIZE="${4:-l}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO=/Users/szu004/dev/sql-on-fhir.js
OUT="$HERE/results/$LABEL"
DATA="$REPO/benchmark/data/synthea-clinical-wide/1/$SIZE"
RESOURCES=(Patient Condition Encounter Observation)
# Phase 2 dry runs only: fewer types or one extensions setting. Unset, nothing changes.
[ -n "${RESOURCES_OVERRIDE:-}" ] && read -r -a RESOURCES <<< "$RESOURCES_OVERRIDE"
EXTS=(0 1)
[ -n "${EXTS_OVERRIDE:-}" ] && read -r -a EXTS <<< "$EXTS_OVERRIDE"

[ -e "$OUT" ] && [ "${RESUME:-}" != 1 ] && { echo "refusing to overwrite $OUT" >&2; exit 1; }
mkdir -p "$OUT"
export PATHLING_LABEL="$LABEL"
# A private Ivy home per label, as run-revision.sh does, with cache and jars made up front.
IVY="$HERE/ivy/$LABEL"
mkdir -p "$IVY/cache" "$IVY/jars"
export PYSPARK_SUBMIT_ARGS="--driver-memory 16g ${EXTRA_JARS:+--jars $EXTRA_JARS }--conf spark.jars.ivy=$IVY pyspark-shell"
PY="$VENV/bin/python"
export PYSPARK_PYTHON="$PY" PYSPARK_DRIVER_PYTHON="$PY"

[ -e "$OUT/meta.json" ] || "$PY" "$HERE/write_meta.py" "$OUT/meta.json" "$LABEL" "$VENV" "$DATA" "$REPO" ${PATHLING_SRC:+"$PATHLING_SRC"}
{ date; uptime; pgrep -fl java | cut -c1-200 || true; df -h /System/Volumes/Data | tail -1; } > "$OUT/conditions-start.txt"

XML_DIR="$HERE/data/xml/$SIZE"
SOURCES=(xml-lines xml-bundles xml-bundles-txn)
[ "$ROUTE" = pathling ] || [ "$ROUTE" = aplus ] || [ "$ROUTE" = b ] && SOURCES=(json "${SOURCES[@]}")
[ "$ROUTE" = avariant ] && SOURCES=(json)
[ -n "${SOURCES_OVERRIDE:-}" ] && read -r -a SOURCES <<< "$SOURCES_OVERRIDE"
JSON_DATA="${JSON_DIR:-$DATA}"
for EXT in "${EXTS[@]}"; do
  for SRC in "${SOURCES[@]}"; do
    DIR="$HERE/data/xml-bundles/$SIZE"; [ "$SRC" = xml-bundles-txn ] && DIR="$HERE/data/xml-bundles-txn/$SIZE"
    NAME="phases-$SRC-ext$EXT"
    [ -e "$OUT/$NAME.json" ] || ENABLE_EXTENSIONS=$EXT "$PY" "$HERE/scripts/phases.py" "$SRC" "$ROUTE" "$JSON_DATA" "$XML_DIR" "$DIR" \
      "$OUT/$NAME.json" "${RESOURCES[@]}" > "$OUT/$NAME.out" 2> "$OUT/$NAME.err"
    grep -E '^(Patient|Condition|Encounter|Observation) ' "$OUT/$NAME.out" | sed "s/^/$NAME /"
  done
  df -h /System/Volumes/Data | tail -1
done
# The jar Spark resolved into the private Ivy home, for comparison with the built jar in meta.json.
shasum -a 256 "$IVY"/jars/au.csiro.pathling_library-runtime-*.jar > "$OUT/resolved-jar.sha256"
{ date; uptime; pgrep -fl java | cut -c1-200 || true; df -h /System/Volumes/Data | tail -1; } > "$OUT/conditions-end.txt"
echo "done $LABEL"
