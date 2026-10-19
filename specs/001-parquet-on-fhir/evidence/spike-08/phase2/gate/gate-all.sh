#!/usr/bin/env bash
# Spike 08 phase 2: the candidates' correctness gate at size s, all types and sources.
H=/Users/szu004/dev/pathling/.local/work/issue/2367/pathling-compare
L=/Users/szu004/dev/pathling/.claude/worktrees/issue/2831-spike-08/.local/work/issue/2831-spike-08
P=$H/venv-514f8f8/bin/python; S=/Users/szu004/dev/sql-on-fhir.js/benchmark/data/synthea-clinical-wide/1/s
export PYSPARK_PYTHON=$P PYSPARK_DRIVER_PYTHON=$P PYSPARK_SUBMIT_ARGS="--driver-memory 8g --jars ${JAR:-$L/proto/spike08-proto.jar} --conf spark.ui.showConsoleProgress=false --conf spark.jars.ivy=$L/ivy-dev pyspark-shell"
for spec in "b json,xml-lines,xml-bundles,xml-bundles-txn" "aplus json,xml-lines,xml-bundles,xml-bundles-txn" "avariant json"; do
  set -- $spec
  echo "##### $1"
  $P $H/scripts/gate_candidates.py $1 $2 $S $H/data/xml/s $H/data/xml-bundles/s $H/data/xml-bundles-txn/s Patient Condition Encounter Observation 2>&1 \
    | grep -E '^(==|   |      )|Error|Exception|Traceback' | grep -v "^\s*at "
done
echo "##### messy (drop), against HEAD's encoding of the clean data"
for r in pathling aplus avariant b; do
  SPIKE_POLICY=drop SPIKE_FINDINGS=1 $P $H/scripts/gate_messy.py $r $S $H/data/json-messy/s Patient Condition Encounter Observation 2>&1 | grep -E "^$r |Traceback|Error:"
done
