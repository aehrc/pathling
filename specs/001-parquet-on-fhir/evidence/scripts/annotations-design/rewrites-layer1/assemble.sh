#!/bin/bash
# Assembles the evidence file from the write-up parts and the generated tables.
set -e
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
D=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/rewrites-layer1
python3 "$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/rewrites-layer1/make_tables.py" "$D/logs/probe-4.3.log" "$D/logs/probe-4.0.log" > "$D/logs/tables.md"
python3 "$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/rewrites-layer1/make_tables.py" "$D/logs/probe-4.3.log" "$D/logs/probe-4.0.log" "$D/logs/semantic-4.3.log" > "$D/logs/semantic.md"
cat "$D/head.md" "$D/logs/semantic.md" "$D/mid.md" "$D/logs/tables.md" "$D/tail.md" \
  > "$W/specs/001-parquet-on-fhir/evidence/annotations-design-rewrites-layer1.md"
wc -l "$W/specs/001-parquet-on-fhir/evidence/annotations-design-rewrites-layer1.md"
