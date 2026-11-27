#!/bin/bash
# Assembles the evidence file from the write-up head and tail and the generated tables.
set -e
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
D=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/project-before-generate
sed -i '' 's/on the 22-row Patient set/on the 22-patient set/' "$D/head.md"
cat "$D/head.md" "$D/logs/tables.md" "$D/tail.md" \
  > "$W/specs/001-parquet-on-fhir/evidence/annotations-design-project-before-generate.md"
wc -l "$W/specs/001-parquet-on-fhir/evidence/annotations-design-project-before-generate.md"
