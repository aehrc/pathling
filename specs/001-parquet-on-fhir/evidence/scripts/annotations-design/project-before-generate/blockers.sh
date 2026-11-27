#!/bin/bash
# Prints the optimised plan of the rewritten views that do not prune fully, to attribute blockers.
# Usage: blockers.sh 4.3
cd "$(dirname "$0")"
V='{"resourceType":"ViewDefinition","resource":"Patient","status":"active","select":'
for sel in \
  '[{"column":[{"path":"name.exists()","name":"r","collection":true}]}]' \
  '[{"column":[{"path":"address.period.start","name":"r","collection":true}]}]' \
  '[{"column":[{"path":"name.period.start","name":"r","collection":true}]}]' ; do
  echo "=== $sel"
  bash shape.sh "$1" "$V$sel}" 2>&1 | sed -n '/^OPTIMISED/,$p'
done
echo "=== where exists"
bash shape.sh "$1" "$V"'[{"forEach":"name","column":[{"path":"family","name":"f"}]}],"where":[{"path":"name.exists(use = '"'"'nickname'"'"')"}]}' 2>&1 | sed -n '/^OPTIMISED/,$p'
