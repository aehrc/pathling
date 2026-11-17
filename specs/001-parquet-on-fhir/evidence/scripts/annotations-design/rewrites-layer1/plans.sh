#!/bin/bash
# Prints the analysed plan of one view path. Usage: plans.sh 4.3 '<path>' [forEach]
cd "$(dirname "$0")"
V='{"resourceType":"ViewDefinition","resource":"Patient","status":"active","select":'
if [ -n "$3" ]; then
  SEL="[{\"forEach\":\"$3\",\"column\":[{\"path\":\"$2\",\"name\":\"r\"}]}]}"
else
  SEL="[{\"column\":[{\"path\":\"$2\",\"name\":\"r\",\"collection\":true}]}]}"
fi
bash shape.sh "$1" "$V$SEL" 2>&1 | sed -n '/^ANALYSED/,/^REWRITTEN/p' | cut -c1-3500
