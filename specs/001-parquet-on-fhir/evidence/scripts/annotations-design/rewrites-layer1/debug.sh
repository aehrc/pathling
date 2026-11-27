#!/bin/bash
# Runs one view as P+R+D3b with the rule verbose. Usage: debug.sh 4.3|4.0 '<path>'
set -e
. "$(cd "$(dirname "$0")" && pwd)/env.sh" "$1"
V="{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\",\"select\":[{\"column\":[{\"path\":\"$2\",\"name\":\"r\",\"collection\":true}]}]}"
javac -nowarn -cp "$CP" -d "$CLASSES" "$HERE/DebugRule.java"
java "${JOPTS[@]}" -cp "$CP" DebugRule "$DATA/data-$1" "$V" 2>&1 | grep -v "^WARNING\|^Setting\|^Using" | cut -c1-1500
