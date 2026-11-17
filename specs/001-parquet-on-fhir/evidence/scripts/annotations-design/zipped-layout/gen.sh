#!/bin/bash
# Generates the data of both layouts and both metadata variants. Usage: gen.sh [rows]
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
"$PY" "$HERE/gen.py" "$DATA" "${1:-200000}" 2>&1 | tee "$OUT/gen.log"
