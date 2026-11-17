#!/bin/bash
# File sizes, per-column chunk sizes from the footers, and the Polars plan check.
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
mkdir -p "$OUT/logs"
"$PY" "$HERE/size.py" "$DATA" > "$OUT/logs/size.log" 2>&1
