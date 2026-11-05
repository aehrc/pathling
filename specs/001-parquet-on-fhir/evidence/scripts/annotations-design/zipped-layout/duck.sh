#!/bin/bash
# DuckDB reads (duck.py) and the C10a queries (c10a-duck.sql, through the duckdb Python module).
. "$(cd "$(dirname "$0")" && pwd)/env.sh"
mkdir -p "$OUT/logs"
"$PY" "$HERE/duck.py" "$DATA" > "$OUT/logs/duck.log" 2>&1
"$PY" "$HERE/c10a_duck.py" > "$OUT/logs/c10a-duck.log" 2>&1
