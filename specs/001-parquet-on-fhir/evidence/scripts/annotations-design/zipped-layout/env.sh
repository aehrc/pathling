# Shared environment for the zipped-layout probe. Usage: . env.sh
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
HERE=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/zipped-layout
SP=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes
OUT=$SP/zipped-layout
DATA=$OUT/data
# The python of a venv with pyarrow, duckdb, polars and fsspec
# (uv venv; uv pip install pyarrow duckdb polars fsspec). PY overrides it.
PY=${PY:-$W/.local/annotations-design/venv/bin/python}
# The Spark classpath of the rewrites-layer1 probe, which holds the 4.0.2 and 4.3.0 variants.
R1=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/rewrites-layer1
TP=$(thirdparty_cp)
sparkcp() {
  if [ "$1" = "4.3" ]; then
    TP2=$(tr ':' '\n' < "$TP" | grep -v 'org/apache/spark/' | paste -sd: -)
    echo "$(cat "$(spark43_cp)"):$TP2"
  else
    cat "$TP"
  fi
}
JOPTS=(-Xmx6g -Djava.security.manager=allow
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
