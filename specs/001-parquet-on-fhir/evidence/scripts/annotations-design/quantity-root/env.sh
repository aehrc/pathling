# Shared environment for the quantity-root probes. Source from anywhere.
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
TP=$(thirdparty_cp)
SCRIPTS=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design
HERE=$SCRIPTS/quantity-root
DATA=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/quantity-root
PUSHDOWN_DATA=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/pushdown
JOPTS=(-Xmx6g -Djava.security.manager=allow
  -Dlogback.configurationFile="$W/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/logback.xml"
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
