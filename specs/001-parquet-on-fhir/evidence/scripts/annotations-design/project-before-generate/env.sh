# Shared environment. Usage: . env.sh <4.3|4.0>. Sets CP, CLASSES, SPARKCP, J and JOPTS.
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
HERE=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/project-before-generate
SP=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes
DATA=$SP/project-before-generate
TP=$(thirdparty_cp)
if [ "$1" = "4.3" ]; then
  # The Pathling jars are built against 4.0.2, so its jars are dropped and 4.3.0 goes first.
  TP2=$(tr ':' '\n' < "$TP" | grep -v 'org/apache/spark/' | paste -sd: -)
  SPARKCP="$(cat "$(spark43_cp)"):$TP2"
else
  SPARKCP="$(cat "$TP")"
fi
CLASSES=$DATA/classes-$1
CP="$SPARKCP:$J:$CLASSES"
JOPTS=(-Xmx6g -Djava.security.manager=allow
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED)
