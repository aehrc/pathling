# Shared environment for the D3a-complex sketch. Source from anywhere.
# TP is a classpath file of the third-party jars (the build needs only Spark, parquet-hadoop and
# the Scala 2.13 library from it).
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
TP=$(thirdparty_cp)
SCRIPTS=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/d3a-complex-sketch
DATA=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/d3a-complex-sketch
