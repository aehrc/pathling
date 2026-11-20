# Shared environment for the probes. Source from anywhere.
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
TP=$(thirdparty_cp)
RUN="$W/specs/001-parquet-on-fhir/evidence/scripts/ticket-12/run.sh"
SCRIPTS=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/singular-meta-pruning
DATA=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/singular-meta-pruning
