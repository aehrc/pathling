# Shared environment for the Databricks probes. Source from anywhere.
. "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
# The Spark and Delta classpath of the OSS spike, used only to compile and to run locally.
TP=$(thirdparty_cp)
HERE_DBX=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/lambda-pruning-databricks
OSS=$W/specs/001-parquet-on-fhir/evidence/scripts/annotations-design/lambda-pruning-rule
WORK=$W/.local/work/issue/2832_41-42_decimal-and-annotations/spikes/lambda-pruning-databricks
PROFILE=pathling
DBFS=dbfs:/lpr
# DATABRICKS_USER is your user name in the workspace, which owns the uploaded files.
WS=/Workspace/Users/${DATABRICKS_USER}/lpr
