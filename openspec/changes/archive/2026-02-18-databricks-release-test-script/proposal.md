## Why

The release checklist requires manual testing of snapshot library API, dev Python release, and R package on Databricks. This is time-consuming and error-prone. Automating it with a script ensures consistent, repeatable validation of candidate releases on the target Databricks runtime.

## What changes

- New Python script that automates the Databricks release testing workflow using the Databricks CLI and REST API.
- Accepts a Maven SNAPSHOT version, a PyPI dev release version, a GitHub Actions run ID (for the R package artifact), and a Databricks cluster ID.
- Configures the nominated cluster with the correct Java/Python/Maven libraries and Databricks runtime.
- Downloads the R package from the GitHub Actions artifacts, uploads it to DBFS.
- Starts the cluster and runs two nominated test notebooks, reporting pass/fail.

## Capabilities

### New capabilities

- `databricks-release-test`: A CLI script that automates end-to-end testing of a candidate Pathling release on a nominated Databricks cluster, including package configuration, cluster startup, R package upload, and notebook execution with pass/fail reporting.

### Modified capabilities

_None._

## Impact

- New script added to the repository (likely under a `scripts/` directory).
- Depends on the Databricks CLI being installed and authenticated.
- Depends on the GitHub CLI being installed and authenticated (for downloading R package artifacts).
- No changes to existing code or APIs.
