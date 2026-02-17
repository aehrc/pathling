## Context

The Pathling release checklist includes a manual step: "Test snapshot library API, dev Python library release and R package on target Databricks release." This involves navigating the Databricks UI to configure libraries on a cluster, uploading the R package, starting the cluster, and manually running test notebooks. The Databricks CLI and REST API provide programmatic access to all of these operations.

The current release process publishes:

- SNAPSHOT JARs to Maven Central snapshots (via the pre-release workflow).
- Dev Python packages to PyPI (via the python-pre-release workflow).
- Built R packages as GitHub Actions artifacts (attached to the pre-release workflow run).

The target Databricks runtime is currently 17.3 LTS, and clusters require `JNAME=zulu21-ca-amd64` for Java 21 support.

## Goals / Non-goals

**Goals:**

- Automate the Databricks testing step of the release checklist into a single command.
- Support configuring Maven (SNAPSHOT), PyPI (dev), and R packages on a nominated cluster.
- Start the cluster and run nominated test notebooks with pass/fail reporting.
- Keep the script simple and self-contained with minimal dependencies.

**Non-goals:**

- Creating or managing Databricks clusters (the cluster must already exist).
- Automating other release checklist steps.
- Supporting non-Databricks Spark environments.
- Managing Databricks authentication (user must have CLI configured).

## Decisions

### Python with Databricks CLI/REST API

Use a Python script that invokes the Databricks CLI (`databricks` command) and REST API for cluster and job operations. The Databricks CLI handles authentication and provides commands for cluster management, library installation, DBFS uploads, and job runs.

**Alternative considered:** Shell script — rejected because Python provides better error handling, JSON parsing, and structured argument handling.

### CLI arguments for all configurable inputs

Accept all variable inputs as CLI arguments: Maven version, PyPI version, GitHub run ID, cluster ID, notebook paths, and Databricks runtime version. This avoids config files and keeps the script stateless.

### Download R package via GitHub CLI

Use `gh run download` to fetch the R package artifact from a nominated GitHub Actions run. This reuses the existing `gh` authentication and avoids needing GitHub API tokens.

### Use Databricks Jobs API for notebook execution

Run notebooks via the Jobs API (`POST /api/2.1/jobs/runs/submit`) rather than the Clusters API. This provides built-in run tracking, output capture, and success/failure status without polling notebook state.

### Cluster library configuration via REST API

Use the Clusters API (`POST /api/2.0/clusters/edit`) to set the cluster's library configuration (Maven coordinates with snapshot repo, PyPI package), Spark environment variables, and the Databricks runtime version. This is idempotent and replaces whatever was previously configured.

## Risks / Trade-offs

- **Databricks CLI version dependency** → Document required CLI version; use stable API endpoints only.
- **Authentication complexity** → Require pre-configured Databricks CLI profile; do not manage tokens in the script.
- **Cluster state conflicts** → The script will terminate the cluster before reconfiguring, which may disrupt other users. Document this and require the user to nominate a dedicated test cluster.
- **GitHub Actions artifact expiry** → Artifacts expire after 90 days by default. The R package must be downloaded before expiry.
- **Network timeouts during cluster start** → Implement polling with a configurable timeout for cluster startup and notebook runs.
